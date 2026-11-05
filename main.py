

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
from urllib3.util.retry import Retry

load_dotenv()
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

SAFE_BASE_URL = os.getenv("SAFE_BASE_URL", "https://safe.trade/api/v2")
GATE_BASE_URL = os.getenv("GATE_BASE_URL", "https://api.gateio.ws/api/v4")
SAFE_MARKET = os.getenv("SAFE_MARKET", "pearlusdt").lower()
GATE_MARKET = os.getenv("GATE_MARKET", "PEARL_USDT")
PAPER_BASE = "PEARL"
PAPER_QUOTE = "USDT"


def D(value: Any) -> Decimal:
    return Decimal(str(value))


@dataclass(frozen=True)
class Level:
    price: Decimal
    amount: Decimal


@dataclass(frozen=True)
class Book:
    name: str
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]


def parse_levels(items: Any) -> tuple[Level, ...]:
    result: list[Level] = []
    for item in items or []:
        if isinstance(item, dict):
            price = item.get("price")
            amount = item.get("amount", item.get("quantity", item.get("volume")))
        else:
            price, amount = item[0], item[1]
        if price is not None and amount is not None and D(amount) > 0:
            result.append(Level(D(price), D(amount)))
    return tuple(result)


def parse_book(name: str, raw: Any) -> Book:
    if isinstance(raw, dict) and isinstance(raw.get("data"), dict):
        raw = raw["data"]
    bids, asks = parse_levels(raw.get("bids")), parse_levels(raw.get("asks"))
    if not bids or not asks:
        raise RuntimeError(f"{name} order book has no bids/asks: {raw}")
    return Book(name, bids, asks)


class MarketClient:
    def __init__(self) -> None:
        self.http = requests.Session()
        self.http.trust_env = True
        retry = Retry(total=2, connect=2, read=2, status=0, backoff_factor=0.3, allowed_methods=frozenset({"GET"}), raise_on_status=False)
        adapter = HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=1)
        self.http.mount("http://", adapter)
        self.http.mount("https://", adapter)
        self.http.headers.update({"User-Agent": "safeToGate-paper-arbitrage/1.0", "Accept": "application/json"})
        self.last_date_header = None

    def get(self, url: str, **kwargs: Any) -> Any:
        response = self.http.get(url, timeout=8, **kwargs)
        response.raise_for_status()
        self.last_date_header = response.headers.get("Date")
        return response.json()

    def safe_book(self) -> Book:
        paths = [
            f"{SAFE_BASE_URL}/trade/public/markets/{SAFE_MARKET}/depth",
            f"{SAFE_BASE_URL}/public/markets/{SAFE_MARKET}/order-book",
            f"{SAFE_BASE_URL}/peatio/public/markets/{SAFE_MARKET}/order-book",
        ]
        errors = []
        for url in paths:
            try:
                return parse_book("SafeTrade", self.get(url))
            except (requests.RequestException, RuntimeError, ValueError) as exc:
                errors.append(f"{url}: {exc}")
        raise RuntimeError("SafeTrade unavailable: " + " | ".join(errors))

    def gate_book(self) -> Book:
        raw = self.get(f"{GATE_BASE_URL}/spot/order_book", params={"currency_pair": GATE_MARKET, "limit": 50})
        return parse_book("Gate", raw)


class PaperDB:
    def __init__(self, path: str) -> None:
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS cycles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL,
                direction TEXT NOT NULL,
                amount TEXT NOT NULL,
                entry_spread TEXT NOT NULL,
                entry_buy_price TEXT NOT NULL,
                entry_sell_price TEXT NOT NULL,
                entry_fee TEXT NOT NULL,
                rebalance_spread TEXT,
                rebalance_buy_price TEXT,
                rebalance_sell_price TEXT,
                rebalance_fee TEXT,
                profit_usdt TEXT,
                started_at TEXT NOT NULL,
                completed_at TEXT
            )
        """)
        self.db.execute("CREATE TABLE IF NOT EXISTS bot_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        self.db.commit()

    def state(self, key: str) -> Any | None:
        row = self.db.execute("SELECT value FROM bot_state WHERE key = ?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def save_state(self, key: str, value: Any) -> None:
        self.db.execute("INSERT OR REPLACE INTO bot_state(key,value) VALUES (?,?)", (key, json.dumps(value, ensure_ascii=False)))
        self.db.commit()

    def start_cycle(self, direction: str, amount: Decimal, spread: Decimal, buy_price: Decimal, sell_price: Decimal, fee: Decimal) -> int:
        cursor = self.db.execute(
            "INSERT INTO cycles(status,direction,amount,entry_spread,entry_buy_price,entry_sell_price,entry_fee,started_at) VALUES(?,?,?,?,?,?,?,datetime('now','localtime'))",
            ("OPEN", direction, str(amount), str(spread), str(buy_price), str(sell_price), str(fee)),
        )
        self.db.commit()
        return int(cursor.lastrowid)

    def finish_cycle(self, cycle_id: int, spread: Decimal, buy_price: Decimal, sell_price: Decimal, fee: Decimal, profit: Decimal) -> None:
        self.db.execute(
            "UPDATE cycles SET status='COMPLETED',rebalance_spread=?,rebalance_buy_price=?,rebalance_sell_price=?,rebalance_fee=?,profit_usdt=?,completed_at=datetime('now','localtime') WHERE id=?",
            (str(spread), str(buy_price), str(sell_price), str(fee), str(profit), cycle_id),
        )
        self.db.commit()

    def summary(self) -> tuple[int, Decimal]:
        row = self.db.execute("SELECT COUNT(*), COALESCE(SUM(CAST(profit_usdt AS REAL)),0) FROM cycles WHERE status='COMPLETED'").fetchone()
        return int(row[0]), D(row[1])


class PaperEngine:
    def __init__(self, db: PaperDB, initial_prl: Decimal, initial_usdt: Decimal, fee: Decimal, reserve_prl: Decimal, reserve_usdt: Decimal) -> None:
        self.db, self.fee = db, fee
        self.reserve_prl, self.reserve_usdt = reserve_prl, reserve_usdt
        self.initial_total_usdt = initial_usdt * D("2")
        saved = db.state("balances")
        self.balances = saved or {
            "SafeTrade": {"PEARL": str(initial_prl), "USDT": str(initial_usdt)},
            "Gate": {"PEARL": str(initial_prl), "USDT": str(initial_usdt)},
        }
        self.active = db.state("active_cycle") or []
        if isinstance(self.active, dict):
            self.active = [self.active]
        self.persist()

    def persist(self) -> None:
        self.db.save_state("balances", self.balances)
        self.db.save_state("active_cycle", self.active)

    def bal(self, exchange: str, asset: str) -> Decimal:
        return D(self.balances[exchange][asset])

    def change(self, exchange: str, asset: str, amount: Decimal) -> None:
        self.balances[exchange][asset] = str(self.bal(exchange, asset) + amount)

    @property
    def active_count(self) -> int:
        return len(self.active)

    def can_trade(self, buy_exchange: str, sell_exchange: str, amount: Decimal, buy_price: Decimal) -> bool:
        buy_cost = amount * buy_price * (D("1") + self.fee)
        return self.bal(buy_exchange, PAPER_QUOTE) - buy_cost >= self.reserve_usdt and self.bal(sell_exchange, PAPER_BASE) - amount >= self.reserve_prl

    def entry(self, direction: str, buy_exchange: str, sell_exchange: str, amount: Decimal, spread: Decimal, buy_price: Decimal, sell_price: Decimal) -> int:
        if not self.can_trade(buy_exchange, sell_exchange, amount, buy_price):
            raise RuntimeError("paper balance is insufficient for requested amount")
        buy_cost = amount * buy_price * (D("1") + self.fee)
        sell_value = amount * sell_price * (D("1") - self.fee)
        self.change(buy_exchange, PAPER_QUOTE, -buy_cost)
        self.change(buy_exchange, PAPER_BASE, amount)
        self.change(sell_exchange, PAPER_BASE, -amount)
        self.change(sell_exchange, PAPER_QUOTE, sell_value)
        cycle_id = self.db.start_cycle(direction, amount, spread, buy_price, sell_price, self.fee)
        self.active.append({"id": cycle_id, "direction": direction, "amount": str(amount), "buy_exchange": buy_exchange, "sell_exchange": sell_exchange, "entry_buy_price": str(buy_price), "entry_sell_price": str(sell_price)})
        self.persist()
        return cycle_id

    def rebalance_all(self, spread: Decimal, buy_price: Decimal, sell_price: Decimal) -> Decimal:
        if not self.active:
            raise RuntimeError("no active cycle")
        total_profit = D("0")
        for cycle in self.active:
            amount = D(cycle["amount"])
            rebuy_exchange = cycle["sell_exchange"]
            resell_exchange = cycle["buy_exchange"]
            if not self.can_trade(rebuy_exchange, resell_exchange, amount, buy_price):
                raise RuntimeError("paper balance is insufficient for rebalance")
        for cycle in self.active:
            amount = D(cycle["amount"])
            rebuy_exchange = cycle["sell_exchange"]
            resell_exchange = cycle["buy_exchange"]
            buy_cost = amount * buy_price * (D("1") + self.fee)
            sell_value = amount * sell_price * (D("1") - self.fee)
            self.change(rebuy_exchange, PAPER_QUOTE, -buy_cost)
            self.change(rebuy_exchange, PAPER_BASE, amount)
            self.change(resell_exchange, PAPER_BASE, -amount)
            self.change(resell_exchange, PAPER_QUOTE, sell_value)
            entry_cost = amount * D(cycle["entry_buy_price"]) * (D("1") + self.fee)
            entry_value = amount * D(cycle["entry_sell_price"]) * (D("1") - self.fee)
            profit = entry_value - entry_cost + sell_value - buy_cost
            total_profit += profit
            self.db.finish_cycle(int(cycle["id"]), spread, buy_price, sell_price, self.fee, profit)
        self.active = []
        self.persist()
        return total_profit

    def estimate_rebalance_profit(self, buy_price: Decimal, sell_price: Decimal) -> Decimal:
        """估算全部未完成周期反向成交后的合计利润，不修改余额。"""
        total_profit = D("0")
        for cycle in self.active:
            amount = D(cycle["amount"])
            entry_cost = amount * D(cycle["entry_buy_price"]) * (D("1") + self.fee)
            entry_value = amount * D(cycle["entry_sell_price"]) * (D("1") - self.fee)
            rebalance_cost = amount * buy_price * (D("1") + self.fee)
            rebalance_value = amount * sell_price * (D("1") - self.fee)
            total_profit += entry_value - entry_cost + rebalance_value - rebalance_cost
        return total_profit


def route(book_safe: Book, book_gate: Book) -> tuple[str, str, Decimal, Decimal, Decimal, Decimal]:
    safe_to_gate = book_gate.bids[0].price - book_safe.asks[0].price
    gate_to_safe = book_safe.bids[0].price - book_gate.asks[0].price
    if safe_to_gate >= gate_to_safe:
        return "SafeTrade->Gate", "SafeTrade", "Gate", safe_to_gate, book_safe.asks[0].price, book_gate.bids[0].price
    return "Gate->SafeTrade", "Gate", "SafeTrade", gate_to_safe, book_gate.asks[0].price, book_safe.bids[0].price


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper-only SafeTrade/Gate PEARL arbitrage")
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--amount", type=Decimal, default=D(os.getenv("PAPER_ORDER_AMOUNT", "4.5")))
    parser.add_argument("--trigger-spread", type=Decimal, default=D("0.005"))
    parser.add_argument("--rebalance-spread", type=Decimal, default=D("0.002"))
    parser.add_argument("--initial-prl", type=Decimal, default=D("10"))
    parser.add_argument("--initial-usdt", type=Decimal, default=D("10"))
    parser.add_argument("--fee", type=Decimal, default=D(os.getenv("PAPER_TAKER_FEE", "0.001")))
    parser.add_argument("--max-open-entries", type=int, default=5, help="价差持续时最多连续套利次数")
    parser.add_argument("--reserve-prl", type=Decimal, default=D("2"), help="每边至少保留的 PEARL")
    parser.add_argument("--reserve-usdt", type=Decimal, default=D("2"), help="每边至少保留的 USDT")
    parser.add_argument("--db", default=os.getenv("PAPER_DB", "arbitrage.db"))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--reset", action="store_true", help="重置纸上余额和未完成周期")
    parser.add_argument("--live", action="store_true", help="启用真实双边下单")
    parser.add_argument("--confirm-live", action="store_true", help="真实下单确认开关")
    parser.add_argument("--live-preflight", action="store_true", help="只读验证真实 API、余额和行情")
    parser.add_argument("--live-reset-halt", action="store_true", help="人工确认后解除真实模式熔断；不会下单")
    parser.add_argument("--live-clear-halt", action="store_true", help="仅清除本地真实模式熔断标记，不访问交易所、不下单")
    args = parser.parse_args()
    if args.amount <= 0 or args.trigger_spread <= 0 or args.rebalance_spread < 0:
        raise SystemExit("amount and thresholds must be non-negative/positive")
    if args.live and (not args.confirm_live or os.getenv("ENABLE_LIVE_TRADING", "false").lower() != "true"):
        raise SystemExit("安全保护：真实下单需要 --live --confirm-live，并在 .env 设置 ENABLE_LIVE_TRADING=true")
    if args.live_clear_halt:
        db = PaperDB(args.db)
        db.save_state("live_halted", False)
        print("已清除本地真实模式熔断标记：未访问交易所，未提交订单。")
        db.db.close()
        return
    if args.live or args.live_preflight:
        from live_trader import run_live
        run_live(args, preflight_only=args.live_preflight)
        return

    market, db = MarketClient(), PaperDB(args.db)
    if args.reset:
        reset_balances = {
            "SafeTrade": {"PEARL": str(args.initial_prl), "USDT": str(args.initial_usdt)},
            "Gate": {"PEARL": str(args.initial_prl), "USDT": str(args.initial_usdt)},
        }
        # --reset 表示开启全新的纸上测试，周期记录也一并清空。
        db.db.execute("DELETE FROM cycles")
        db.db.commit()
        db.save_state("balances", reset_balances)
        db.save_state("active_cycle", None)
        print(f"已完全重置：SafeTrade 和 Gate 均为 {args.initial_prl} PEARL、{args.initial_usdt} USDT；历史周期已清空。")
        db.db.close()
        return
    engine = PaperEngine(db, args.initial_prl, args.initial_usdt, args.fee, args.reserve_prl, args.reserve_usdt)
    print("纸上交易模式：只读取行情，不会调用任何下单接口")
    state_name = lambda active: "等待再平衡" if active else "等待入场"
    try:
        while True:
            try:
                safe, gate = market.safe_book(), market.gate_book()
                direction, buy_exchange, sell_exchange, spread, buy_price, sell_price = route(safe, gate)
                print(time.strftime("%Y-%m-%d %H:%M:%S"), f"SafeTrade 买一={safe.bids[0].price} 卖一={safe.asks[0].price}", "|", f"Gate 买一={gate.bids[0].price} 卖一={gate.asks[0].price}")
                print(f"  状态={state_name(engine.active)} 已开周期={engine.active_count} 价差={spread} USDT，绝对值={abs(spread)} USDT")
                print(f"  纸上余额：SafeTrade PEARL={engine.bal('SafeTrade','PEARL')} USDT={engine.bal('SafeTrade','USDT')} | Gate PEARL={engine.bal('Gate','PEARL')} USDT={engine.bal('Gate','USDT')}")

                if not engine.active and spread > args.trigger_spread:
                    if engine.can_trade(buy_exchange, sell_exchange, args.amount, buy_price):
                        cycle_id = engine.entry(direction, buy_exchange, sell_exchange, args.amount, spread, buy_price, sell_price)
                        print(f"  纸上入场 #{cycle_id}：{buy_exchange} 买入 @ {buy_price}，{sell_exchange} 卖出 @ {sell_price}，数量={args.amount}")
                    else:
                        print(f"  已达到余额保护上限：继续买入会低于保留余额（PEARL={args.reserve_prl}，USDT={args.reserve_usdt}），等待再平衡")
                elif engine.active:
                    # 所有连续入场周期都沿用第一笔的方向，等待同一方向价差持续或反向收窄。
                    first = engine.active[0]
                    rebuy_exchange = first["sell_exchange"]
                    resell_exchange = first["buy_exchange"]
                    rebuy_price = gate.asks[0].price if rebuy_exchange == "Gate" else safe.asks[0].price
                    resell_price = gate.bids[0].price if resell_exchange == "Gate" else safe.bids[0].price
                    reverse_signed_spread = resell_price - rebuy_price
                    reverse_spread = abs(reverse_signed_spread)
                    # 按当前监测到的跨所价差触发再平衡；实际利润仍按反向盘口成交价计算。
                    estimated_profit = engine.estimate_rebalance_profit(rebuy_price, resell_price)
                    if reverse_spread <= args.rebalance_spread and estimated_profit > 0:
                        open_count = engine.active_count
                        profit = engine.rebalance_all(reverse_spread, rebuy_price, resell_price)
                        print(f"  纸上统一再平衡：反向可执行价差绝对值={reverse_spread} USDT，预计周期利润={estimated_profit} USDT；已平衡 {open_count} 个周期，本次合计利润={profit} USDT")
                    elif direction == first["direction"] and spread > args.trigger_spread and engine.active_count < args.max_open_entries:
                        if engine.can_trade(buy_exchange, sell_exchange, args.amount, buy_price):
                            cycle_id = engine.entry(direction, buy_exchange, sell_exchange, args.amount, spread, buy_price, sell_price)
                            print(f"  纸上追加入场 #{cycle_id}：{buy_exchange} 买入 @ {buy_price}，{sell_exchange} 卖出 @ {sell_price}，数量={args.amount}，累计周期={engine.active_count}")
                        else:
                            print(f"  已达到余额保护上限：当前最多完成 {engine.active_count} 次，等待再平衡")
                    elif direction == first["direction"] and spread > args.trigger_spread:
                        print(f"  已达到连续套利上限 {args.max_open_entries} 次，等待再平衡")
                    else:
                        print(f"  等待再平衡：反向价差={reverse_signed_spread} USDT，绝对值={reverse_spread} USDT，预计周期利润={estimated_profit} USDT；需要绝对值 <= {args.rebalance_spread} 且利润 > 0")
                completed, total = db.summary()
                print(f"  数据库统计：已完成周期={completed}，累计利润={total} USDT")
            except (requests.RequestException, RuntimeError, ValueError, ArithmeticError) as exc:
                print(f"  行情请求失败：{exc}；程序将在下一轮继续重试")
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n已收到停止指令，程序正常退出；数据库状态已保留。")
    finally:
        db.db.close()


if __name__ == "__main__":
    main()
