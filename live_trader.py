from __future__ import annotations

import concurrent.futures
import email.utils
import hashlib
import hmac
import json
import os
import threading
import time
from decimal import Decimal
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from main import GATE_BASE_URL, GATE_MARKET, SAFE_BASE_URL, SAFE_MARKET, D, MarketClient, PaperDB, route


MIN_BALANCE = D("9")
MAX_USE = D("10")
SAFE_MIN_ORDER_AMOUNT = D("2")


class NoFillError(RuntimeError):
    """两边订单都没有成交，可以安全等待下一轮重试。"""


class UnequalFillError(RuntimeError):
    """两边成交数量不一致，必须熔断人工核对。"""


class TelegramNotifier:
    def __init__(self) -> None:
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if not self.token or not self.chat_id:
            print("Telegram 通知未启用：VPS 的 .env 缺少 TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID")

    def send(self, message: str) -> None:
        if not self.token or not self.chat_id:
            return
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={"chat_id": self.chat_id, "text": message},
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("ok") is not True:
                raise RuntimeError(payload.get("description", "Telegram API 返回失败"))
        except requests.RequestException as exc:
            print(f"Telegram 通知发送失败：{exc}")
        except (ValueError, RuntimeError) as exc:
            print(f"Telegram API 返回失败：{exc}")


def amount_of(rows: Any, currency: str, field: str) -> Decimal:
    currency = currency.lower()
    for row in rows or []:
        if str(row.get("currency", "")).lower() == currency:
            return D(row.get(field, 0))
    return D("0")


def order_id(data: Any) -> str | None:
    if not isinstance(data, dict):
        return None
    for key in ("id", "uuid", "order_id"):
        if data.get(key) is not None:
            return str(data[key])
    return None


def filled_amount(data: Any) -> Decimal:
    if not isinstance(data, dict):
        return D("0")
    for key in ("filled_amount", "executed_amount", "executed_volume", "deal_amount"):
        if data.get(key) is not None:
            return D(data[key])
    state = str(data.get("status", data.get("state", ""))).lower()
    if data.get("amount") is not None and state in ("closed", "done", "filled"):
        return D(data["amount"])
    if data.get("origin_volume") is not None and data.get("remaining_volume") is not None:
        return max(D("0"), D(data["origin_volume"]) - D(data["remaining_volume"]))
    return D("0")


def fill_price(data: Any, fallback: Decimal) -> Decimal:
    if isinstance(data, dict):
        for key in ("avg_deal_price", "avg_price", "average_price", "fill_price", "price"):
            if data.get(key) not in (None, "", "0"):
                return D(data[key])
    return fallback


class SafeLive:
    name = "SafeTrade"

    def __init__(self, http: requests.Session, key: str, secret: str):
        self.http, self.key, self.secret = http, key, secret
        self.nonce_lock = threading.Lock()
        self.last_nonce = 0
        self.clock_offset_ms = 0

    def sync_clock(self, date_header: str | None = None) -> None:
        samples = []
        if date_header:
            try:
                server_ms = int(email.utils.parsedate_to_datetime(date_header).timestamp() * 1000)
                self.clock_offset_ms = server_ms - int(time.time() * 1000)
                return
            except (TypeError, ValueError, OverflowError):
                pass
        # 使用已验证可用的深度接口，避免 ticker 接口在部分代理线路上被重置。
        url = SAFE_BASE_URL + f"/trade/public/markets/{SAFE_MARKET}/depth?_={int(time.time() * 1000)}"
        last_error = None
        for _ in range(3):
            try:
                started = time.time()
                response = self.http.get(url, timeout=15)
                response.raise_for_status()
                date_header = response.headers.get("Date")
                if date_header:
                    server_ms = int(email.utils.parsedate_to_datetime(date_header).timestamp() * 1000)
                    age_header = response.headers.get("Age")
                    if age_header and age_header.isdigit():
                        server_ms += int(age_header) * 1000
                    midpoint_ms = int((started + time.time()) * 500)
                    samples.append(server_ms - midpoint_ms)
                    break
            except requests.RequestException as exc:
                last_error = exc
                time.sleep(0.5)
        if samples:
            self.clock_offset_ms = min(samples, key=abs)
        elif last_error is not None:
            raise RuntimeError(f"SafeTrade 公共接口无法通过当前代理连接，无法校准 nonce 时间：{last_error}") from last_error
        else:
            raise RuntimeError("SafeTrade 公共接口没有返回 Date 时间头，无法安全校准 nonce")

    def headers(self) -> dict[str, str]:
        with self.nonce_lock:
            nonce = max(int(time.time() * 1000) + self.clock_offset_ms, self.last_nonce + 1)
            self.last_nonce = nonce
        nonce_text = str(nonce)
        signature = hmac.new(self.secret.encode(), (nonce_text + self.key).encode(), hashlib.sha256).hexdigest()
        return {"X-Auth-Apikey": self.key, "X-Auth-Nonce": nonce_text, "X-Auth-Signature": signature, "Content-Type": "application/json;charset=utf-8", "Accept": "application/json"}

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = self.headers()
        if method.upper() == "GET":
            response = self.http.get(SAFE_BASE_URL + path, headers=headers, timeout=15, **kwargs)
        elif method.upper() == "POST":
            response = self.http.post(SAFE_BASE_URL + path, headers=headers, timeout=15, **kwargs)
        else:
            response = self.http.request(method, SAFE_BASE_URL + path, headers=headers, timeout=15, **kwargs)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = response.text[:500].replace("\n", " ")
            raise RuntimeError(f"SafeTrade API {response.status_code}: {detail}") from exc
        return response.json()

    def balances(self) -> dict[str, Decimal]:
        rows = self.request("GET", "/trade/account/balances/spot")
        return {"PEARL": amount_of(rows, "pearl", "balance"), "USDT": amount_of(rows, "usdt", "balance")}

    def create_ioc(self, side: str, amount: Decimal, price: Decimal) -> Any:
        # SafeTrade 官方现货接口未确认支持 time_in_force=IOC；采用限价单后立即查询并撤销剩余的等效流程。
        payload = {"market": SAFE_MARKET, "side": side, "price": format(price, "f"), "amount": format(amount, "f"), "type": "limit"}
        return self.request("POST", "/trade/market/orders", json=payload)

    def order(self, oid: str) -> Any:
        return self.request("GET", f"/trade/market/orders/{oid}")

    def cancel(self, oid: str) -> Any:
        return self.request("POST", f"/trade/market/orders/{oid}/cancel")


class GateLive:
    name = "Gate"

    def __init__(self, http: requests.Session, key: str, secret: str):
        self.http, self.key, self.secret = http, key, secret

    def headers(self, method: str, path: str, query: str = "", body: str = "") -> dict[str, str]:
        timestamp = str(int(time.time()))
        digest = hashlib.sha512(body.encode()).hexdigest()
        text = "\n".join([method.upper(), path, query, digest, timestamp])
        signature = hmac.new(self.secret.encode(), text.encode(), hashlib.sha512).hexdigest()
        return {"KEY": self.key, "Timestamp": timestamp, "SIGN": signature, "Content-Type": "application/json", "Accept": "application/json"}

    def request(self, method: str, path: str, query: str = "", **kwargs: Any) -> Any:
        body_text = kwargs.pop("body_text", "")
        # Sign and send exactly the same raw JSON bytes.
        if body_text:
            kwargs.pop("json", None)
            kwargs["data"] = body_text
        # Gate 签名必须包含 /api/v4 前缀；请求 URL 的 GATE_BASE_URL 已经包含该前缀。
        headers = self.headers(method, "/api/v4" + path, query, body_text)
        if method.upper() == "GET":
            response = self.http.get(GATE_BASE_URL + path, headers=headers, timeout=15, **kwargs)
        elif method.upper() == "DELETE":
            response = self.http.delete(GATE_BASE_URL + path, headers=headers, timeout=15, **kwargs)
        else:
            response = self.http.request(method, GATE_BASE_URL + path, headers=headers, timeout=15, **kwargs)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = response.text[:500].replace("\n", " ")
            raise RuntimeError(f"Gate API {response.status_code}: {detail}") from exc
        return response.json()

    def balances(self) -> dict[str, Decimal]:
        rows = self.request("GET", "/spot/accounts")
        return {"PEARL": amount_of(rows, "pearl", "available"), "USDT": amount_of(rows, "usdt", "available")}

    def create_ioc(self, side: str, amount: Decimal, price: Decimal) -> Any:
        payload = {"currency_pair": GATE_MARKET, "type": "limit", "account": "spot", "side": side, "amount": format(amount, "f"), "price": format(price, "f"), "time_in_force": "ioc"}
        body = json.dumps(payload, separators=(",", ":"))
        return self.request("POST", "/spot/orders", body_text=body)

    def order(self, oid: str) -> Any:
        return self.request("GET", f"/spot/orders/{oid}", "currency_pair=" + GATE_MARKET, params={"currency_pair": GATE_MARKET})

    def cancel(self, oid: str) -> Any:
        return self.request("DELETE", f"/spot/orders/{oid}", "currency_pair=" + GATE_MARKET, params={"currency_pair": GATE_MARKET})


def live_balances(clients: dict[str, Any]) -> dict[str, dict[str, Decimal]]:
    # 余额预检顺序请求，避免代理或交易所对并发签名请求触发 nonce/限频异常。
    return {name: client.balances() for name, client in clients.items()}


def assert_start_balances(balances: dict[str, dict[str, Decimal]]) -> None:
    errors = []
    for exchange, row in balances.items():
        if row["PEARL"] < MIN_BALANCE:
            errors.append(f"{exchange} PEARL={row['PEARL']} < {MIN_BALANCE}")
        if row["USDT"] < MIN_BALANCE:
            errors.append(f"{exchange} USDT={row['USDT']} < {MIN_BALANCE}")
    if errors:
        raise RuntimeError("真实余额未达到启动门槛：" + "; ".join(errors))


def journal_order(db: PaperDB | None, exchange: str, role: str, event: str, response: Any, amount: Decimal, price: Decimal) -> None:
    if db is None or response is None:
        return
    db.db.execute("""CREATE TABLE IF NOT EXISTS live_order_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
        exchange TEXT NOT NULL, role TEXT NOT NULL, event TEXT NOT NULL,
        order_id TEXT, requested_amount TEXT, requested_price TEXT,
        response TEXT NOT NULL
    )""")
    db.db.execute(
        "INSERT INTO live_order_events(created_at,exchange,role,event,order_id,requested_amount,requested_price,response) VALUES(datetime('now','localtime'),?,?,?,?,?,?,?)",
        (exchange, role, event, order_id(response), str(amount), str(price), json.dumps(response, ensure_ascii=False, default=str)),
    )
    db.db.commit()


def submit_pair(clients: dict[str, Any], buy_exchange: str, sell_exchange: str, amount: Decimal, buy_price: Decimal, sell_price: Decimal, db: PaperDB | None = None) -> tuple[Any, Any]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        buy_job = pool.submit(clients[buy_exchange].create_ioc, "buy", amount, buy_price)
        sell_job = pool.submit(clients[sell_exchange].create_ioc, "sell", amount, sell_price)
        buy_result = sell_result = None
        logged = set()
        try:
            buy_result = buy_job.result()
            journal_order(db, buy_exchange, "buy", "submitted", buy_result, amount, buy_price)
            logged.add(("buy", order_id(buy_result)))
            sell_result = sell_job.result()
            journal_order(db, sell_exchange, "sell", "submitted", sell_result, amount, sell_price)
            logged.add(("sell", order_id(sell_result)))
        except Exception as exc:
            # 一边提交成功、另一边提交失败时，尽力撤销已返回的订单，避免留下裸订单。
            for client, result in ((clients[buy_exchange], buy_result), (clients[sell_exchange], sell_result)):
                if result is not None:
                    cancel_if_open(client, result)
            for job, client in ((buy_job, clients[buy_exchange]), (sell_job, clients[sell_exchange])):
                if job.done() and not job.cancelled():
                    try:
                        result = job.result()
                    except Exception:
                        continue
                    if job is buy_job:
                        buy_result = result
                        key = ("buy", order_id(result))
                        if key not in logged:
                            journal_order(db, buy_exchange, "buy", "submitted", result, amount, buy_price)
                            logged.add(key)
                    else:
                        sell_result = result
                        key = ("sell", order_id(result))
                        if key not in logged:
                            journal_order(db, sell_exchange, "sell", "submitted", result, amount, sell_price)
                            logged.add(key)
                    cancel_if_open(client, result)
            raise RuntimeError(
                "双边提交失败，已尽力撤销另一边未完成订单；"
                f"Safe/Gate订单ID={order_id(buy_result)}/{order_id(sell_result)}；原因={exc}"
            ) from exc
    return buy_result, sell_result


def cancel_if_open(client: Any, result: Any) -> Any:
    oid = order_id(result)
    if not oid:
        return None
    try:
        status = client.order(oid)
        filled = filled_amount(status)
        requested = D(status.get("amount", 0)) if isinstance(status, dict) else D("0")
        state = str(status.get("status", status.get("state", ""))).lower()
        if requested > filled and state in ("open", "wait", "active", "new", "partially_filled"):
            return client.cancel(oid)
    except requests.RequestException:
        return None
    return None


def checked_pair(clients: dict[str, Any], buy_exchange: str, sell_exchange: str, amount: Decimal, buy_price: Decimal, sell_price: Decimal, db: PaperDB | None = None) -> tuple[Decimal, Decimal, Decimal, Decimal, Any, Any]:
    if amount < SAFE_MIN_ORDER_AMOUNT:
        raise RuntimeError(f"SafeTrade PEARL/USDT 最小下单量为 {SAFE_MIN_ORDER_AMOUNT} PEARL，当前请求 {amount} PEARL；未提交订单")
    buy_result = sell_result = None
    try:
        buy_result, sell_result = submit_pair(clients, buy_exchange, sell_exchange, amount, buy_price, sell_price, db)
        buy_id, sell_id = order_id(buy_result), order_id(sell_result)
        if not buy_id or not sell_id:
            raise RuntimeError("订单接口未返回有效订单 ID")
        time.sleep(0.25)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            buy_status = pool.submit(clients[buy_exchange].order, buy_id).result()
            sell_status = pool.submit(clients[sell_exchange].order, sell_id).result()
        journal_order(db, buy_exchange, "buy", "status", buy_status, amount, buy_price)
        journal_order(db, sell_exchange, "sell", "status", sell_status, amount, sell_price)
        buy_filled, sell_filled = filled_amount(buy_status), filled_amount(sell_status)
        buy_cancel = cancel_if_open(clients[buy_exchange], buy_status)
        sell_cancel = cancel_if_open(clients[sell_exchange], sell_status)
        journal_order(db, buy_exchange, "buy", "cancel", buy_cancel, amount, buy_price)
        journal_order(db, sell_exchange, "sell", "cancel", sell_cancel, amount, sell_price)
        if buy_filled != sell_filled:
            print(f"订单状态原始响应：{buy_exchange}={json.dumps(buy_status, ensure_ascii=False, default=str)} | {sell_exchange}={json.dumps(sell_status, ensure_ascii=False, default=str)}")
            raise UnequalFillError(f"两边成交数量不一致：买入={buy_filled}，卖出={sell_filled}；未成交部分已尝试撤销，策略已停止")
        if buy_filled <= 0:
            print(f"订单状态原始响应：{buy_exchange}={json.dumps(buy_status, ensure_ascii=False, default=str)} | {sell_exchange}={json.dumps(sell_status, ensure_ascii=False, default=str)}")
            raise NoFillError("两边订单均未成交，已撤销剩余部分，本轮安全重试")
        return buy_filled, fill_price(buy_status, buy_price), sell_filled, fill_price(sell_status, sell_price), buy_result, sell_result
    except Exception:
        if buy_result is not None:
            cancel_if_open(clients[buy_exchange], buy_result)
        if sell_result is not None:
            cancel_if_open(clients[sell_exchange], sell_result)
        raise


def save_live_state(db: PaperDB, active: list[dict[str, Any]], halted: bool = False) -> None:
    db.save_state("live_active", active)
    db.save_state("live_halted", halted)


def log_live_orders(db: PaperDB, buy_result: Any, sell_result: Any, buy_exchange: str, sell_exchange: str, amount: Decimal, buy_price: Decimal, sell_price: Decimal) -> None:
    db.db.execute("""CREATE TABLE IF NOT EXISTS live_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL,
        buy_exchange TEXT, sell_exchange TEXT, amount TEXT,
        buy_order_id TEXT, sell_order_id TEXT, buy_price TEXT, sell_price TEXT,
        buy_response TEXT, sell_response TEXT
    )""")
    db.db.execute("INSERT INTO live_orders(created_at,buy_exchange,sell_exchange,amount,buy_order_id,sell_order_id,buy_price,sell_price,buy_response,sell_response) VALUES(datetime('now','localtime'),?,?,?,?,?,?,?,?,?)", (buy_exchange, sell_exchange, str(amount), order_id(buy_result), order_id(sell_result), str(buy_price), str(sell_price), json.dumps(buy_result, ensure_ascii=False, default=str), json.dumps(sell_result, ensure_ascii=False, default=str)))
    db.db.commit()


def _run_live_once(args: Any, preflight_only: bool = False, notifier: TelegramNotifier | None = None) -> None:
    def new_session() -> requests.Session:
        session = requests.Session()
        session.trust_env = True
        retry = Retry(total=2, connect=2, read=2, status=0, backoff_factor=0.25, allowed_methods=frozenset({"GET"}), raise_on_status=False)
        adapter = HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=1)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": "safeToGate-live/1.0", "Accept": "application/json", "Connection": "close"})
        return session

    clients = {
        "SafeTrade": SafeLive(new_session(), os.getenv("SAFE_API_KEY", ""), os.getenv("SAFE_API_SECRET", "")),
        "Gate": GateLive(new_session(), os.getenv("GATE_API_KEY", ""), os.getenv("GATE_API_SECRET", "")),
    }
    if not all(client.key and client.secret for client in clients.values()):
        raise RuntimeError("SafeTrade 和 Gate 都必须配置 API Key/Secret")
    state_db = PaperDB(args.db)
    has_active_cycle = bool(state_db.state("live_active"))
    state_db.db.close()
    if not preflight_only and not getattr(args, "live_reset_halt", False):
        guard_db = PaperDB(args.db)
        if bool(guard_db.state("live_halted")):
            guard_db.db.close()
            raise RuntimeError("真实策略已因单边成交/订单异常停止，请先核对订单和余额，再使用 --live-clear-halt 解锁")
        guard_db.db.close()
    try:
        market = MarketClient()
        safe, gate = market.safe_book(), market.gate_book()
        clients["SafeTrade"].sync_clock()
        balances = live_balances(clients)
    except (requests.RequestException, RuntimeError, ValueError) as exc:
        if preflight_only:
            print(f"预检失败：{exc}")
            print("未提交任何订单。请检查代理、SafeTrade API 白名单、系统时间和 API 密钥状态。")
            return
        raise
    try:
        if not has_active_cycle:
            assert_start_balances(balances)
    except RuntimeError as exc:
        if preflight_only:
            print(f"余额：SafeTrade PEARL={balances['SafeTrade']['PEARL']} USDT={balances['SafeTrade']['USDT']} | Gate PEARL={balances['Gate']['PEARL']} USDT={balances['Gate']['USDT']}")
            print(f"预检失败：{exc}")
            print("未读取盘口，未提交任何订单。请先把两边余额恢复到至少 9 PEARL + 9 USDT。")
            return
        raise
    direction, buy_exchange, sell_exchange, spread, buy_price, sell_price = route(safe, gate)
    print("真实接口预检通过：只读余额和行情已成功获取")
    print(f"余额：SafeTrade PEARL={balances['SafeTrade']['PEARL']} USDT={balances['SafeTrade']['USDT']} | Gate PEARL={balances['Gate']['PEARL']} USDT={balances['Gate']['USDT']}")
    print(f"盘口：SafeTrade 买一={safe.bids[0].price} 卖一={safe.asks[0].price} | Gate 买一={gate.bids[0].price} 卖一={gate.asks[0].price}")
    print(f"当前可执行价差={spread}，本次最大使用上限={MAX_USE} PEARL / {MAX_USE} USDT")
    if preflight_only:
        print("预检模式结束：没有提交订单")
        return
    db = PaperDB(args.db)
    active = db.state("live_active") or []
    halted = bool(db.state("live_halted"))
    if halted:
        if getattr(args, "live_reset_halt", False):
            db.save_state("live_halted", False)
            halted = False
            print("已解除真实模式熔断；本轮仍只按行情条件判断，不会强制补仓。")
        else:
            db.db.close()
            raise RuntimeError("真实策略已因单边成交/订单异常停止，请人工检查后使用 --live-reset-halt 解锁")
    if spread <= args.trigger_spread and not active:
        print(f"当前价差未达到 {args.trigger_spread}，不会下单")
        db.db.close()
        return
    if active:
        first = active[0]
        rebuy_exchange = first["sell_exchange"]
        resell_exchange = first["buy_exchange"]
        rebuy_price = gate.asks[0].price if rebuy_exchange == "Gate" else safe.asks[0].price
        resell_price = gate.bids[0].price if resell_exchange == "Gate" else safe.bids[0].price
        reverse_spread = abs(rebuy_price - resell_price)
        estimated = D("0")
        for cycle in active:
            amount = D(cycle["amount"])
            estimated += amount * (D(cycle["entry_sell_price"]) * (D("1") - args.fee) - D(cycle["entry_buy_price"]) * (D("1") + args.fee) + resell_price * (D("1") - args.fee) - rebuy_price * (D("1") + args.fee))
        if reverse_spread <= args.rebalance_spread and estimated > 0:
            total_amount = sum((D(c["amount"]) for c in active), D("0"))
            if total_amount < SAFE_MIN_ORDER_AMOUNT:
                db.db.close()
                raise RuntimeError(f"再平衡数量 {total_amount} PEARL 小于 SafeTrade 最小下单量 {SAFE_MIN_ORDER_AMOUNT} PEARL，未提交订单")
            rebuy_book = gate if rebuy_exchange == "Gate" else safe
            resell_book = gate if resell_exchange == "Gate" else safe
            if rebuy_book.asks[0].amount < total_amount or resell_book.bids[0].amount < total_amount:
                print(f"再平衡取消：盘口一档数量不足，买方可买={rebuy_book.asks[0].amount}，卖方可卖={resell_book.bids[0].amount}，需要={total_amount}")
                db.db.close()
                return
            print(f"真实统一再平衡：数量={total_amount}，预计利润={estimated} USDT")
            try:
                filled_buy, actual_buy_price, filled_sell, actual_sell_price, buy_result, sell_result = checked_pair(clients, rebuy_exchange, resell_exchange, total_amount, rebuy_price, resell_price, db)
            except NoFillError as exc:
                print(f"再平衡本轮未成交：{exc}；保留活动周期，下一轮继续尝试")
                if notifier:
                    notifier.send(f"再平衡未成交\n原因：{exc}\n保留活动周期，下一轮继续尝试")
                db.db.close()
                return
            except Exception as exc:
                save_live_state(db, active, True)
                db.db.close()
                raise RuntimeError(f"再平衡订单请求出现网络或交易所异常，策略已熔断；原始错误：{exc}；请先核对两边订单和余额") from exc
            log_live_orders(db, buy_result, sell_result, rebuy_exchange, resell_exchange, total_amount, actual_buy_price, actual_sell_price)
            if filled_buy != total_amount or filled_sell != total_amount:
                save_live_state(db, active, True)
                raise RuntimeError("再平衡未全部成交，策略已停止")
            for cycle in active:
                amount = D(cycle["amount"])
                profit = amount * (D(cycle["entry_sell_price"]) * (D("1") - args.fee) - D(cycle["entry_buy_price"]) * (D("1") + args.fee) + actual_sell_price * (D("1") - args.fee) - actual_buy_price * (D("1") + args.fee))
                db.finish_cycle(int(cycle["id"]), reverse_spread, actual_buy_price, actual_sell_price, args.fee, profit)
            total_profit = sum((D(c["amount"]) * (D(c["entry_sell_price"]) * (D("1") - args.fee) - D(c["entry_buy_price"]) * (D("1") + args.fee) + actual_sell_price * (D("1") - args.fee) - actual_buy_price * (D("1") + args.fee)) for c in active), D("0"))
            settled_count = len(active)
            save_live_state(db, [], False)
            db.save_state("live_recovery", False)
            print(f"真实再平衡完成：已结算 {settled_count} 个周期")
            if notifier:
                notifier.send(f"套利周期完成\n结算周期：{settled_count}\n本次利润：{total_profit} USDT\n两边库存已再平衡")
            db.db.close()
            return
        if direction == first["direction"] and spread > args.trigger_spread and len(active) < args.max_open_entries:
            pass
        else:
            print(f"真实模式等待：反向绝对价差={reverse_spread}，预计利润={estimated}")
            db.db.close()
            return
    # 入场下单前重新读取 Gate 深度，避免使用旧的买一价格和数量。
    gate = market.gate_book()
    if buy_exchange == "Gate":
        buy_price = gate.asks[0].price
    if sell_exchange == "Gate":
        sell_price = gate.bids[0].price
    spread = sell_price - buy_price
    if spread <= args.trigger_spread:
        print(f"下单前 Gate 深度刷新后价差为 {spread}，低于入场阈值 {args.trigger_spread}，本轮不下单")
        db.db.close()
        return
    capped_base = min(MAX_USE, balances[sell_exchange]["PEARL"])
    capped_quote = min(MAX_USE, balances[buy_exchange]["USDT"])
    # 买入需要额外支付手续费，不能只用 USDT / price 计算，否则交易所会因余额不足拒单。
    amount = min(args.amount, capped_base, capped_quote / (buy_price * (D("1") + args.fee)))
    if active and amount < args.amount:
        db.save_state("live_recovery", True)
        print(f"库存恢复模式：当前可用数量仅 {amount} PEARL，无法支持完整 {args.amount} PEARL 双边套利；等待反向价差 <= {args.rebalance_spread} 且预计利润 > 0 后恢复库存")
        if notifier:
            notifier.send(f"进入库存恢复模式\n当前可用数量：{amount} PEARL\n目标交易数量：{args.amount} PEARL\n等待反向价差 <= {args.rebalance_spread} 且预计利润 > 0")
        db.db.close()
        return
    if amount <= 0:
        if active:
            print("追加入场跳过：当前可用余额不足，保留已有周期等待再平衡")
            db.db.close()
            return
        db.db.close()
        raise RuntimeError("按 10 PEARL/10 USDT 使用上限计算后，可下单数量为 0")
    if amount < SAFE_MIN_ORDER_AMOUNT:
        if active:
            print(f"追加入场跳过：可用数量 {amount} 小于最小下单量 {SAFE_MIN_ORDER_AMOUNT}，保留已有周期等待再平衡")
            db.db.close()
            return
        db.db.close()
        raise RuntimeError(f"可下单数量 {amount} PEARL 小于 SafeTrade 最小下单量 {SAFE_MIN_ORDER_AMOUNT} PEARL，未提交订单")
    buy_book = gate if buy_exchange == "Gate" else safe
    sell_book = gate if sell_exchange == "Gate" else safe
    if buy_book.asks[0].amount < amount or sell_book.bids[0].amount < amount:
        print(f"入场取消：盘口一档数量不足，买方可买={buy_book.asks[0].amount}，卖方可卖={sell_book.bids[0].amount}，需要={amount}")
        db.db.close()
        return
    print(f"准备真实 IOC 限价双边下单：{buy_exchange} 买入 {amount} @ {buy_price}；{sell_exchange} 卖出 {amount} @ {sell_price}")
    try:
        filled_buy, actual_buy_price, filled_sell, actual_sell_price, buy_result, sell_result = checked_pair(clients, buy_exchange, sell_exchange, amount, buy_price, sell_price, db)
    except NoFillError as exc:
        print(f"本轮双边订单均未成交：{exc}；下一轮继续尝试")
        if notifier:
            notifier.send(f"本轮套利未成交\n原因：{exc}\n下一轮继续尝试")
        db.db.close()
        return
    except Exception as exc:
        # 已有活动周期时，余额不足只表示本轮无法追加，不应熔断已有周期。
        if active and "insufficient_balance" in str(exc).lower():
            print(f"追加入场跳过：{exc}；保留已有周期，等待反向价差再平衡")
            db.db.close()
            return
        save_live_state(db, active, True)
        db.db.close()
        raise RuntimeError(f"入场订单请求出现网络或交易所异常，策略已熔断；原始错误：{exc}；请先核对两边订单和余额") from exc
    log_live_orders(db, buy_result, sell_result, buy_exchange, sell_exchange, amount, actual_buy_price, actual_sell_price)
    if filled_buy != filled_sell:
        save_live_state(db, active, True)
        db.db.close()
        raise RuntimeError("两边成交数量不一致，策略已停止")
    cycle_id = db.start_cycle(direction, filled_buy, spread, actual_buy_price, actual_sell_price, args.fee)
    active.append({"id": cycle_id, "direction": direction, "amount": str(filled_buy), "buy_exchange": buy_exchange, "sell_exchange": sell_exchange, "entry_buy_price": str(actual_buy_price), "entry_sell_price": str(actual_sell_price)})
    save_live_state(db, active, False)
    entry_estimate = filled_buy * (actual_sell_price * (D("1") - args.fee) - actual_buy_price * (D("1") + args.fee))
    if notifier:
        notifier.send(f"套利入场成功\n买入：{buy_exchange} {filled_buy} PEARL @ {actual_buy_price}\n卖出：{sell_exchange} {filled_sell} PEARL @ {actual_sell_price}\n本阶段预计收益：{entry_estimate} USDT\n等待反向价差 <= {args.rebalance_spread} 再平衡")
    print(f"真实入场完成：买入 {filled_buy} @ {actual_buy_price}，卖出 {filled_sell} @ {actual_sell_price}；已开周期={len(active)}")
    db.db.close()


def run_live(args: Any, preflight_only: bool = False) -> None:
    notifier = TelegramNotifier()
    notifier.send("套利程序已启动\n模式：只读预检" if preflight_only else "套利程序已启动\n模式：真实交易")
    if preflight_only:
        try:
            _run_live_once(args, preflight_only=True, notifier=notifier)
        finally:
            notifier.send("套利程序已停止\n预检结束，未提交订单")
        return
    try:
        while True:
            try:
                _run_live_once(args, preflight_only=False, notifier=notifier)
            except (requests.RequestException, RuntimeError, ValueError, ArithmeticError) as exc:
                print(f"真实模式错误：{exc}")
                notifier.send(f"套利失败\n原因：{exc}")
                message = str(exc)
                if any(token in message for token in ("熔断", "已因单边成交", "真实余额未达到", "nonce_expired", "ConnectionResetError", "SafeTrade unavailable")):
                    break
            if getattr(args, "once", False):
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n真实模式已停止；未自动补仓或提交额外订单。")

    finally:
        notifier.send("套利程序已停止\n未自动补仓，程序已退出")
