# Pearl/USDT 跨交易所套利 Demo

这是一个面向学习和测试的 Python demo，监控 SafeTrade 与 Gate 的 `PEARL/USDT` 订单簿，寻找“SafeTrade 买入、Gate 卖出”或反方向的价差。

默认是 **模拟模式**：只读取公开行情并打印信号，不会下单。真实交易需要同时满足：

1. 两个平台都有足够的 PEARL/USDT 库存（跨所套利通常需要预先在两边备货，不能临时等待链上转账）。
2. API Key 仅开启必要权限，禁止提现权限，并先用小额/模拟盘验证。
3. 明确设置手续费、最小成交量、滑点和最小净利润阈值。

## 运行

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
python arbitrage_demo.py --once
python arbitrage_demo.py --interval 5
```

如果只想看 Gate 模拟盘，设置 `GATE_BASE_URL=https://api-testnet.gateapi.io/api/v4`。

## 参数

```powershell
python arbitrage_demo.py --interval 3 --amount 10 --min-profit 0.008
python arbitrage_demo.py --once --json
```

后续主入口使用：

```powershell
python main.py --once --show-balances
python main.py --interval 5 --amount 10 --min-profit 0.008
```

当前 `main.py` 是严格的纸上交易模式，只读取公开盘口，不调用任何下单接口。默认假设两边各有 10 PEARL 和 10 USDT：价差大于 0.01 时可以连续模拟买低卖高，默认最多 5 个周期；之后等待反向恢复库存的绝对价差小于等于 0.005，再一次性模拟反向交易，把两边 PEARL 恢复到原目标，并把每个周期利润写入 `arbitrage.db`。

```powershell
python main.py --interval 5 --amount 1 --trigger-spread 0.01 --rebalance-spread 0.005 --max-open-entries 5
```

完全重置纸上测试数据为两边各 10 PEARL 和 10 USDT，并清空历史周期：

```powershell
python main.py --reset
```

SQLite 表 `cycles` 保存每次入场价、平衡价、手续费、完成时间和 `profit_usdt`；`bot_state` 保存纸上余额和未完成周期，程序重启后可以继续等待平衡。

新版会读取两边现货余额和账户 taker 费率，按照订单簿多档深度计算成交均价，并在余额不足时不发出套利信号。真实下单需要同时使用 `--live --confirm-live`，并在 `.env` 设置 `ENABLE_LIVE_TRADING=true`。

套利路径是预充值双边库存：SafeTrade 和 Gate 两边都提前准备 PEARL 与 USDT。发现价差后，在低价交易所买入 PEARL，同时在高价交易所卖出 PEARL。不要在套利热路径中临时提现；提现确认时间和链上到账时间通常超过价差存在时间。提现只用于低频库存再平衡。

- `--amount`：每次尝试的 PEARL 数量。
- `--min-profit`：最低净利润率，例如 `0.008` 表示 0.8%。
- `--once`：只抓取一次行情。
- `--live`：启用真实下单保护开关；没有 `--live` 时绝不会下单。

## 重要限制

Demo 使用盘口最优买一/卖一做估算，不代表实际可成交价格。生产系统还必须处理盘口深度、部分成交、订单超时、余额锁定、断线重连、限频、异常回滚和审计日志。跨交易所套利也会有库存风险、API 风险和滑点风险。

