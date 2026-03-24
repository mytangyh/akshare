# AKShare A 股实时行情 WebSocket 接口

## 说明

AKShare 本身提供的是数据抓取能力，不直接提供消息订阅协议。
当前示例在 `AStockQuoteSubscriptionService` 之上封装了一层 FastAPI WebSocket 接口，
用于实现最基础的行情订阅和取消订阅能力。

默认底层行情源使用 `stock_bid_ask_em`，按单只 A 股代码轮询并向客户端推送快照。

## 安装

```shell
pip install "akshare[realtime]"
```

## 启动

```python
from akshare.service.quote_websocket import run_quote_websocket_app

run_quote_websocket_app(host="127.0.0.1", port=8000, poll_interval=1.0)
```

服务启动后：

- 健康检查：`GET /healthz`
- WebSocket 地址：`ws://127.0.0.1:8000/ws/quotes`

## 请求协议

订阅：

```json
{"action": "subscribe", "symbol": "000001"}
```

取消订阅：

```json
{"action": "unsubscribe", "symbol": "000001"}
```

或者：

```json
{"action": "unsubscribe", "subscription_id": "your-subscription-id"}
```

心跳：

```json
{"action": "ping"}
```

## 响应协议

订阅成功：

```json
{
  "type": "subscribed",
  "symbol": "000001",
  "subscription_id": "9d7b4f4f6be14956a72ef4d0c988de90",
  "reused": false
}
```

行情推送：

```json
{
  "type": "quote",
  "symbol": "000001",
  "subscription_id": "9d7b4f4f6be14956a72ef4d0c988de90",
  "data": {
    "symbol": "000001",
    "name": "平安银行",
    "price": 10.55,
    "change": 0.05,
    "change_pct": 0.48,
    "volume": 872663.0,
    "amount": 910278600.0,
    "open_price": 10.48,
    "high_price": 10.57,
    "low_price": 10.47,
    "prev_close": 10.50,
    "turnover_rate": 0.45,
    "source_time": "09:30:01",
    "captured_at": "2026-03-24T06:10:00+00:00",
    "raw": {}
  }
}
```

取消成功：

```json
{
  "type": "unsubscribed",
  "symbol": "000001",
  "subscription_id": "9d7b4f4f6be14956a72ef4d0c988de90",
  "removed": true
}
```

## 备注

1. 同一个 WebSocket 连接内，对同一股票重复订阅会复用已有订阅关系。
2. 当连接关闭时，服务端会自动清理该连接对应的全部订阅。
3. 该实现是最小可用版本，适合作为你后续接入认证、用户会话、限流和多节点广播的基础层。
