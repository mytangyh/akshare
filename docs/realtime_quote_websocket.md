# AKShare A 股实时行情 WebSocket 接口说明

## 适用范围

本文档描述当前分支中的 A 股实时行情推送服务实现，适合提供给其他 AI 或自动化系统作为接口约定使用。

该服务不是 AKShare 原生提供的订阅协议，而是在 AKShare 数据抓取能力之上封装的一层 WebSocket 服务。

## 服务能力

- 支持 WebSocket 行情订阅
- 支持取消订阅
- 支持同一连接内多股票订阅
- 服务端汇总全部活跃股票代码后统一轮询，避免重复请求上游
- 支持通过环境变量切换行情源提供方
- 上游失败时自动切换备用数据源
- 行情未变化时默认不重复推送

## 接口地址

- 健康检查: `GET /healthz`
- WebSocket: `ws://<host>:8000/ws/quotes`

## 服务部署信息

### 当前已部署实例

以下信息对应 2026-03-24 已部署的线上实例:

- 公网服务器 IP: `122.51.144.85`
- 公网健康检查: `http://122.51.144.85:8000/healthz`
- 公网 WebSocket: `ws://122.51.144.85:8000/ws/quotes`
- 部署目录: `/root/akshare-websocket`
- 启动方式: `docker-compose up -d --build`
- 当前协议: `ws`
- 当前未配置域名、HTTPS、鉴权

### 服务器环境

- 操作系统: `Debian 12`
- 容器运行时: `docker.io`
- 编排工具: `docker-compose` `1.29.2`
- Python 依赖安装默认使用阿里云 PyPI 镜像
- Docker 镜像拉取已配置腾讯云镜像加速

### Docker Compose 关键信息

- Compose 文件: `docker-compose.yml`
- 服务名: `quote-websocket`
- 镜像名: `akshare-quote-websocket:dev`
- 容器内监听端口: `8000`
- 默认宿主机映射端口: `8000:8000`
- 重启策略: `unless-stopped`
- 健康检查地址: `http://127.0.0.1:8000/healthz`
- 已配置 `host.docker.internal:host-gateway`，便于容器访问宿主机上的 OpenD

### 可配置环境变量

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| `QUOTE_PROVIDER` | `default` | 行情源提供方，当前支持 `default`、`web`、`futu` |
| `QUOTE_POLL_INTERVAL` | `3.0` | 行情轮询基础间隔，单位秒 |
| `QUOTE_LOG_LEVEL` | `INFO` | 服务日志级别 |
| `QUOTE_SERVICE_PORT` | `8000` | 宿主机暴露端口 |
| `FUTU_OPEND_HOST` | `host.docker.internal` | Futu OpenD 地址 |
| `FUTU_OPEND_PORT` | `11111` | Futu OpenD 端口 |
| `PIP_INDEX_URL` | `https://mirrors.aliyun.com/pypi/simple/` | Python 包安装镜像源 |
| `PIP_TRUSTED_HOST` | `mirrors.aliyun.com` | Python 包安装可信主机 |

### Futu 模式部署要求

当 `QUOTE_PROVIDER=futu` 时：

- 服务端不再使用东方财富/雪球作为默认源
- 服务会通过 `futu-api` 连接外部 OpenD
- 需要提前启动并登录 OpenD
- 容器必须能访问 `FUTU_OPEND_HOST:FUTU_OPEND_PORT`

推荐部署方式：

1. 在宿主机或另一台内网机器上启动 OpenD。
2. 设置 `QUOTE_PROVIDER=futu`。
3. 设置 `FUTU_OPEND_HOST` 和 `FUTU_OPEND_PORT`。
4. 执行 `docker-compose up -d --build`。

示例：

```shell
export QUOTE_PROVIDER=futu
export FUTU_OPEND_HOST=host.docker.internal
export FUTU_OPEND_PORT=11111
docker-compose up -d --build
```

### 镜像入口

- Dockerfile: `Dockerfile.websocket`
- 容器启动命令: `uvicorn app.main:app --host 0.0.0.0 --port 8000`
- Python 入口文件: `app/main.py`

### 常用运维命令

```shell
cd /root/akshare-websocket
docker-compose ps
docker-compose logs -f
docker-compose up -d --build
docker-compose restart
```

### 对其他 AI 的部署判断建议

- 如果访问 `http://122.51.144.85:8000/healthz` 返回 `status=ok`，可认为服务在线
- 如果 WebSocket 握手成功且 `subscribe` 后收到 `subscribed` 消息，说明网关层与应用层均正常
- 如果公网不可访问，应优先检查云服务器安全组、系统防火墙和 `8000` 端口监听状态
- 如果订阅成功但没有 `quote` 消息，应继续检查上游行情源是否超时、被限流或返回空数据
- 不要假设服务器已经配置了反向代理、TLS 证书或身份认证

## 股票代码规则

- 输入支持 `000001`、`SZ000001`、`SH600000`、`BJ430047`
- 服务端会标准化为 6 位纯数字代码
- 当前仅面向 A 股场景

## 请求协议

订阅:

```json
{"action": "subscribe", "symbol": "000001"}
```

取消订阅:

```json
{"action": "unsubscribe", "symbol": "000001"}
```

或者:

```json
{"action": "unsubscribe", "subscription_id": "9d7b4f4f6be14956a72ef4d0c988de90"}
```

心跳:

```json
{"action": "ping"}
```

## 响应协议

订阅成功:

```json
{
  "type": "subscribed",
  "symbol": "000001",
  "subscription_id": "9d7b4f4f6be14956a72ef4d0c988de90",
  "reused": false
}
```

行情推送:

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
    "prev_close": 10.5,
    "turnover_rate": 0.45,
    "source_time": null,
    "captured_at": "2026-03-24T06:10:00+00:00",
    "raw": {
      "代码": "000001",
      "名称": "平安银行",
      "最新": 10.55,
      "涨跌": 0.05,
      "涨幅": 0.48,
      "总手": 872663,
      "金额": 910278600,
      "今开": 10.48,
      "最高": 10.57,
      "最低": 10.47,
      "昨收": 10.5,
      "换手": 0.45,
      "量比": 1.22,
      "均价": 10.52
    }
  }
}
```

取消订阅成功:

```json
{
  "type": "unsubscribed",
  "symbol": "000001",
  "subscription_id": "9d7b4f4f6be14956a72ef4d0c988de90",
  "removed": true
}
```

错误响应:

```json
{
  "type": "error",
  "message": "unsupported action",
  "supported_actions": ["subscribe", "unsubscribe", "ping"]
}
```

## 标准行情字段

`quote.data` 中这些字段可视为当前实现的稳定输出:

| 字段 | 类型 | 含义 | 说明 |
| --- | --- | --- | --- |
| `symbol` | `str` | 股票代码 | 标准化后的 6 位代码 |
| `name` | `str \| null` | 股票名称 | 如 `平安银行` |
| `price` | `float \| null` | 最新价 | 对应 `最新`、`最新价` 或 `现价` |
| `change` | `float \| null` | 涨跌额 | 对应 `涨跌` 或 `涨跌额` |
| `change_pct` | `float \| null` | 涨跌幅 | 百分比数值，不带 `%` |
| `volume` | `float \| null` | 成交量 | 来自 `总手` 或 `成交量` |
| `amount` | `float \| null` | 成交额 | 来自 `金额` 或 `成交额` |
| `open_price` | `float \| null` | 今开 | 来自 `今开` 或 `开盘` |
| `high_price` | `float \| null` | 最高价 |  |
| `low_price` | `float \| null` | 最低价 |  |
| `prev_close` | `float \| null` | 昨收价 |  |
| `turnover_rate` | `float \| null` | 换手率 | 来自 `换手`、`换手率` 或 `周转率` |
| `source_time` | `str \| null` | 上游返回的行情时间 | 批量源多数情况下为空 |
| `captured_at` | `str` | 服务端抓取时间 | ISO 8601 UTC 时间字符串 |
| `raw` | `dict[str, Any]` | 原始字段集合 | 字段不稳定，依赖具体数据源 |

## `raw` 字段说明

`raw` 用于保留上游的原始字段。下游 AI 如果只需要稳定协议，应优先使用标准字段，不要把 `raw` 当成固定契约。

### 默认批量源中的常见字段

当前默认批量源为东方财富批量接口，通常会在 `raw` 中出现:

- `代码`
- `名称`
- `最新`
- `涨跌`
- `涨幅`
- `总手`
- `金额`
- `今开`
- `最高`
- `最低`
- `昨收`
- `换手`
- `量比`
- `均价`

### 东方财富单股兜底源中的额外字段

当批量源失败或缺失时，会尝试东方财富单股接口。此时 `raw` 还可能包含:

- `buy_1` 到 `buy_5`
- `buy_1_vol` 到 `buy_5_vol`
- `sell_1` 到 `sell_5`
- `sell_1_vol` 到 `sell_5_vol`
- `涨停`
- `跌停`
- `外盘`
- `内盘`

### 雪球兜底源中的额外字段

如果东方财富也失败，会继续尝试雪球个股接口。此时 `raw` 还可能包含:

- `现价`
- `成交量`
- `成交额`
- `振幅`
- `均价`
- `时间`
- `市净率`
- `市盈率(TTM)`
- `市盈率(静)`
- `市盈率(动)`
- `资产净值/总市值`
- `流通值`
- `流通股`
- `52周最高`
- `52周最低`
- `今年以来涨幅`
- `涨停`
- `跌停`

### Futu 源中的常见字段

当 `QUOTE_PROVIDER=futu` 时，`raw` 通常会包含:

- `代码`
- `Futu代码`
- `名称`
- `最新`
- `涨跌`
- `涨幅`
- `总手`
- `金额`
- `今开`
- `最高`
- `最低`
- `昨收`
- `换手`
- `振幅`
- `时间`
- `停牌`
- `状态`

## 服务端轮询逻辑

当前默认行为:

1. 使用一个全局轮询任务，而不是每只股票一个轮询任务。
2. 每轮汇总全部活跃订阅代码，按批次统一请求上游。
3. 默认基础轮询间隔为 `3s`。
4. 默认批次大小为 `50`。
5. 如果整轮请求全部失败，轮询间隔按指数退避增加，最大到 `30s`。
6. 如果行情关键字段未变化，默认不重复推送。

去重依据:

- `price`
- `change`
- `change_pct`
- `volume`
- `amount`
- `source_time`

## 上游数据源优先级

### 默认模式

当前顺序如下:

1. 东方财富批量接口
2. 东方财富单股接口
3. 雪球个股接口

这意味着:

- 多股票同时订阅时，会优先走批量接口
- 个别股票在批量返回缺失时，才会走单股兜底
- `raw` 字段内容会随实际命中的数据源不同而变化

### Futu 模式

当 `QUOTE_PROVIDER=futu` 时：

1. 使用 Futu OpenAPI `subscribe(..., QUOTE)` 订阅所需代码
2. 使用 `get_stock_quote(...)` 批量拉取已订阅报价
3. 默认不再附加东方财富/雪球网页兜底

## 对其他 AI 的使用建议

- 先判断消息的 `type`
- 只有 `type == "quote"` 时再解析 `data`
- 优先读取标准字段，不要默认 `raw` 一定有某个键
- 不要假设 `source_time` 一定存在
- 不要假设所有行情消息都按固定 3 秒严格到达
- 服务端默认会做去重，所以行情不变化时可能不会推送新消息
- 如果需要盘口五档、市盈率、量比、均价等扩展字段，应先检查 `raw`
- 如果部署在云服务器且网页源被风控，优先考虑 `QUOTE_PROVIDER=futu`

## 当前实现边界

- 这是单机版 WebSocket 服务，没有做鉴权
- 没有做用户级权限隔离
- 没有做分布式广播
- 没有做更细粒度的上游限流与熔断
- `raw` 字段属于透传层，不保证长期稳定

## 参考实现位置

- WebSocket 入口: `akshare/service/quote_websocket.py`
- 订阅与轮询核心: `akshare/service/quote_subscription.py`
- Futu 行情源: `akshare/service/quote_futu.py`
- 东方财富单股源: `akshare/stock/stock_ask_bid_em.py`
- 雪球个股源: `akshare/stock/stock_xq.py`
