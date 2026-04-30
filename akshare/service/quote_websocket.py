#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/24 14:10
Desc: A 股实时行情 WebSocket 接口
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
import logging
import os
from contextlib import asynccontextmanager, suppress
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from .ifind_minute_series import IFindMinuteSeriesService, MinuteSeriesSnapshot
from .quote_subscription import AStockQuoteSubscriptionService, QuoteSnapshot

LOGGER = logging.getLogger(__name__)

if not LOGGER.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )
    LOGGER.addHandler(_handler)
    LOGGER.propagate = False
LOGGER.setLevel(
    getattr(logging, os.getenv("QUOTE_LOG_LEVEL", "INFO").upper(), logging.INFO)
)

_OUTGOING_QUEUE_MAXSIZE = 1000
_BEIJING_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class _MinuteSeriesLiveHandle:
    subscription_id: str
    symbol: str
    task: asyncio.Task[None]


def _create_default_quote_service(
    poll_interval: float,
) -> AStockQuoteSubscriptionService:
    provider = os.getenv("QUOTE_PROVIDER", "default").strip().lower()
    if provider in {"default", "web"}:
        LOGGER.info("quote provider selected provider=%s", provider)
        return AStockQuoteSubscriptionService(poll_interval=poll_interval)

    if provider == "futu":
        from .quote_futu import FutuQuoteBatchFetcher

        host = os.getenv("FUTU_OPEND_HOST", "127.0.0.1").strip() or "127.0.0.1"
        raw_port = os.getenv("FUTU_OPEND_PORT", "11111")
        try:
            port = int(raw_port)
        except ValueError as err:
            raise ValueError("FUTU_OPEND_PORT 必须是整数") from err
        if port <= 0:
            raise ValueError("FUTU_OPEND_PORT 必须大于 0")

        LOGGER.info(
            "quote provider selected provider=futu host=%s port=%s",
            host,
            port,
        )
        return AStockQuoteSubscriptionService(
            batch_fetcher=FutuQuoteBatchFetcher(host=host, port=port),
            fallback_fetchers=[],
            poll_interval=poll_interval,
        )

    if provider == "ifind":
        from .quote_ifind import IFindQuoteBatchFetcher

        access_token = os.getenv("IFIND_ACCESS_TOKEN", "").strip()
        refresh_token = os.getenv("IFIND_REFRESH_TOKEN", "").strip()
        raw_timeout = os.getenv("IFIND_REQUEST_TIMEOUT", "10")
        raw_skip_off_hours = os.getenv("IFIND_SKIP_OFF_HOURS_REQUESTS", "1").strip()
        try:
            request_timeout = float(raw_timeout)
        except ValueError as err:
            raise ValueError("IFIND_REQUEST_TIMEOUT 必须是数字") from err
        if request_timeout <= 0:
            raise ValueError("IFIND_REQUEST_TIMEOUT 必须大于 0")
        skip_off_hours_requests = raw_skip_off_hours not in {"0", "false", "False"}

        LOGGER.info(
            "quote provider selected provider=ifind has_access_token=%s has_refresh_token=%s request_timeout=%s skip_off_hours_requests=%s",
            bool(access_token),
            bool(refresh_token),
            request_timeout,
            skip_off_hours_requests,
        )
        return AStockQuoteSubscriptionService(
            batch_fetcher=IFindQuoteBatchFetcher(
                access_token=access_token,
                refresh_token=refresh_token,
                request_timeout=request_timeout,
                skip_off_hours_requests=skip_off_hours_requests,
            ),
            fallback_fetchers=[],
            poll_interval=poll_interval,
            suppress_duplicate=False,
        )

    raise ValueError(
        "QUOTE_PROVIDER 仅支持 default、web、futu、ifind"
    )


def _create_default_minute_series_service() -> IFindMinuteSeriesService | None:
    provider = os.getenv("QUOTE_PROVIDER", "default").strip().lower()
    if provider != "ifind":
        LOGGER.info(
            "minute series service disabled provider=%s reason=provider_not_ifind",
            provider,
        )
        return None

    access_token = os.getenv("IFIND_ACCESS_TOKEN", "").strip()
    refresh_token = os.getenv("IFIND_REFRESH_TOKEN", "").strip()
    raw_timeout = os.getenv("IFIND_REQUEST_TIMEOUT", "10")
    try:
        request_timeout = float(raw_timeout)
    except ValueError as err:
        raise ValueError("IFIND_REQUEST_TIMEOUT 必须是数字") from err
    if request_timeout <= 0:
        raise ValueError("IFIND_REQUEST_TIMEOUT 必须大于 0")

    LOGGER.info(
        "minute series service enabled provider=ifind has_access_token=%s has_refresh_token=%s request_timeout=%s",
        bool(access_token),
        bool(refresh_token),
        request_timeout,
    )
    return IFindMinuteSeriesService(
        access_token=access_token,
        refresh_token=refresh_token,
        request_timeout=request_timeout,
    )


def create_quote_websocket_app(
    service: AStockQuoteSubscriptionService | None = None,
    minute_series_service: IFindMinuteSeriesService | None = None,
    poll_interval: float = 3.0,
):
    """
    创建 FastAPI WebSocket 应用
    """

    FastAPI, WebSocket, WebSocketDisconnect = _load_fastapi()

    quote_service = service or _create_default_quote_service(
        poll_interval=poll_interval
    )
    minute_series_service = minute_series_service or _create_default_minute_series_service()

    @asynccontextmanager
    async def lifespan(app):
        LOGGER.info(
            "quote websocket app started poll_interval=%s",
            quote_service.current_poll_interval,
        )
        try:
            yield
        finally:
            LOGGER.info("quote websocket app stopping")
            await quote_service.stop()
            if minute_series_service is not None:
                await minute_series_service.stop()
            LOGGER.info("quote websocket app stopped")

    app = FastAPI(
        title="AKShare A-Stock Quote WebSocket",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.quote_service = quote_service
    app.state.minute_series_service = minute_series_service

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {
            "status": "ok",
            "active_symbols": list(quote_service.active_symbols),
            "subscriptions": quote_service.subscription_size,
            "minute_series_enabled": minute_series_service is not None,
        }

    @app.websocket("/ws/quotes")
    async def quotes_websocket(websocket: WebSocket) -> None:
        connection_id = uuid4().hex[:8]
        client = _client_repr(websocket)
        await websocket.accept()
        LOGGER.info(
            "websocket connected connection_id=%s client=%s",
            connection_id,
            client,
        )
        outgoing_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=_OUTGOING_QUEUE_MAXSIZE
        )
        sender_task = asyncio.create_task(
            _sender_loop(
                websocket=websocket,
                outgoing_queue=outgoing_queue,
                connection_id=connection_id,
                client=client,
            )
        )
        subscriptions_by_symbol: dict[str, Any] = {}
        forwarders_by_symbol: dict[str, asyncio.Task[None]] = {}
        minute_series_live_handles: dict[str, _MinuteSeriesLiveHandle] = {}
        minute_series_subscription_index: dict[str, str] = {}

        try:
            while True:
                receive_task = asyncio.create_task(websocket.receive_json())
                done, _ = await asyncio.wait(
                    {receive_task, sender_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if sender_task in done:
                    receive_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await receive_task
                    break

                payload = await receive_task
                LOGGER.info(
                    "websocket request received connection_id=%s client=%s payload=%s",
                    connection_id,
                    client,
                    _summarize_payload(payload),
                )
                try:
                    action = str(payload.get("action", "")).strip().lower()

                    if action == "subscribe":
                        symbol = payload.get("symbol")
                        normalized_symbol = quote_service._normalize_symbol(symbol)

                        current_subscription = subscriptions_by_symbol.get(
                            normalized_symbol
                        )
                        if current_subscription is not None:
                            LOGGER.info(
                                "websocket subscribe reused connection_id=%s client=%s symbol=%s subscription_id=%s",
                                connection_id,
                                client,
                                normalized_symbol,
                                current_subscription.subscription_id,
                            )
                            _offer_message(
                                outgoing_queue,
                                {
                                    "type": "subscribed",
                                    "symbol": normalized_symbol,
                                    "subscription_id": current_subscription.subscription_id,
                                    "reused": True,
                                },
                            )
                            continue

                        subscription = await quote_service.subscribe(normalized_symbol)
                        subscriptions_by_symbol[normalized_symbol] = subscription
                        LOGGER.info(
                            "websocket subscribed connection_id=%s client=%s symbol=%s subscription_id=%s active_connection_subscriptions=%s",
                            connection_id,
                            client,
                            normalized_symbol,
                            subscription.subscription_id,
                            len(subscriptions_by_symbol),
                        )
                        _offer_message(
                            outgoing_queue,
                            {
                                "type": "subscribed",
                                "symbol": normalized_symbol,
                                "subscription_id": subscription.subscription_id,
                                "reused": False,
                            },
                        )
                        forwarders_by_symbol[normalized_symbol] = asyncio.create_task(
                            _forward_subscription(
                                subscription=subscription,
                                outgoing_queue=outgoing_queue,
                            )
                        )
                        continue

                    if action == "unsubscribe":
                        result = await _unsubscribe_by_payload(
                            payload=payload,
                            quote_service=quote_service,
                            subscriptions_by_symbol=subscriptions_by_symbol,
                            forwarders_by_symbol=forwarders_by_symbol,
                        )
                        LOGGER.info(
                            "websocket unsubscribed connection_id=%s client=%s symbol=%s subscription_id=%s removed=%s active_connection_subscriptions=%s",
                            connection_id,
                            client,
                            result.get("symbol"),
                            result.get("subscription_id"),
                            result.get("removed"),
                            len(subscriptions_by_symbol),
                        )
                        _offer_message(outgoing_queue, result)
                        continue

                    if action == "subscribe_minute_series":
                        if minute_series_service is None:
                            raise ValueError(
                                "当前 QUOTE_PROVIDER 不支持分钟序列订阅，仅 ifind 可用"
                            )
                        raw_timestamp = payload.get("timestamp")
                        if not raw_timestamp:
                            raise ValueError(
                                "subscribe_minute_series requires timestamp"
                            )
                        normalized_symbol = minute_series_service.normalize_symbol(
                            payload.get("symbol")
                        )
                        current_handle = minute_series_live_handles.get(normalized_symbol)
                        if current_handle is not None:
                            await _unsubscribe_minute_series_symbol(
                                symbol=normalized_symbol,
                                live_handles_by_symbol=minute_series_live_handles,
                                subscription_index=minute_series_subscription_index,
                            )

                        snapshot = await minute_series_service.get_snapshot(
                            symbol=normalized_symbol,
                            request_timestamp=str(raw_timestamp),
                        )
                        subscription_id = None
                        if snapshot.live:
                            subscription_id = uuid4().hex
                            task = asyncio.create_task(
                                _forward_minute_series(
                                    symbol=normalized_symbol,
                                    subscription_id=subscription_id,
                                    minute_series_service=minute_series_service,
                                    outgoing_queue=outgoing_queue,
                                    initial_last_timestamp=snapshot.last_timestamp,
                                )
                            )
                            minute_series_live_handles[normalized_symbol] = (
                                _MinuteSeriesLiveHandle(
                                    subscription_id=subscription_id,
                                    symbol=normalized_symbol,
                                    task=task,
                                )
                            )
                            minute_series_subscription_index[subscription_id] = (
                                normalized_symbol
                            )

                        _offer_message(
                            outgoing_queue,
                            {
                                "type": "subscribed_minute_series",
                                "symbol": normalized_symbol,
                                "subscription_id": subscription_id,
                                "live": snapshot.live,
                            },
                        )
                        _offer_message(
                            outgoing_queue,
                            _minute_series_snapshot_to_message(
                                snapshot=snapshot,
                                subscription_id=subscription_id,
                            ),
                        )
                        continue

                    if action == "unsubscribe_minute_series":
                        result = await _unsubscribe_minute_series_by_payload(
                            payload=payload,
                            minute_series_service=minute_series_service,
                            live_handles_by_symbol=minute_series_live_handles,
                            subscription_index=minute_series_subscription_index,
                        )
                        _offer_message(outgoing_queue, result)
                        continue

                    if action == "ping":
                        LOGGER.info(
                            "websocket ping connection_id=%s client=%s",
                            connection_id,
                            client,
                        )
                        _offer_message(outgoing_queue, {"type": "pong"})
                        continue

                    LOGGER.warning(
                        "websocket unsupported action connection_id=%s client=%s action=%s",
                        connection_id,
                        client,
                        action,
                    )
                    _offer_message(
                        outgoing_queue,
                        {
                            "type": "error",
                            "message": "unsupported action",
                            "supported_actions": [
                                "subscribe",
                                "unsubscribe",
                                "subscribe_minute_series",
                                "unsubscribe_minute_series",
                                "ping",
                            ],
                        },
                    )
                except ValueError as err:
                    LOGGER.warning(
                        "websocket request rejected connection_id=%s client=%s error=%s payload=%s",
                        connection_id,
                        client,
                        err,
                        _summarize_payload(payload),
                    )
                    _offer_message(
                        outgoing_queue, {"type": "error", "message": str(err)}
                    )
                except Exception:
                    LOGGER.exception(
                        "websocket request failed connection_id=%s client=%s payload=%s",
                        connection_id,
                        client,
                        _summarize_payload(payload),
                    )
                    _offer_message(
                        outgoing_queue,
                        {
                            "type": "error",
                            "message": "internal server error",
                        },
                    )
        except WebSocketDisconnect:
            LOGGER.info(
                "websocket disconnected connection_id=%s client=%s active_connection_subscriptions=%s",
                connection_id,
                client,
                len(subscriptions_by_symbol),
            )
        finally:
            sender_task.cancel()
            await asyncio.gather(sender_task, return_exceptions=True)

            symbols = list(subscriptions_by_symbol)
            for symbol in symbols:
                await _unsubscribe_symbol(
                    symbol=symbol,
                    quote_service=quote_service,
                    subscriptions_by_symbol=subscriptions_by_symbol,
                    forwarders_by_symbol=forwarders_by_symbol,
                )
            minute_symbols = list(minute_series_live_handles)
            for symbol in minute_symbols:
                await _unsubscribe_minute_series_symbol(
                    symbol=symbol,
                    live_handles_by_symbol=minute_series_live_handles,
                    subscription_index=minute_series_subscription_index,
                )
            LOGGER.info(
                "websocket cleanup completed connection_id=%s client=%s",
                connection_id,
                client,
            )

    return app


def run_quote_websocket_app(
    host: str = "127.0.0.1",
    port: int = 8000,
    service: AStockQuoteSubscriptionService | None = None,
    poll_interval: float = 3.0,
) -> None:
    """
    直接运行行情 WebSocket 服务
    """

    try:
        import uvicorn
    except ImportError as err:
        raise ImportError(
            "运行 WebSocket 服务需要安装 uvicorn，可执行: pip install 'akshare[realtime]'"
        ) from err

    app = create_quote_websocket_app(
        service=service,
        poll_interval=poll_interval,
    )
    LOGGER.info(
        "quote websocket app boot host=%s port=%s poll_interval=%s",
        host,
        port,
        poll_interval,
    )
    uvicorn.run(app, host=host, port=port)


async def _unsubscribe_by_payload(
    payload: dict[str, Any],
    quote_service: AStockQuoteSubscriptionService,
    subscriptions_by_symbol: dict[str, Any],
    forwarders_by_symbol: dict[str, asyncio.Task[None]],
) -> dict[str, Any]:
    subscription_id = payload.get("subscription_id")
    symbol = payload.get("symbol")

    if symbol:
        normalized_symbol = quote_service._normalize_symbol(symbol)
        return await _unsubscribe_symbol(
            symbol=normalized_symbol,
            quote_service=quote_service,
            subscriptions_by_symbol=subscriptions_by_symbol,
            forwarders_by_symbol=forwarders_by_symbol,
        )

    if subscription_id:
        for current_symbol, subscription in subscriptions_by_symbol.items():
            if subscription.subscription_id == subscription_id:
                return await _unsubscribe_symbol(
                    symbol=current_symbol,
                    quote_service=quote_service,
                    subscriptions_by_symbol=subscriptions_by_symbol,
                    forwarders_by_symbol=forwarders_by_symbol,
                )
        return {
            "type": "unsubscribed",
            "subscription_id": subscription_id,
            "removed": False,
        }

    return {
        "type": "error",
        "message": "unsubscribe requires symbol or subscription_id",
    }


async def _unsubscribe_symbol(
    symbol: str,
    quote_service: AStockQuoteSubscriptionService,
    subscriptions_by_symbol: dict[str, Any],
    forwarders_by_symbol: dict[str, asyncio.Task[None]],
) -> dict[str, Any]:
    subscription = subscriptions_by_symbol.pop(symbol, None)
    forwarder_task = forwarders_by_symbol.pop(symbol, None)

    if forwarder_task is not None:
        forwarder_task.cancel()
        with suppress(asyncio.CancelledError):
            await forwarder_task

    if subscription is None:
        return {"type": "unsubscribed", "symbol": symbol, "removed": False}

    removed = await quote_service.unsubscribe(subscription.subscription_id)
    return {
        "type": "unsubscribed",
        "symbol": symbol,
        "subscription_id": subscription.subscription_id,
        "removed": removed,
    }


async def _forward_subscription(
    subscription,
    outgoing_queue: asyncio.Queue[dict[str, Any]],
) -> None:
    try:
        while True:
            snapshot = await subscription.queue.get()
            _offer_message(
                outgoing_queue,
                {
                    "type": "quote",
                    "symbol": subscription.symbol,
                    "subscription_id": subscription.subscription_id,
                    "data": _snapshot_to_dict(snapshot),
                },
            )
    except asyncio.CancelledError:
        raise


async def _unsubscribe_minute_series_by_payload(
    payload: dict[str, Any],
    minute_series_service: IFindMinuteSeriesService,
    live_handles_by_symbol: dict[str, _MinuteSeriesLiveHandle],
    subscription_index: dict[str, str],
) -> dict[str, Any]:
    symbol = payload.get("symbol")
    subscription_id = payload.get("subscription_id")

    if symbol:
        normalized_symbol = minute_series_service.normalize_symbol(symbol)
        return await _unsubscribe_minute_series_symbol(
            symbol=normalized_symbol,
            live_handles_by_symbol=live_handles_by_symbol,
            subscription_index=subscription_index,
        )

    if subscription_id:
        current_symbol = subscription_index.get(str(subscription_id))
        if current_symbol is None:
            return {
                "type": "unsubscribed_minute_series",
                "subscription_id": subscription_id,
                "removed": False,
            }
        return await _unsubscribe_minute_series_symbol(
            symbol=current_symbol,
            live_handles_by_symbol=live_handles_by_symbol,
            subscription_index=subscription_index,
        )

    return {
        "type": "error",
        "message": "unsubscribe_minute_series requires symbol or subscription_id",
    }


async def _unsubscribe_minute_series_symbol(
    symbol: str,
    live_handles_by_symbol: dict[str, _MinuteSeriesLiveHandle],
    subscription_index: dict[str, str],
) -> dict[str, Any]:
    handle = live_handles_by_symbol.pop(symbol, None)
    if handle is None:
        return {
            "type": "unsubscribed_minute_series",
            "symbol": symbol,
            "removed": False,
        }

    subscription_index.pop(handle.subscription_id, None)
    handle.task.cancel()
    with suppress(asyncio.CancelledError):
        await handle.task
    return {
        "type": "unsubscribed_minute_series",
        "symbol": symbol,
        "subscription_id": handle.subscription_id,
        "removed": True,
    }


async def _forward_minute_series(
    symbol: str,
    subscription_id: str,
    minute_series_service: IFindMinuteSeriesService,
    outgoing_queue: asyncio.Queue[dict[str, Any]],
    initial_last_timestamp: str | None,
) -> None:
    last_sent_timestamp = initial_last_timestamp
    try:
        while True:
            await _sleep_until_next_minute_series_update(
                now_func=minute_series_service.current_time
            )
            snapshot = await minute_series_service.get_live_snapshot(symbol)
            if (
                snapshot.last_timestamp is not None
                and snapshot.last_timestamp != last_sent_timestamp
            ):
                _offer_message(
                    outgoing_queue,
                    _minute_series_snapshot_to_message(
                        snapshot=snapshot,
                        subscription_id=subscription_id,
                    ),
                )
                last_sent_timestamp = snapshot.last_timestamp
            if not snapshot.live:
                return
    except asyncio.CancelledError:
        raise


async def _sleep_until_next_minute_series_update(
    now_func,
) -> None:
    now = now_func().astimezone(_BEIJING_TZ)
    current_clock = now.time().replace(tzinfo=None)

    if current_clock < dt_time(9, 30):
        target = now.replace(hour=9, minute=30, second=2, microsecond=0)
    elif current_clock < dt_time(11, 30):
        next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        target = next_minute + timedelta(seconds=2)
    elif current_clock < dt_time(13, 0):
        target = now.replace(hour=13, minute=0, second=2, microsecond=0)
    elif current_clock < dt_time(15, 0):
        next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        target = next_minute + timedelta(seconds=2)
    else:
        target = now + timedelta(seconds=60)

    await asyncio.sleep(max((target - now).total_seconds(), 0))


async def _sender_loop(
    websocket,
    outgoing_queue: asyncio.Queue[dict[str, Any]],
    connection_id: str,
    client: str,
) -> None:
    try:
        while True:
            message = await outgoing_queue.get()
            await websocket.send_json(message)
            _log_outgoing_message(
                connection_id=connection_id,
                client=client,
                message=message,
            )
    except asyncio.CancelledError:
        raise
    except Exception:
        LOGGER.exception(
            "websocket sender failed connection_id=%s client=%s",
            connection_id,
            client,
        )
        raise


def _snapshot_to_dict(snapshot: QuoteSnapshot) -> dict[str, Any]:
    return {
        "symbol": snapshot.symbol,
        "name": snapshot.name,
        "price": snapshot.price,
        "change": snapshot.change,
        "change_pct": snapshot.change_pct,
        "volume": snapshot.volume,
        "amount": snapshot.amount,
        "open_price": snapshot.open_price,
        "high_price": snapshot.high_price,
        "low_price": snapshot.low_price,
        "prev_close": snapshot.prev_close,
        "turnover_rate": snapshot.turnover_rate,
        "source_time": snapshot.source_time,
        "captured_at": snapshot.captured_at.isoformat(),
        "raw": snapshot.raw,
    }


def _minute_series_snapshot_to_message(
    snapshot: MinuteSeriesSnapshot,
    subscription_id: str | None,
) -> dict[str, Any]:
    return {
        "type": "minute_series",
        "symbol": snapshot.symbol,
        "subscription_id": subscription_id,
        "previousClose": snapshot.previous_close,
        "points": [
            {"timestamp": point.timestamp, "value": point.value}
            for point in snapshot.points
        ],
        "live": snapshot.live,
    }


def _offer_message(
    queue: asyncio.Queue[dict[str, Any]], message: dict[str, Any]
) -> None:
    if queue.full():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
    queue.put_nowait(message)


def _client_repr(websocket) -> str:
    client = getattr(websocket, "client", None)
    if client is None:
        return "unknown"
    host = getattr(client, "host", None) or "unknown"
    port = getattr(client, "port", None)
    if port is None:
        return str(host)
    return f"{host}:{port}"


def _summarize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "action": payload.get("action"),
        "symbol": payload.get("symbol"),
        "subscription_id": payload.get("subscription_id"),
        "timestamp": payload.get("timestamp"),
    }


def _log_outgoing_message(
    connection_id: str,
    client: str,
    message: dict[str, Any],
) -> None:
    message_type = message.get("type")
    if message_type == "quote":
        data = message.get("data", {})
        LOGGER.info(
            "websocket message sent connection_id=%s client=%s type=%s symbol=%s subscription_id=%s price=%s change_pct=%s source_time=%s",
            connection_id,
            client,
            message_type,
            message.get("symbol"),
            message.get("subscription_id"),
            data.get("price"),
            data.get("change_pct"),
            data.get("source_time"),
        )
        return

    if message_type == "minute_series":
        points = message.get("points") or []
        last_timestamp = points[-1].get("timestamp") if points else None
        LOGGER.info(
            "websocket message sent connection_id=%s client=%s type=%s symbol=%s subscription_id=%s points=%s last_timestamp=%s previous_close=%s",
            connection_id,
            client,
            message_type,
            message.get("symbol"),
            message.get("subscription_id"),
            len(points),
            last_timestamp,
            message.get("previousClose"),
        )
        return

    LOGGER.info(
        "websocket message sent connection_id=%s client=%s type=%s symbol=%s subscription_id=%s",
        connection_id,
        client,
        message_type,
        message.get("symbol"),
        message.get("subscription_id"),
    )


def _load_fastapi():
    try:
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    except ImportError as err:
        raise ImportError(
            "使用 WebSocket 接口需要安装 fastapi，可执行: pip install 'akshare[realtime]'"
        ) from err
    globals()["FastAPI"] = FastAPI
    globals()["WebSocket"] = WebSocket
    globals()["WebSocketDisconnect"] = WebSocketDisconnect
    return FastAPI, WebSocket, WebSocketDisconnect

