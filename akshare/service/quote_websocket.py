#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/24 14:10
Desc: A 股实时行情 WebSocket 接口
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress
from typing import Any
from uuid import uuid4

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


def create_quote_websocket_app(
    service: AStockQuoteSubscriptionService | None = None,
    poll_interval: float = 3.0,
):
    """
    创建 FastAPI WebSocket 应用
    """

    FastAPI, WebSocket, WebSocketDisconnect = _load_fastapi()

    quote_service = service or _create_default_quote_service(
        poll_interval=poll_interval
    )

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
            LOGGER.info("quote websocket app stopped")

    app = FastAPI(
        title="AKShare A-Stock Quote WebSocket",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.quote_service = quote_service

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {
            "status": "ok",
            "active_symbols": list(quote_service.active_symbols),
            "subscriptions": quote_service.subscription_size,
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
        outgoing_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
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

        try:
            while True:
                payload = await websocket.receive_json()
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
                            await outgoing_queue.put(
                                {
                                    "type": "subscribed",
                                    "symbol": normalized_symbol,
                                    "subscription_id": current_subscription.subscription_id,
                                    "reused": True,
                                }
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
                        await outgoing_queue.put(
                            {
                                "type": "subscribed",
                                "symbol": normalized_symbol,
                                "subscription_id": subscription.subscription_id,
                                "reused": False,
                            }
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
                        await outgoing_queue.put(result)
                        continue

                    if action == "ping":
                        LOGGER.info(
                            "websocket ping connection_id=%s client=%s",
                            connection_id,
                            client,
                        )
                        await outgoing_queue.put({"type": "pong"})
                        continue

                    LOGGER.warning(
                        "websocket unsupported action connection_id=%s client=%s action=%s",
                        connection_id,
                        client,
                        action,
                    )
                    await outgoing_queue.put(
                        {
                            "type": "error",
                            "message": "unsupported action",
                            "supported_actions": ["subscribe", "unsubscribe", "ping"],
                        }
                    )
                except ValueError as err:
                    LOGGER.warning(
                        "websocket request rejected connection_id=%s client=%s error=%s payload=%s",
                        connection_id,
                        client,
                        err,
                        _summarize_payload(payload),
                    )
                    await outgoing_queue.put({"type": "error", "message": str(err)})
                except Exception:
                    LOGGER.exception(
                        "websocket request failed connection_id=%s client=%s payload=%s",
                        connection_id,
                        client,
                        _summarize_payload(payload),
                    )
                    await outgoing_queue.put(
                        {
                            "type": "error",
                            "message": "internal server error",
                        }
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
            with suppress(asyncio.CancelledError):
                await sender_task

            symbols = list(subscriptions_by_symbol)
            for symbol in symbols:
                await _unsubscribe_symbol(
                    symbol=symbol,
                    quote_service=quote_service,
                    subscriptions_by_symbol=subscriptions_by_symbol,
                    forwarders_by_symbol=forwarders_by_symbol,
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
            await outgoing_queue.put(
                {
                    "type": "quote",
                    "symbol": subscription.symbol,
                    "subscription_id": subscription.subscription_id,
                    "data": _snapshot_to_dict(snapshot),
                }
            )
    except asyncio.CancelledError:
        raise


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

