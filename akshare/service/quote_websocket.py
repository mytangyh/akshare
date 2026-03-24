#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/24 14:10
Desc: A 股实时行情 WebSocket 接口
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from typing import Any

from .quote_subscription import AStockQuoteSubscriptionService, QuoteSnapshot


def create_quote_websocket_app(
    service: AStockQuoteSubscriptionService | None = None,
    poll_interval: float = 3.0,
):
    """
    创建 FastAPI WebSocket 应用
    """

    FastAPI, WebSocket, WebSocketDisconnect = _load_fastapi()

    quote_service = service or AStockQuoteSubscriptionService(
        poll_interval=poll_interval
    )

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            await quote_service.stop()

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
        await websocket.accept()
        outgoing_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        sender_task = asyncio.create_task(
            _sender_loop(websocket=websocket, outgoing_queue=outgoing_queue)
        )
        subscriptions_by_symbol: dict[str, Any] = {}
        forwarders_by_symbol: dict[str, asyncio.Task[None]] = {}

        try:
            while True:
                payload = await websocket.receive_json()
                action = str(payload.get("action", "")).strip().lower()

                if action == "subscribe":
                    symbol = payload.get("symbol")
                    normalized_symbol = quote_service._normalize_symbol(symbol)

                    current_subscription = subscriptions_by_symbol.get(normalized_symbol)
                    if current_subscription is not None:
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
                    await outgoing_queue.put(result)
                    continue

                if action == "ping":
                    await outgoing_queue.put({"type": "pong"})
                    continue

                await outgoing_queue.put(
                    {
                        "type": "error",
                        "message": "unsupported action",
                        "supported_actions": ["subscribe", "unsubscribe", "ping"],
                    }
                )
        except WebSocketDisconnect:
            pass
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


async def _sender_loop(websocket, outgoing_queue: asyncio.Queue[dict[str, Any]]) -> None:
    try:
        while True:
            message = await outgoing_queue.get()
            await websocket.send_json(message)
    except asyncio.CancelledError:
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

