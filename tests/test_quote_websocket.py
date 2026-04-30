#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/24 14:10
Desc: A 股实时行情 WebSocket 测试
"""

import asyncio
import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient


def _ensure_package(name: str, path: Path) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    module.__path__ = [str(path)]
    sys.modules[name] = module


def _load_module(module_name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_quote_modules():
    project_root = Path(__file__).resolve().parents[1]
    akshare_root = project_root / "akshare"
    service_root = akshare_root / "service"

    _ensure_package("akshare", akshare_root)
    _ensure_package("akshare.service", service_root)

    subscription_module = _load_module(
        "akshare.service.quote_subscription",
        service_root / "quote_subscription.py",
    )
    websocket_module = _load_module(
        "akshare.service.quote_websocket",
        service_root / "quote_websocket.py",
    )
    minute_series_module = _load_module(
        "akshare.service.ifind_minute_series",
        service_root / "ifind_minute_series.py",
    )
    return subscription_module, websocket_module, minute_series_module


(
    QUOTE_SUBSCRIPTION_MODULE,
    QUOTE_WEBSOCKET_MODULE,
    IFIND_MINUTE_SERIES_MODULE,
) = _load_quote_modules()
AStockQuoteSubscriptionService = (
    QUOTE_SUBSCRIPTION_MODULE.AStockQuoteSubscriptionService
)
create_quote_websocket_app = QUOTE_WEBSOCKET_MODULE.create_quote_websocket_app
offer_message = QUOTE_WEBSOCKET_MODULE._offer_message
MinuteSeriesPoint = IFIND_MINUTE_SERIES_MODULE.MinuteSeriesPoint
MinuteSeriesSnapshot = IFIND_MINUTE_SERIES_MODULE.MinuteSeriesSnapshot


def _mock_quote_df(price: float, source_time: str = "09:30:00") -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("名称", "平安银行"),
            ("最新", price),
            ("涨跌", 0.05),
            ("涨幅", 0.48),
            ("总手", 872663),
            ("金额", 910278600),
            ("换手", 0.45),
            ("最高", price + 0.02),
            ("最低", price - 0.08),
            ("今开", price - 0.07),
            ("昨收", price - 0.05),
            ("时间", source_time),
        ],
        columns=["item", "value"],
    )


def test_healthz_and_websocket_subscribe_unsubscribe():
    def fetcher(symbol: str) -> pd.DataFrame:
        return _mock_quote_df(price=10.01, source_time="09:30:01")

    service = AStockQuoteSubscriptionService(fetcher=fetcher, poll_interval=0.01)
    app = create_quote_websocket_app(service=service)

    with TestClient(app) as client:
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

        with client.websocket_connect("/ws/quotes") as websocket:
            websocket.send_json({"action": "subscribe", "symbol": "000001"})
            subscribed_message = websocket.receive_json()
            assert subscribed_message["type"] == "subscribed"
            assert subscribed_message["symbol"] == "000001"
            assert subscribed_message["reused"] is False

            quote_message = websocket.receive_json()
            assert quote_message["type"] == "quote"
            assert quote_message["symbol"] == "000001"
            assert quote_message["data"]["name"] == "平安银行"
            assert quote_message["data"]["price"] is not None

            websocket.send_json({"action": "unsubscribe", "symbol": "000001"})
            unsubscribed_message = websocket.receive_json()
            assert unsubscribed_message["type"] == "unsubscribed"
            assert unsubscribed_message["symbol"] == "000001"
            assert unsubscribed_message["removed"] is True


def test_sender_failure_still_cleans_up_subscriptions():
    class BrokenSendWebSocket:
        def __init__(self):
            self.client = types.SimpleNamespace(host="127.0.0.1", port=9527)
            self._messages = [{"action": "subscribe", "symbol": "000001"}]

        async def accept(self):
            return None

        async def receive_json(self):
            if self._messages:
                return self._messages.pop(0)
            await asyncio.Event().wait()

        async def send_json(self, message):
            raise RuntimeError("socket send failed")

    async def runner():
        def fetcher(symbol: str) -> pd.DataFrame:
            return _mock_quote_df(price=10.01, source_time="09:30:01")

        service = AStockQuoteSubscriptionService(fetcher=fetcher, poll_interval=0.01)
        app = create_quote_websocket_app(service=service)
        endpoint = next(
            route.endpoint
            for route in app.router.routes
            if getattr(route, "path", None) == "/ws/quotes"
        )

        task = asyncio.create_task(endpoint(BrokenSendWebSocket()))
        await asyncio.sleep(0.1)

        assert task.done() is True
        assert service.subscription_size == 0
        assert service.active_symbols == ()
        assert service.worker_count == 0

        with TestClient(app) as client:
            response = client.get("/healthz")
            assert response.json()["subscriptions"] == 0

        await service.stop()

    asyncio.run(runner())


def test_offer_message_drops_oldest_when_queue_full():
    queue: asyncio.Queue[dict[str, int]] = asyncio.Queue(maxsize=2)

    offer_message(queue, {"seq": 1})
    offer_message(queue, {"seq": 2})
    offer_message(queue, {"seq": 3})

    assert queue.qsize() == 2
    assert queue.get_nowait() == {"seq": 2}
    assert queue.get_nowait() == {"seq": 3}


def test_subscribe_minute_series_replays_full_snapshot_and_live_updates():
    sleep_calls = {"count": 0}

    async def fake_sleep(now_func):
        sleep_calls["count"] += 1
        if sleep_calls["count"] == 1:
            await asyncio.sleep(0)
            return
        await asyncio.Event().wait()

    class FakeMinuteSeriesService:
        def __init__(self):
            self.live_calls = 0

        @staticmethod
        def normalize_symbol(symbol: str) -> str:
            return str(symbol).strip().upper()

        async def get_snapshot(self, symbol: str, request_timestamp: str):
            assert symbol == "000001.SH"
            assert request_timestamp == "2026-03-30 09:30:00"
            return MinuteSeriesSnapshot(
                symbol="000001.SH",
                previous_close=3900.0,
                points=(
                    MinuteSeriesPoint(timestamp="2026-03-30 09:30", value=3910.0),
                ),
                live=True,
            )

        async def get_live_snapshot(self, symbol: str):
            self.live_calls += 1
            return MinuteSeriesSnapshot(
                symbol="000001.SH",
                previous_close=3900.0,
                points=(
                    MinuteSeriesPoint(timestamp="2026-03-30 09:30", value=3910.0),
                    MinuteSeriesPoint(timestamp="2026-03-30 09:31", value=3911.0),
                ),
                live=True,
            )

        async def stop(self):
            return None

    original_sleep = QUOTE_WEBSOCKET_MODULE._sleep_until_next_minute_series_update
    QUOTE_WEBSOCKET_MODULE._sleep_until_next_minute_series_update = fake_sleep

    def fetcher(symbol: str) -> pd.DataFrame:
        return _mock_quote_df(price=10.01, source_time="09:30:01")

    service = AStockQuoteSubscriptionService(fetcher=fetcher, poll_interval=0.01)
    minute_service = FakeMinuteSeriesService()
    app = create_quote_websocket_app(
        service=service,
        minute_series_service=minute_service,
    )

    try:
        with TestClient(app) as client:
            with client.websocket_connect("/ws/quotes") as websocket:
                websocket.send_json(
                    {
                        "action": "subscribe_minute_series",
                        "symbol": "000001.SH",
                        "timestamp": "2026-03-30 09:30:00",
                    }
                )
                subscribed_message = websocket.receive_json()
                assert subscribed_message["type"] == "subscribed_minute_series"
                assert subscribed_message["symbol"] == "000001.SH"
                assert subscribed_message["live"] is True

                initial_series = websocket.receive_json()
                assert initial_series["type"] == "minute_series"
                assert initial_series["previousClose"] == 3900.0
                assert initial_series["points"] == [
                    {"timestamp": "2026-03-30 09:30", "value": 3910.0}
                ]

                updated_series = websocket.receive_json()
                assert updated_series["type"] == "minute_series"
                assert updated_series["points"] == [
                    {"timestamp": "2026-03-30 09:30", "value": 3910.0},
                    {"timestamp": "2026-03-30 09:31", "value": 3911.0},
                ]

                websocket.send_json(
                    {"action": "unsubscribe_minute_series", "symbol": "000001.SH"}
                )
                unsubscribed_message = websocket.receive_json()
                assert unsubscribed_message["type"] == "unsubscribed_minute_series"
                assert unsubscribed_message["symbol"] == "000001.SH"
                assert unsubscribed_message["removed"] is True
    finally:
        QUOTE_WEBSOCKET_MODULE._sleep_until_next_minute_series_update = original_sleep
