#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/24 14:10
Desc: A 股实时行情 WebSocket 测试
"""

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
    return subscription_module, websocket_module


QUOTE_SUBSCRIPTION_MODULE, QUOTE_WEBSOCKET_MODULE = _load_quote_modules()
AStockQuoteSubscriptionService = (
    QUOTE_SUBSCRIPTION_MODULE.AStockQuoteSubscriptionService
)
create_quote_websocket_app = QUOTE_WEBSOCKET_MODULE.create_quote_websocket_app


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
