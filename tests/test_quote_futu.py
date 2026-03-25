#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/25 13:55
Desc: Futu 行情适配测试
"""

import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


def _load_module(module_name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVICE_ROOT = PROJECT_ROOT / "akshare" / "service"

QUOTE_FUTU_MODULE = _load_module(
    "quote_futu_for_test",
    SERVICE_ROOT / "quote_futu.py",
)
QUOTE_SUBSCRIPTION_MODULE = _load_module(
    "quote_subscription_for_futu_test",
    SERVICE_ROOT / "quote_subscription.py",
)

FutuQuoteBatchFetcher = QUOTE_FUTU_MODULE.FutuQuoteBatchFetcher
AStockQuoteSubscriptionService = (
    QUOTE_SUBSCRIPTION_MODULE.AStockQuoteSubscriptionService
)


class _FakeQuoteContext:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.subscribed_codes = []
        self.closed = False

    def subscribe(self, code_list, subtype_list, subscribe_push=False):
        self.subscribed_codes.append((tuple(code_list), tuple(subtype_list)))
        return 0, "ok"

    def get_stock_quote(self, code_list):
        return (
            0,
            pd.DataFrame(
                [
                    {
                        "code": "SZ.000001",
                        "name": "平安银行",
                        "last_price": 10.5,
                        "change_val": 0.1,
                        "change_rate": 0.96,
                        "volume": 100000,
                        "turnover": 1000000,
                        "open_price": 10.4,
                        "high_price": 10.6,
                        "low_price": 10.3,
                        "prev_close_price": 10.4,
                        "turnover_rate": 0.45,
                        "amplitude": 1.2,
                        "data_date": "2026-03-25",
                        "data_time": "14:30:03",
                    },
                    {
                        "code": "SH.600000",
                        "name": "浦发银行",
                        "last_price": 9.8,
                        "change_val": -0.05,
                        "change_rate": -0.51,
                        "volume": 200000,
                        "turnover": 3000000,
                        "open_price": 9.85,
                        "high_price": 9.9,
                        "low_price": 9.7,
                        "prev_close_price": 9.85,
                        "turnover_rate": 0.33,
                        "amplitude": 2.0,
                        "data_date": "2026-03-25",
                        "data_time": "14:30:03",
                    },
                ]
            ),
        )

    def close(self):
        self.closed = True


class _FakeFutuApi:
    RET_OK = 0
    SubType = SimpleNamespace(QUOTE="QUOTE")

    def __init__(self):
        self.contexts = []

    def OpenQuoteContext(self, host: str, port: int):
        context = _FakeQuoteContext(host=host, port=port)
        self.contexts.append(context)
        return context


def test_futu_batch_fetcher_maps_quote_fields():
    fake_api = _FakeFutuApi()
    fetcher = FutuQuoteBatchFetcher(host="127.0.0.1", port=11111, api=fake_api)

    result = fetcher(["000001", "600000"])

    assert set(result) == {"000001", "600000"}
    assert fake_api.contexts[0].subscribed_codes[0][0] == ("SZ.000001", "SH.600000")
    quote_df = result["000001"]
    raw = dict(quote_df.itertuples(index=False, name=None))
    assert raw["名称"] == "平安银行"
    assert raw["最新"] == 10.5
    assert raw["时间"] == "2026-03-25 14:30:03"


def test_service_stop_closes_futu_batch_fetcher():
    fake_api = _FakeFutuApi()
    fetcher = FutuQuoteBatchFetcher(host="127.0.0.1", port=11111, api=fake_api)

    service = AStockQuoteSubscriptionService(
        batch_fetcher=fetcher,
        fallback_fetchers=[],
        poll_interval=0.01,
    )

    asyncio.run(service.stop())

    assert fake_api.contexts == []

    fetcher(["000001"])
    asyncio.run(service.stop())
    assert fake_api.contexts[0].closed is True
