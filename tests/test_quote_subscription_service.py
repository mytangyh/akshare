#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/24 13:30
Desc: A 股实时行情订阅服务测试
"""

import asyncio
import importlib.util
import sys
from pathlib import Path

import pandas as pd


def _load_service_class():
    module_path = (
        Path(__file__).resolve().parents[1]
        / "akshare"
        / "service"
        / "quote_subscription.py"
    )
    spec = importlib.util.spec_from_file_location(
        "quote_subscription_service_for_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.AStockQuoteSubscriptionService


AStockQuoteSubscriptionService = _load_service_class()


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


def test_subscribe_and_unsubscribe_share_same_worker():
    async def runner():
        state = {"count": 0}

        def fetcher(symbol: str) -> pd.DataFrame:
            state["count"] += 1
            return _mock_quote_df(
                price=10 + state["count"] * 0.01,
                source_time=f"09:30:{state['count']:02d}",
            )

        service = AStockQuoteSubscriptionService(
            fetcher=fetcher, poll_interval=0.01
        )
        first = await service.subscribe("SZ000001")
        second = await service.subscribe("000001")

        first_snapshot = await asyncio.wait_for(first.queue.get(), timeout=0.5)
        second_snapshot = await asyncio.wait_for(second.queue.get(), timeout=0.5)

        assert first_snapshot.symbol == "000001"
        assert second_snapshot.symbol == "000001"
        assert first_snapshot.price is not None
        assert service.active_symbols == ("000001",)
        assert service.subscription_size == 2
        assert service.worker_count == 1

        assert await service.unsubscribe(first.subscription_id) is True
        assert service.subscription_size == 1
        assert service.active_symbols == ("000001",)

        assert await service.unsubscribe(second.subscription_id) is True
        await asyncio.sleep(0)
        assert service.subscription_size == 0
        assert service.active_symbols == ()
        assert service.worker_count == 0

        await service.stop()

    asyncio.run(runner())


def test_new_subscriber_receives_cached_snapshot():
    async def runner():
        state = {"count": 0}

        def fetcher(symbol: str) -> pd.DataFrame:
            state["count"] += 1
            return _mock_quote_df(price=10.45)

        service = AStockQuoteSubscriptionService(fetcher=fetcher, poll_interval=1.0)
        first = await service.subscribe("000001")
        first_snapshot = await asyncio.wait_for(first.queue.get(), timeout=0.5)

        call_count = state["count"]
        second = await service.subscribe("000001")
        second_snapshot = await asyncio.wait_for(second.queue.get(), timeout=0.1)

        assert state["count"] == call_count
        assert second_snapshot.price == first_snapshot.price
        assert second_snapshot.raw["名称"] == "平安银行"

        await service.unsubscribe(first.subscription_id)
        await service.unsubscribe(second.subscription_id)
        await service.stop()

    asyncio.run(runner())

def test_batch_fetcher_aggregates_multiple_symbols():
    async def runner():
        calls = []

        def batch_fetcher(symbols: list[str]) -> dict[str, pd.DataFrame]:
            calls.append(tuple(symbols))
            return {
                symbol: _mock_quote_df(
                    price=10.0 + index,
                    source_time=f"09:30:0{index}",
                )
                for index, symbol in enumerate(symbols, start=1)
            }

        service = AStockQuoteSubscriptionService(
            batch_fetcher=batch_fetcher,
            fallback_fetchers=[],
            poll_interval=0.01,
        )
        first = await service.subscribe("000001")
        second = await service.subscribe("300033")

        first_snapshot = await asyncio.wait_for(first.queue.get(), timeout=0.5)
        second_snapshot = await asyncio.wait_for(second.queue.get(), timeout=0.5)

        assert set(calls[0]) == {"000001", "300033"}
        assert first_snapshot.symbol == "000001"
        assert second_snapshot.symbol == "300033"
        assert service.worker_count == 1

        await service.unsubscribe(first.subscription_id)
        await service.unsubscribe(second.subscription_id)
        await service.stop()

    asyncio.run(runner())



def test_fallback_fetcher_used_when_batch_source_fails():
    async def runner():
        state = {"batch_calls": 0, "fallback_calls": 0}

        def batch_fetcher(symbols: list[str]) -> dict[str, pd.DataFrame]:
            state["batch_calls"] += 1
            raise RuntimeError("batch source unavailable")

        def fallback_fetcher(symbol: str) -> pd.DataFrame:
            state["fallback_calls"] += 1
            return _mock_quote_df(price=12.34, source_time="09:31:00")

        service = AStockQuoteSubscriptionService(
            batch_fetcher=batch_fetcher,
            fallback_fetchers=[fallback_fetcher],
            poll_interval=0.01,
        )
        subscription = await service.subscribe("000001")
        snapshot = await asyncio.wait_for(subscription.queue.get(), timeout=0.5)

        assert snapshot.price == 12.34
        assert state["batch_calls"] >= 1
        assert state["fallback_calls"] >= 1
        assert service.latest_error("000001") is None

        await service.unsubscribe(subscription.subscription_id)
        await service.stop()

    asyncio.run(runner())



def test_poll_interval_backs_off_after_total_failure():
    async def runner():
        def batch_fetcher(symbols: list[str]) -> dict[str, pd.DataFrame]:
            raise RuntimeError("all sources failed")

        service = AStockQuoteSubscriptionService(
            batch_fetcher=batch_fetcher,
            fallback_fetchers=[],
            poll_interval=0.01,
            max_backoff=0.04,
        )
        subscription = await service.subscribe("000001")

        await asyncio.sleep(0.05)
        assert service.current_poll_interval > 0.01
        assert service.latest_error("000001") is not None

        await service.unsubscribe(subscription.subscription_id)
        await service.stop()

    asyncio.run(runner())
