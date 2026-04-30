#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/29 10:45
Desc: iFinD 分钟序列缓存服务测试
"""

import asyncio
import importlib.util
import sys
import types
from datetime import date, datetime
from pathlib import Path


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


def _load_ifind_minute_series_module():
    project_root = Path(__file__).resolve().parents[1]
    akshare_root = project_root / "akshare"
    service_root = akshare_root / "service"

    _ensure_package("akshare", akshare_root)
    _ensure_package("akshare.service", service_root)

    return _load_module(
        "akshare.service.ifind_minute_series",
        service_root / "ifind_minute_series.py",
    )


IFIND_MINUTE_SERIES_MODULE = _load_ifind_minute_series_module()
IFindMinuteSeriesService = IFIND_MINUTE_SERIES_MODULE.IFindMinuteSeriesService


class _FakeIFindMinuteSeriesService(IFindMinuteSeriesService):
    def __init__(self, now_func):
        super().__init__(
            access_token="token",
            now_func=now_func,
            trade_calendar=[
                date(2026, 3, 26),
                date(2026, 3, 27),
                date(2026, 3, 30),
            ],
        )
        self.hf_calls = []
        self.prev_close_calls = []
        self.day_points = {
            ("000001.SH", date(2026, 3, 26)): [
                ("2026-03-26 14:59", 3889.08),
            ],
            ("000001.SH", date(2026, 3, 27)): [
                ("2026-03-27 09:30", 3852.0936),
                ("2026-03-27 09:31", 3863.0668),
                ("2026-03-27 09:32", 3860.7667),
            ],
            ("000001.SH", date(2026, 3, 30)): [
                ("2026-03-30 09:30", 3910.0),
                ("2026-03-30 09:31", 3911.0),
                ("2026-03-30 09:32", 3912.0),
            ],
        }

    def _fetch_close_series_sync(self, symbol, start_at, end_at):
        start_text = start_at.strftime("%Y-%m-%d %H:%M")
        end_text = end_at.strftime("%Y-%m-%d %H:%M")
        self.hf_calls.append((symbol, start_text, end_text))
        return [
            (timestamp, value)
            for timestamp, value in self.day_points[(symbol, start_at.date())]
            if start_text <= timestamp <= end_text
        ]

    def _fetch_realtime_previous_close_sync(self, symbol):
        self.prev_close_calls.append(symbol)
        return 3900.0


def test_historical_snapshot_uses_cache_and_returns_full_day():
    current_now = datetime(2026, 3, 30, 10, 0)
    service = _FakeIFindMinuteSeriesService(now_func=lambda: current_now)

    async def runner():
        first = await service.get_snapshot("000001.SH", "2026-03-27 10:30:00")
        second = await service.get_snapshot("000001.SH", "2026-03-27 11:00:00")

        assert [point.timestamp for point in first.points] == [
            "2026-03-27 09:30",
            "2026-03-27 09:31",
            "2026-03-27 09:32",
        ]
        assert first.previous_close == 3889.08
        assert first.live is False
        assert second.points == first.points
        assert service.hf_calls == [
            ("000001.SH", "2026-03-27 09:30", "2026-03-27 14:59"),
            ("000001.SH", "2026-03-26 14:59", "2026-03-26 14:59"),
        ]

        await service.stop()

    asyncio.run(runner())


def test_live_snapshot_fetches_only_incremental_minutes():
    now_holder = {"value": datetime(2026, 3, 30, 9, 31, 30)}
    service = _FakeIFindMinuteSeriesService(now_func=lambda: now_holder["value"])

    async def runner():
        initial = await service.get_snapshot("000001.SH", "2026-03-30 09:30:00")
        now_holder["value"] = datetime(2026, 3, 30, 9, 32, 30)
        updated = await service.get_live_snapshot("000001.SH")

        assert [point.timestamp for point in initial.points] == [
            "2026-03-30 09:30",
            "2026-03-30 09:31",
        ]
        assert [point.timestamp for point in updated.points] == [
            "2026-03-30 09:30",
            "2026-03-30 09:31",
            "2026-03-30 09:32",
        ]
        assert updated.previous_close == 3900.0
        assert service.prev_close_calls == ["000001.SH"]
        assert service.hf_calls == [
            ("000001.SH", "2026-03-30 09:30", "2026-03-30 09:31"),
            ("000001.SH", "2026-03-30 09:32", "2026-03-30 09:32"),
        ]

        await service.stop()

    asyncio.run(runner())


def test_same_day_snapshot_ignores_client_timestamp_and_returns_latest_available():
    current_now = datetime(2026, 3, 30, 9, 32, 30)
    service = _FakeIFindMinuteSeriesService(now_func=lambda: current_now)

    async def runner():
        snapshot = await service.get_snapshot("000001.SH", "2026-03-30 09:30:00")

        assert [point.timestamp for point in snapshot.points] == [
            "2026-03-30 09:30",
            "2026-03-30 09:31",
            "2026-03-30 09:32",
        ]
        assert snapshot.previous_close == 3900.0
        assert snapshot.live is True
        assert service.prev_close_calls == ["000001.SH"]
        assert service.hf_calls == [
            ("000001.SH", "2026-03-30 09:30", "2026-03-30 09:32"),
        ]

        await service.stop()

    asyncio.run(runner())


def test_midday_snapshot_uses_cached_live_path_instead_of_full_day_history():
    current_now = datetime(2026, 3, 30, 12, 5)
    service = _FakeIFindMinuteSeriesService(now_func=lambda: current_now)

    async def runner():
        snapshot = await service.get_snapshot("000001.SH", "2026-03-30 12:00:00")

        assert [point.timestamp for point in snapshot.points] == [
            "2026-03-30 09:30",
            "2026-03-30 09:31",
            "2026-03-30 09:32",
        ]
        assert snapshot.previous_close == 3900.0
        assert snapshot.live is True
        assert service.prev_close_calls == ["000001.SH"]
        assert service.hf_calls == [
            ("000001.SH", "2026-03-30 09:30", "2026-03-30 11:30"),
        ]

        await service.stop()

    asyncio.run(runner())


def test_preopen_snapshot_does_not_fetch_future_minutes():
    current_now = datetime(2026, 3, 30, 9, 0)
    service = _FakeIFindMinuteSeriesService(now_func=lambda: current_now)

    async def runner():
        snapshot = await service.get_snapshot("000001.SH", "2026-03-30 10:30:00")

        assert snapshot.points == ()
        assert snapshot.previous_close == 3900.0
        assert snapshot.live is True
        assert service.prev_close_calls == ["000001.SH"]
        assert service.hf_calls == []

        await service.stop()

    asyncio.run(runner())
