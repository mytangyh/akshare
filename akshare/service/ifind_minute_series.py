#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/29 10:20
Desc: iFinD 指数分钟序列缓存服务
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta
import json
import logging
import os
from pathlib import Path
import threading
from typing import Any, Callable
from zoneinfo import ZoneInfo

import requests

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


@dataclass(frozen=True)
class MinuteSeriesPoint:
    timestamp: str
    value: float | None


@dataclass(frozen=True)
class MinuteSeriesSnapshot:
    symbol: str
    previous_close: float | None
    points: tuple[MinuteSeriesPoint, ...]
    live: bool

    @property
    def last_timestamp(self) -> str | None:
        if not self.points:
            return None
        return self.points[-1].timestamp


@dataclass
class _MinuteSeriesCacheEntry:
    symbol: str
    trade_date: date
    previous_close: float | None = None
    points: dict[str, float | None] = field(default_factory=dict)
    complete: bool = False

    @property
    def last_timestamp(self) -> str | None:
        if not self.points:
            return None
        return next(reversed(self.points))


class IFindMinuteSeriesService:
    _HF_URL = "https://quantapi.51ifind.com/api/v1/high_frequency"
    _QUOTE_URL = "https://quantapi.51ifind.com/api/v1/real_time_quotation"
    _REFRESH_URL = "https://quantapi.51ifind.com/api/v1/get_access_token"
    _BEIJING_TZ = ZoneInfo("Asia/Shanghai")
    _MORNING_START = dt_time(9, 30)
    _MORNING_END = dt_time(11, 30)
    _AFTERNOON_START = dt_time(13, 0)
    _AFTERNOON_END = dt_time(15, 0)
    _FULL_DAY_START = dt_time(9, 30)
    _FULL_DAY_END = dt_time(14, 59)

    def __init__(
        self,
        access_token: str | None = None,
        refresh_token: str | None = None,
        request_timeout: float = 10.0,
        session: requests.Session | None = None,
        now_func: Callable[[], datetime] | None = None,
        trade_calendar: list[date] | None = None,
    ) -> None:
        self._access_token = (access_token or "").strip()
        self._refresh_token = (refresh_token or "").strip()
        self._request_timeout = request_timeout
        self._session = session or requests.Session()
        self._owns_session = session is None
        self._now_func = now_func or self._default_now
        self._calendar = trade_calendar or self._load_trade_calendar()

        self._cache: dict[tuple[str, date], _MinuteSeriesCacheEntry] = {}
        self._key_locks: dict[tuple[str, date], asyncio.Lock] = {}
        self._token_lock = threading.Lock()

        if not self._access_token and not self._refresh_token:
            raise ValueError(
                "使用 iFinD 分钟序列服务需要设置 IFIND_ACCESS_TOKEN 或 IFIND_REFRESH_TOKEN"
            )

    async def get_snapshot(
        self, symbol: str, request_timestamp: str | datetime
    ) -> MinuteSeriesSnapshot:
        normalized_symbol = self.normalize_symbol(symbol)
        requested_at = self._normalize_datetime(request_timestamp)
        trade_date = requested_at.date()
        self._ensure_trade_day(trade_date)
        key = (normalized_symbol, trade_date)
        key_lock = self._get_key_lock(key)

        async with key_lock:
            entry = self._cache.setdefault(
                key,
                _MinuteSeriesCacheEntry(symbol=normalized_symbol, trade_date=trade_date),
            )
            now = self._normalize_datetime(self._now_func())
            if self._is_live_day_request(requested_at=requested_at, now=now):
                latest_available = self._latest_available_minute(now)
                if entry.previous_close is None:
                    entry.previous_close = await self._fetch_realtime_previous_close(
                        entry.symbol
                    )
                if latest_available is None:
                    return MinuteSeriesSnapshot(
                        symbol=entry.symbol,
                        previous_close=entry.previous_close,
                        points=(),
                        live=True,
                    )

                # For the current trading day the client timestamp is only used
                # to identify the trade date. The server should always return
                # data through the latest available minute on this side.
                target_at = latest_available
                await self._ensure_live_snapshot(entry=entry, target_at=target_at)
                return self._build_snapshot(
                    entry=entry,
                    end_timestamp=self._format_minute(target_at),
                    live=True,
                )

            await self._ensure_historical_snapshot(entry=entry)
            return self._build_snapshot(entry=entry, live=False)

    async def get_live_snapshot(self, symbol: str) -> MinuteSeriesSnapshot:
        normalized_symbol = self.normalize_symbol(symbol)
        now = self._normalize_datetime(self._now_func())
        trade_date = now.date()
        self._ensure_trade_day(trade_date)
        key = (normalized_symbol, trade_date)
        key_lock = self._get_key_lock(key)

        async with key_lock:
            entry = self._cache.setdefault(
                key,
                _MinuteSeriesCacheEntry(symbol=normalized_symbol, trade_date=trade_date),
            )
            latest_available = self._latest_available_minute(now)
            if latest_available is None:
                if self._has_future_trading_minutes(now) and entry.previous_close is None:
                    entry.previous_close = await self._fetch_realtime_previous_close(
                        entry.symbol
                    )
                return MinuteSeriesSnapshot(
                    symbol=entry.symbol,
                    previous_close=entry.previous_close,
                    points=(),
                    live=self._has_future_trading_minutes(now),
                )
            if latest_available is not None:
                await self._ensure_live_snapshot(entry=entry, target_at=latest_available)
            return self._build_snapshot(
                entry=entry,
                live=self._has_future_trading_minutes(now),
            )

    async def stop(self) -> None:
        if self._owns_session:
            await asyncio.to_thread(self._session.close)
            LOGGER.info("ifind minute series session closed")

    def current_time(self) -> datetime:
        return self._normalize_datetime(self._now_func())

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        normalized_symbol = str(symbol).strip().upper()
        if "." in normalized_symbol:
            code, market = normalized_symbol.split(".", 1)
        elif normalized_symbol[:2] in {"SH", "SZ", "BJ"}:
            market = normalized_symbol[:2]
            code = normalized_symbol[2:]
        else:
            raise ValueError(
                "symbol 必须带交易所后缀，例如 000001.SH 或 SH000001"
            )
        market = market[:2]
        if market not in {"SH", "SZ", "BJ"}:
            raise ValueError("symbol 交易所后缀仅支持 SH、SZ、BJ")
        if len(code) != 6 or not code.isdigit():
            raise ValueError("symbol 必须是 6 位数字代码")
        return f"{code}.{market}"

    @classmethod
    def _default_now(cls) -> datetime:
        return datetime.now(cls._BEIJING_TZ)

    @classmethod
    def _normalize_datetime(cls, value: str | datetime) -> datetime:
        if isinstance(value, datetime):
            dt_value = value
        else:
            dt_value = datetime.fromisoformat(str(value).strip())
        if dt_value.tzinfo is None:
            return dt_value.replace(tzinfo=cls._BEIJING_TZ)
        return dt_value.astimezone(cls._BEIJING_TZ)

    @classmethod
    def _format_minute(cls, value: datetime) -> str:
        return value.astimezone(cls._BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

    @classmethod
    def _parse_minute(cls, value: str) -> datetime:
        return datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=cls._BEIJING_TZ)

    def _get_key_lock(self, key: tuple[str, date]) -> asyncio.Lock:
        lock = self._key_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._key_locks[key] = lock
        return lock

    def _ensure_trade_day(self, trade_date: date) -> None:
        if trade_date not in self._calendar:
            raise ValueError(f"{trade_date.isoformat()} 不是交易日")

    def _load_trade_calendar(self) -> list[date]:
        calendar_path = Path(__file__).resolve().parents[1] / "file_fold" / "calendar.json"
        raw_days = json.loads(calendar_path.read_text(encoding="utf-8"))
        return [datetime.strptime(item, "%Y%m%d").date() for item in raw_days]

    def _previous_trade_date(self, trade_date: date) -> date:
        position = self._calendar.index(trade_date)
        if position <= 0:
            raise ValueError(f"{trade_date.isoformat()} 没有上一交易日")
        return self._calendar[position - 1]

    def _is_live_day_request(self, requested_at: datetime, now: datetime) -> bool:
        return requested_at.date() == now.date() and self._has_future_trading_minutes(now)

    @classmethod
    def _is_market_open(cls, current_time: datetime) -> bool:
        current_clock = current_time.astimezone(cls._BEIJING_TZ).time().replace(tzinfo=None)
        return any(
            start <= current_clock <= end
            for start, end in (
                (cls._MORNING_START, cls._MORNING_END),
                (cls._AFTERNOON_START, cls._AFTERNOON_END),
            )
        )

    @classmethod
    def _has_future_trading_minutes(cls, current_time: datetime) -> bool:
        current_clock = current_time.astimezone(cls._BEIJING_TZ).time().replace(
            tzinfo=None
        )
        return current_clock < cls._AFTERNOON_END

    @classmethod
    def _latest_available_minute(cls, current_time: datetime) -> datetime | None:
        beijing_now = current_time.astimezone(cls._BEIJING_TZ)
        current_clock = beijing_now.time().replace(tzinfo=None)

        if current_clock < cls._MORNING_START:
            return None
        if current_clock <= cls._MORNING_END:
            return beijing_now.replace(second=0, microsecond=0)
        if current_clock < cls._AFTERNOON_START:
            return beijing_now.replace(
                hour=11, minute=30, second=0, microsecond=0
            )
        if current_clock < cls._AFTERNOON_END:
            return beijing_now.replace(second=0, microsecond=0)
        return beijing_now.replace(hour=14, minute=59, second=0, microsecond=0)

    async def _ensure_historical_snapshot(self, entry: _MinuteSeriesCacheEntry) -> None:
        if not entry.complete:
            day_start = datetime.combine(
                entry.trade_date, self._FULL_DAY_START, tzinfo=self._BEIJING_TZ
            )
            day_end = datetime.combine(
                entry.trade_date, self._FULL_DAY_END, tzinfo=self._BEIJING_TZ
            )
            points = await self._fetch_close_series(
                symbol=entry.symbol,
                start_at=day_start,
                end_at=day_end,
            )
            self._merge_points(entry=entry, points=points)
            entry.complete = True

        if entry.previous_close is None:
            previous_trade_date = self._previous_trade_date(entry.trade_date)
            previous_day_close = await self._fetch_last_close_of_trade_date(
                symbol=entry.symbol,
                trade_date=previous_trade_date,
            )
            entry.previous_close = previous_day_close

    async def _ensure_live_snapshot(
        self, entry: _MinuteSeriesCacheEntry, target_at: datetime
    ) -> None:
        if entry.previous_close is None:
            entry.previous_close = await self._fetch_realtime_previous_close(entry.symbol)

        if entry.last_timestamp is None:
            start_at = datetime.combine(
                entry.trade_date, self._FULL_DAY_START, tzinfo=self._BEIJING_TZ
            )
        else:
            start_at = self._parse_minute(entry.last_timestamp) + timedelta(minutes=1)

        if start_at > target_at:
            return

        points = await self._fetch_close_series(
            symbol=entry.symbol,
            start_at=start_at,
            end_at=target_at,
        )
        self._merge_points(entry=entry, points=points)

    async def _fetch_last_close_of_trade_date(self, symbol: str, trade_date: date) -> float | None:
        end_at = datetime.combine(trade_date, self._FULL_DAY_END, tzinfo=self._BEIJING_TZ)
        points = await self._fetch_close_series(symbol=symbol, start_at=end_at, end_at=end_at)
        if not points:
            return None
        return points[-1][1]

    async def _fetch_close_series(
        self, symbol: str, start_at: datetime, end_at: datetime
    ) -> list[tuple[str, float | None]]:
        return await asyncio.to_thread(
            self._fetch_close_series_sync,
            symbol,
            start_at,
            end_at,
        )

    def _fetch_close_series_sync(
        self, symbol: str, start_at: datetime, end_at: datetime
    ) -> list[tuple[str, float | None]]:
        access_token = self._ensure_access_token()
        payload = {
            "codes": symbol,
            "indicators": "close",
            "starttime": start_at.astimezone(self._BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "endtime": end_at.astimezone(self._BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "functionpara": {"Interval": "1", "Fill": "Previous"},
        }
        body = self._request_json(
            url=self._HF_URL,
            payload=payload,
            access_token=access_token,
            scene="ifind high frequency request",
        )
        if not body.get("tables"):
            return []
        table = body["tables"][0]
        timestamps = table.get("time", []) or []
        values = table.get("table", {}).get("close", []) or []
        return list(zip(timestamps, [self._to_float(item) for item in values]))

    async def _fetch_realtime_previous_close(self, symbol: str) -> float | None:
        return await asyncio.to_thread(self._fetch_realtime_previous_close_sync, symbol)

    def _fetch_realtime_previous_close_sync(self, symbol: str) -> float | None:
        access_token = self._ensure_access_token()
        payload = {
            "codes": symbol,
            "indicators": "preClose",
        }
        body = self._request_json(
            url=self._QUOTE_URL,
            payload=payload,
            access_token=access_token,
            scene="ifind previous close request",
        )
        if not body.get("tables"):
            return None
        table = body["tables"][0].get("table", {})
        values = table.get("preClose", []) or []
        if not values:
            return None
        return self._to_float(values[0])

    def _ensure_access_token(self) -> str:
        with self._token_lock:
            if self._access_token:
                return self._access_token
            return self._refresh_access_token()

    def _refresh_access_token(self) -> str:
        if not self._refresh_token:
            raise RuntimeError("iFinD access_token 已失效，且未配置 IFIND_REFRESH_TOKEN")

        response = self._session.post(
            self._REFRESH_URL,
            headers={
                "Content-Type": "application/json",
                "refresh_token": self._refresh_token,
            },
            timeout=self._request_timeout,
        )
        body = self._decode_json(response=response, scene="ifind refresh token")
        if response.status_code != 200 or body.get("errorcode") not in (0, "0", None):
            raise RuntimeError(
                f"ifind refresh token failed: {body.get('errmsg') or response.status_code}"
            )

        access_token = str(body.get("data", {}).get("access_token", "")).strip()
        if not access_token:
            raise RuntimeError("ifind refresh token succeeded but access_token is empty")

        self._access_token = access_token
        LOGGER.info("ifind minute series access token refreshed")
        return access_token

    def _request_json(
        self, url: str, payload: dict[str, Any], access_token: str, scene: str
    ) -> dict[str, Any]:
        response = self._session.post(
            url,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "access_token": access_token,
                "ifindlang": "cn",
            },
            timeout=self._request_timeout,
        )
        body = self._decode_json(response=response, scene=scene)
        if self._should_refresh_token(response=response, body=body):
            refreshed_token = self._refresh_access_token()
            response = self._session.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "access_token": refreshed_token,
                    "ifindlang": "cn",
                },
                timeout=self._request_timeout,
            )
            body = self._decode_json(
                response=response,
                scene=f"{scene} after refresh",
            )

        if response.status_code != 200:
            raise RuntimeError(f"{scene} failed http_status={response.status_code}")
        if body.get("errorcode") not in (0, "0", None):
            raise RuntimeError(f"{scene} failed: {body.get('errmsg')}")
        return body

    @staticmethod
    def _should_refresh_token(response, body: dict[str, Any]) -> bool:
        if response.status_code == 401:
            return True
        errmsg = str(body.get("errmsg", "")).lower()
        return "token" in errmsg or "unauthorized" in errmsg or "auth" in errmsg

    @staticmethod
    def _decode_json(response, scene: str) -> dict[str, Any]:
        try:
            return response.json()
        except ValueError as err:
            text_preview = response.text[:200].replace("\n", " ")
            raise RuntimeError(f"{scene} returned invalid json: {text_preview}") from err

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _merge_points(
        entry: _MinuteSeriesCacheEntry, points: list[tuple[str, float | None]]
    ) -> None:
        for timestamp, value in points:
            entry.points[timestamp] = value

    @staticmethod
    def _build_snapshot(
        entry: _MinuteSeriesCacheEntry,
        live: bool,
        end_timestamp: str | None = None,
    ) -> MinuteSeriesSnapshot:
        points = tuple(
            MinuteSeriesPoint(timestamp=timestamp, value=value)
            for timestamp, value in entry.points.items()
            if end_timestamp is None or timestamp <= end_timestamp
        )
        return MinuteSeriesSnapshot(
            symbol=entry.symbol,
            previous_close=entry.previous_close,
            points=points,
            live=live,
        )
