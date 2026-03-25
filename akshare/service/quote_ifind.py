#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/25 15:20
Desc: iFinD HTTP 行情抓取适配
"""

from __future__ import annotations

from datetime import datetime, time as dt_time
import logging
import os
import threading
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
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


class IFindQuoteBatchFetcher:
    """
    基于 iFinD HTTP API 的批量行情抓取器。

    当前仅请求 `changeRatio`，用于映射 WebSocket 输出中的 `change_pct`。
    """

    _QUOTE_URL = "https://quantapi.51ifind.com/api/v1/real_time_quotation"
    _REFRESH_URL = "https://quantapi.51ifind.com/api/v1/get_access_token"
    _INDICATORS = "latest,changeRatio"
    _BEIJING_TZ = ZoneInfo("Asia/Shanghai")
    _TRADING_SESSIONS = (
        (dt_time(9, 30), dt_time(11, 30)),
        (dt_time(13, 0), dt_time(15, 0)),
    )

    def __init__(
        self,
        access_token: str | None = None,
        refresh_token: str | None = None,
        request_timeout: float = 10.0,
        session: requests.Session | None = None,
        skip_off_hours_requests: bool = True,
        now_func=None,
    ) -> None:
        self._access_token = (access_token or "").strip()
        self._refresh_token = (refresh_token or "").strip()
        self._request_timeout = request_timeout
        self._session = session or requests.Session()
        self._owns_session = session is None
        self._skip_off_hours_requests = skip_off_hours_requests
        self._now_func = now_func or self._default_now
        self._lock = threading.Lock()
        self._latest_frames: dict[str, pd.DataFrame] = {}

        if not self._access_token and not self._refresh_token:
            raise ValueError(
                "使用 iFinD 行情源需要设置 IFIND_ACCESS_TOKEN 或 IFIND_REFRESH_TOKEN"
            )

    def __call__(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        if not symbols:
            return {}

        normalized_symbols = [self._normalize_symbol(symbol) for symbol in symbols]
        cached_frames: dict[str, pd.DataFrame] = {}
        request_symbols = normalized_symbols
        with self._lock:
            if self._skip_off_hours_requests and not self._is_trading_time(
                current_time=self._now_func()
            ):
                cached_frames = self._get_cached_frames(normalized_symbols)
                request_symbols = [
                    symbol for symbol in normalized_symbols if symbol not in cached_frames
                ]
                if not request_symbols:
                    LOGGER.info(
                        "ifind quote request skipped reason=off_hours symbols=%s cached=%s",
                        ",".join(normalized_symbols),
                        len(cached_frames),
                    )
                    return cached_frames
                LOGGER.info(
                    "ifind quote request reduced reason=off_hours cached=%s missing_symbols=%s",
                    len(cached_frames),
                    ",".join(request_symbols),
                )

            access_token = self._ensure_access_token()
            payload = {
                "codes": ",".join(self._to_ifind_code(symbol) for symbol in request_symbols),
                "indicators": self._INDICATORS,
            }
            body = self._request_quote(payload=payload, access_token=access_token)

        quote_frames = {
            symbol: quote_df.copy(deep=True) for symbol, quote_df in cached_frames.items()
        }
        for item in body.get("tables", []):
            symbol = self._from_ifind_code(str(item.get("thscode", "")))
            quote_df = self._table_to_quote_df(symbol=symbol, item=item)
            if not quote_df.empty:
                quote_frames[symbol] = quote_df
        with self._lock:
            self._latest_frames.update(
                {
                    symbol: quote_df.copy(deep=True)
                    for symbol, quote_df in quote_frames.items()
                }
            )
        return quote_frames

    def close(self) -> None:
        if self._owns_session:
            self._session.close()
            LOGGER.info("ifind session closed")

    def _ensure_access_token(self) -> str:
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
        LOGGER.info("ifind access token refreshed")
        return access_token

    def _request_quote(self, payload: dict[str, Any], access_token: str) -> dict[str, Any]:
        response = self._session.post(
            self._QUOTE_URL,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "access_token": access_token,
            },
            timeout=self._request_timeout,
        )
        body = self._decode_json(response=response, scene="ifind quote request")

        if self._should_refresh_token(response=response, body=body):
            refreshed_token = self._refresh_access_token()
            response = self._session.post(
                self._QUOTE_URL,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "access_token": refreshed_token,
                },
                timeout=self._request_timeout,
            )
            body = self._decode_json(
                response=response, scene="ifind quote request after refresh"
            )

        if response.status_code != 200:
            raise RuntimeError(f"ifind quote request failed http_status={response.status_code}")
        if body.get("errorcode") not in (0, "0", None):
            raise RuntimeError(f"ifind quote request failed: {body.get('errmsg')}")
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

    @classmethod
    def _table_to_quote_df(cls, symbol: str, item: dict[str, Any]) -> pd.DataFrame:
        table = item.get("table", {}) or {}
        source_time = cls._extract_first(item.get("time"))
        latest_price = cls._to_float(cls._extract_first(table.get("latest")))
        change_pct = cls._to_float(cls._extract_first(table.get("changeRatio")))
        prev_close = cls._infer_prev_close(
            latest_price=latest_price,
            change_pct=change_pct,
        )
        change = None
        if latest_price is not None and prev_close is not None:
            change = latest_price - prev_close
        raw = {
            "代码": symbol,
            "iFinD代码": cls._to_text(item.get("thscode")),
            "最新": latest_price,
            "涨跌": change,
            "昨收": prev_close,
            "涨幅": change_pct,
            "时间": cls._to_text(source_time),
        }
        normalized_raw = {
            key: value for key, value in raw.items() if value is not None and value != ""
        }
        return pd.DataFrame(list(normalized_raw.items()), columns=["item", "value"])

    def _get_cached_frames(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        return {
            symbol: self._latest_frames[symbol].copy(deep=True)
            for symbol in symbols
            if symbol in self._latest_frames
        }

    @classmethod
    def _is_trading_time(cls, current_time: datetime) -> bool:
        if current_time.tzinfo is None:
            beijing_now = current_time.replace(tzinfo=cls._BEIJING_TZ)
        else:
            beijing_now = current_time.astimezone(cls._BEIJING_TZ)
        if beijing_now.weekday() >= 5:
            return False
        current_clock = beijing_now.time().replace(tzinfo=None)
        return any(
            start <= current_clock < end for start, end in cls._TRADING_SESSIONS
        )

    @classmethod
    def _default_now(cls) -> datetime:
        return datetime.now(cls._BEIJING_TZ)

    @staticmethod
    def _infer_prev_close(
        latest_price: float | None, change_pct: float | None
    ) -> float | None:
        if latest_price is None or change_pct is None:
            return None
        denominator = 1 + change_pct / 100
        if denominator == 0:
            return None
        return latest_price / denominator

    @staticmethod
    def _extract_first(value: Any) -> Any:
        if isinstance(value, list):
            return value[0] if value else None
        return value

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        normalized_symbol = str(symbol).strip().upper()
        for prefix in ("SH", "SZ", "BJ"):
            if normalized_symbol.startswith(prefix):
                normalized_symbol = normalized_symbol[2:]
                break
        if len(normalized_symbol) != 6 or not normalized_symbol.isdigit():
            raise ValueError("symbol 必须是 6 位 A 股代码，例如 000001 或 SZ000001")
        return normalized_symbol

    @classmethod
    def _to_ifind_code(cls, symbol: str) -> str:
        normalized_symbol = cls._normalize_symbol(symbol)
        if normalized_symbol.startswith(("8", "4")):
            return f"{normalized_symbol}.BJ"
        if normalized_symbol.startswith("6"):
            return f"{normalized_symbol}.SH"
        return f"{normalized_symbol}.SZ"

    @classmethod
    def _from_ifind_code(cls, thscode: str) -> str:
        normalized_code = str(thscode).strip().upper()
        if "." in normalized_code:
            symbol, _ = normalized_code.split(".", 1)
            return cls._normalize_symbol(symbol)
        return cls._normalize_symbol(normalized_code)

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_text(value: Any) -> str | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass
        text = str(value).strip()
        return text or None
