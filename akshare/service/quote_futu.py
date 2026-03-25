#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/25 13:40
Desc: Futu OpenAPI 行情抓取适配
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

import pandas as pd

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


class FutuQuoteBatchFetcher:
    """
    基于 Futu OpenAPI 的批量行情抓取器。

    设计目标：

    1. 通过 OpenD 获取正式行情，而不是依赖公开网页抓取；
    2. 与 AStockQuoteSubscriptionService 的 batch_fetcher 约定兼容；
    3. 维持一个长连接的 OpenQuoteContext，降低重复建连成本。
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 11111,
        api: Any | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._api = api
        self._quote_ctx = None
        self._lock = threading.Lock()

    def __call__(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        if not symbols:
            return {}

        futu_codes = [self._to_futu_code(symbol) for symbol in symbols]
        with self._lock:
            quote_ctx = self._ensure_quote_ctx()
            self._subscribe_quote(quote_ctx=quote_ctx, futu_codes=futu_codes)
            quote_df = self._get_stock_quote(quote_ctx=quote_ctx, futu_codes=futu_codes)

        quote_frames: dict[str, pd.DataFrame] = {}
        for _, row in quote_df.iterrows():
            symbol = self._from_futu_code(str(row.get("code", "")))
            quote_frames[symbol] = self._row_to_quote_df(symbol=symbol, row=row)

        return quote_frames

    def close(self) -> None:
        with self._lock:
            if self._quote_ctx is None:
                return
            try:
                self._quote_ctx.close()
            finally:
                self._quote_ctx = None
                LOGGER.info(
                    "futu quote context closed host=%s port=%s",
                    self._host,
                    self._port,
                )

    def _ensure_quote_ctx(self):
        if self._quote_ctx is not None:
            return self._quote_ctx

        api = self._load_api()
        self._quote_ctx = api.OpenQuoteContext(host=self._host, port=self._port)
        LOGGER.info(
            "futu quote context opened host=%s port=%s",
            self._host,
            self._port,
        )
        return self._quote_ctx

    def _load_api(self):
        if self._api is None:
            try:
                import futu  # type: ignore
            except ImportError as err:
                raise ImportError(
                    "使用 Futu 行情源需要安装 futu-api，可执行: pip install futu-api"
                ) from err
            self._api = futu
        return self._api

    def _subscribe_quote(self, quote_ctx, futu_codes: list[str]) -> None:
        api = self._load_api()
        ret, data = quote_ctx.subscribe(
            futu_codes,
            [api.SubType.QUOTE],
            subscribe_push=False,
        )
        if ret != api.RET_OK:
            raise RuntimeError(f"futu subscribe failed: {data}")
        LOGGER.info("futu quote subscribed codes=%s", ",".join(futu_codes))

    def _get_stock_quote(self, quote_ctx, futu_codes: list[str]) -> pd.DataFrame:
        api = self._load_api()
        ret, data = quote_ctx.get_stock_quote(futu_codes)
        if ret != api.RET_OK:
            raise RuntimeError(f"futu get_stock_quote failed: {data}")
        if data is None or data.empty:
            raise ValueError("futu get_stock_quote returned empty data")
        return data

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
    def _to_futu_code(cls, symbol: str) -> str:
        normalized_symbol = cls._normalize_symbol(symbol)
        if normalized_symbol.startswith(("8", "4")):
            return f"BJ.{normalized_symbol}"
        if normalized_symbol.startswith("6"):
            return f"SH.{normalized_symbol}"
        return f"SZ.{normalized_symbol}"

    @classmethod
    def _from_futu_code(cls, futu_code: str) -> str:
        normalized_code = str(futu_code).strip().upper()
        if "." not in normalized_code:
            return cls._normalize_symbol(normalized_code)
        _, symbol = normalized_code.split(".", 1)
        return cls._normalize_symbol(symbol)

    @classmethod
    def _row_to_quote_df(cls, symbol: str, row: pd.Series) -> pd.DataFrame:
        last_price = cls._to_float(row.get("last_price"))
        prev_close = cls._to_float(row.get("prev_close_price"))
        change = cls._to_float(row.get("change_val"))
        if change is None and last_price is not None and prev_close is not None:
            change = last_price - prev_close

        change_pct = cls._to_float(row.get("change_rate"))
        if change_pct is None and change is not None and prev_close not in (None, 0):
            change_pct = change / prev_close * 100

        data_date = cls._to_text(row.get("data_date"))
        data_time = cls._to_text(row.get("data_time"))
        source_time = None
        if data_date and data_time:
            source_time = f"{data_date} {data_time}"
        elif data_time:
            source_time = data_time
        elif data_date:
            source_time = data_date

        raw = {
            "代码": symbol,
            "Futu代码": cls._to_text(row.get("code")),
            "名称": cls._to_text(row.get("name")),
            "最新": last_price,
            "涨跌": change,
            "涨幅": change_pct,
            "总手": cls._to_float(row.get("volume")),
            "金额": cls._to_float(row.get("turnover")),
            "今开": cls._to_float(row.get("open_price")),
            "最高": cls._to_float(row.get("high_price")),
            "最低": cls._to_float(row.get("low_price")),
            "昨收": prev_close,
            "换手": cls._to_float(row.get("turnover_rate")),
            "振幅": cls._to_float(row.get("amplitude")),
            "时间": source_time,
            "停牌": row.get("suspension"),
            "状态": cls._to_text(row.get("sec_status")),
        }
        normalized_raw = {
            item: value
            for item, value in raw.items()
            if value is not None and value != ""
        }
        return pd.DataFrame(
            list(normalized_raw.items()),
            columns=["item", "value"],
        )

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
