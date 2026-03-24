#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/24 13:30
Desc: A 股实时行情订阅服务
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable
from uuid import uuid4

import pandas as pd
import requests

QuoteFetcher = Callable[[str], pd.DataFrame]
QuoteBatchFetcher = Callable[[list[str]], dict[str, pd.DataFrame]]


@dataclass(frozen=True)
class QuoteSnapshot:
    """
    标准化后的行情快照
    """

    symbol: str
    name: str | None
    price: float | None
    change: float | None
    change_pct: float | None
    volume: float | None
    amount: float | None
    open_price: float | None
    high_price: float | None
    low_price: float | None
    prev_close: float | None
    turnover_rate: float | None
    source_time: str | None
    captured_at: datetime
    raw: dict[str, Any]

    def fingerprint(self) -> tuple[Any, ...]:
        """
        用于去重的关键字段
        """

        return (
            self.price,
            self.change,
            self.change_pct,
            self.volume,
            self.amount,
            self.source_time,
        )


@dataclass(frozen=True)
class QuoteSubscription:
    """
    单个订阅句柄
    """

    subscription_id: str
    symbol: str
    queue: asyncio.Queue[QuoteSnapshot]


class AStockQuoteSubscriptionService:
    """
    A 股实时行情订阅服务。

    默认行为：

    1. 每轮汇总全部活跃股票代码，优先使用东方财富批量接口拉取；
    2. 批量接口缺失或失败时，自动尝试单股接口；
    3. 单股接口失败后，再尝试雪球个股接口；
    4. 整轮没有任何成功结果时，自动指数退避，降低请求频率。
    """

    def __init__(
        self,
        fetcher: QuoteFetcher | None = None,
        batch_fetcher: QuoteBatchFetcher | None = None,
        fallback_fetchers: list[QuoteFetcher] | None = None,
        poll_interval: float = 3.0,
        suppress_duplicate: bool = True,
        batch_size: int = 50,
        fallback_concurrency: int = 3,
        max_backoff: float = 30.0,
    ) -> None:
        if poll_interval <= 0:
            raise ValueError("poll_interval 必须大于 0")
        if batch_size <= 0:
            raise ValueError("batch_size 必须大于 0")
        if fallback_concurrency <= 0:
            raise ValueError("fallback_concurrency 必须大于 0")
        if max_backoff < poll_interval:
            raise ValueError("max_backoff 不能小于 poll_interval")

        self._base_poll_interval = poll_interval
        self._current_poll_interval = poll_interval
        self._max_backoff = max_backoff
        self._suppress_duplicate = suppress_duplicate
        self._batch_size = batch_size
        self._fallback_concurrency = fallback_concurrency

        if batch_fetcher is None:
            if fetcher is not None:
                batch_fetcher = self._make_batch_fetcher(fetcher)
            else:
                batch_fetcher = self._eastmoney_batch_fetch
        self._batch_fetcher = batch_fetcher

        if fallback_fetchers is None:
            if fetcher is not None:
                fallback_fetchers = [fetcher]
            else:
                fallback_fetchers = [
                    self._eastmoney_single_fetch,
                    self._xueqiu_single_fetch,
                ]
        elif fetcher is not None:
            fallback_fetchers = [fetcher, *fallback_fetchers]
        self._fallback_fetchers = fallback_fetchers

        self._lock = asyncio.Lock()
        self._subscribers_by_symbol: dict[
            str, dict[str, asyncio.Queue[QuoteSnapshot]]
        ] = {}
        self._subscription_index: dict[str, str] = {}
        self._latest_snapshots: dict[str, QuoteSnapshot] = {}
        self._last_fingerprints: dict[str, tuple[Any, ...]] = {}
        self._latest_errors: dict[str, Exception] = {}
        self._poll_task: asyncio.Task[None] | None = None

    @property
    def active_symbols(self) -> tuple[str, ...]:
        """
        当前存在订阅的股票代码
        """

        return tuple(sorted(self._subscribers_by_symbol))

    @property
    def subscription_size(self) -> int:
        """
        当前订阅总数
        """

        return len(self._subscription_index)

    @property
    def worker_count(self) -> int:
        """
        当前轮询任务数
        """

        return int(self._poll_task is not None and not self._poll_task.done())

    @property
    def current_poll_interval(self) -> float:
        """
        当前轮询间隔
        """

        return self._current_poll_interval

    def latest_error(self, symbol: str) -> Exception | None:
        """
        获取某个股票最近一次拉取错误
        """

        return self._latest_errors.get(self._normalize_symbol(symbol))

    async def subscribe(
        self, symbol: str, max_queue_size: int = 100
    ) -> QuoteSubscription:
        """
        订阅指定股票行情
        """

        if max_queue_size <= 0:
            raise ValueError("max_queue_size 必须大于 0")
        normalized_symbol = self._normalize_symbol(symbol)
        subscription_id = uuid4().hex
        queue: asyncio.Queue[QuoteSnapshot] = asyncio.Queue(maxsize=max_queue_size)

        async with self._lock:
            symbol_subscribers = self._subscribers_by_symbol.setdefault(
                normalized_symbol, {}
            )
            symbol_subscribers[subscription_id] = queue
            self._subscription_index[subscription_id] = normalized_symbol
            latest_snapshot = self._latest_snapshots.get(normalized_symbol)
            if self._poll_task is None or self._poll_task.done():
                self._poll_task = asyncio.create_task(
                    self._poll_loop(), name="akshare-quote-poller"
                )

        if latest_snapshot is not None:
            self._offer_snapshot(queue, latest_snapshot)

        return QuoteSubscription(
            subscription_id=subscription_id,
            symbol=normalized_symbol,
            queue=queue,
        )

    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        取消订阅
        """

        task_to_cancel: asyncio.Task[None] | None = None

        async with self._lock:
            symbol = self._subscription_index.pop(subscription_id, None)
            if symbol is None:
                return False

            symbol_subscribers = self._subscribers_by_symbol.get(symbol)
            if symbol_subscribers is not None:
                symbol_subscribers.pop(subscription_id, None)
                if not symbol_subscribers:
                    self._subscribers_by_symbol.pop(symbol, None)
                    self._latest_snapshots.pop(symbol, None)
                    self._last_fingerprints.pop(symbol, None)
                    self._latest_errors.pop(symbol, None)

            if not self._subscribers_by_symbol and self._poll_task is not None:
                task_to_cancel = self._poll_task
                self._poll_task = None

        if task_to_cancel is not None:
            task_to_cancel.cancel()
            await asyncio.gather(task_to_cancel, return_exceptions=True)
        return True

    async def stream(
        self, symbol: str, max_queue_size: int = 100
    ) -> AsyncIterator[QuoteSnapshot]:
        """
        以异步生成器的方式输出行情快照，方便直接挂接到 WebSocket 或 SSE。
        """

        subscription = await self.subscribe(
            symbol=symbol, max_queue_size=max_queue_size
        )
        try:
            while True:
                yield await subscription.queue.get()
        finally:
            await self.unsubscribe(subscription.subscription_id)

    async def stop(self) -> None:
        """
        停止全部轮询任务并清空订阅
        """

        async with self._lock:
            task = self._poll_task
            self._poll_task = None
            self._subscribers_by_symbol.clear()
            self._subscription_index.clear()
            self._latest_snapshots.clear()
            self._last_fingerprints.clear()
            self._latest_errors.clear()
            self._current_poll_interval = self._base_poll_interval

        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _poll_loop(self) -> None:
        failure_streak = 0
        try:
            while True:
                async with self._lock:
                    symbols = sorted(self._subscribers_by_symbol)
                if not symbols:
                    return

                snapshots, errors = await self._fetch_snapshots(symbols)
                publish_list: list[
                    tuple[list[asyncio.Queue[QuoteSnapshot]], QuoteSnapshot]
                ] = []

                async with self._lock:
                    for symbol, error in errors.items():
                        if symbol in self._subscribers_by_symbol:
                            self._latest_errors[symbol] = error

                    for symbol, snapshot in snapshots.items():
                        if symbol not in self._subscribers_by_symbol:
                            continue
                        self._latest_snapshots[symbol] = snapshot
                        self._latest_errors.pop(symbol, None)

                        should_publish = True
                        if self._suppress_duplicate:
                            current_fingerprint = snapshot.fingerprint()
                            should_publish = (
                                self._last_fingerprints.get(symbol)
                                != current_fingerprint
                            )
                            self._last_fingerprints[symbol] = current_fingerprint

                        if should_publish:
                            publish_list.append(
                                (
                                    list(
                                        self._subscribers_by_symbol.get(
                                            symbol, {}
                                        ).values()
                                    ),
                                    snapshot,
                                )
                            )

                for queues, snapshot in publish_list:
                    for queue in queues:
                        self._offer_snapshot(queue, snapshot)

                if snapshots:
                    failure_streak = 0
                else:
                    failure_streak += 1

                self._current_poll_interval = min(
                    self._base_poll_interval * (2**failure_streak),
                    self._max_backoff,
                )
                await asyncio.sleep(self._current_poll_interval)
        except asyncio.CancelledError:
            raise
        finally:
            async with self._lock:
                if self._poll_task is asyncio.current_task():
                    self._poll_task = None

    async def _fetch_snapshots(
        self, symbols: list[str]
    ) -> tuple[dict[str, QuoteSnapshot], dict[str, Exception]]:
        quote_frames: dict[str, pd.DataFrame] = {}
        errors: dict[str, Exception] = {}

        for symbol_chunk in self._chunk_symbols(symbols):
            try:
                batch_result = await asyncio.to_thread(self._batch_fetcher, symbol_chunk)
            except Exception as err:
                for symbol in symbol_chunk:
                    errors[symbol] = err
            else:
                for symbol, quote_df in batch_result.items():
                    normalized_symbol = self._normalize_symbol(symbol)
                    if (
                        normalized_symbol in symbols
                        and quote_df is not None
                        and not quote_df.empty
                    ):
                        quote_frames[normalized_symbol] = quote_df

        unresolved_symbols = [
            symbol for symbol in symbols if symbol not in quote_frames
        ]
        if unresolved_symbols and self._fallback_fetchers:
            fallback_frames, fallback_errors = await self._fetch_from_fallbacks(
                unresolved_symbols
            )
            quote_frames.update(fallback_frames)
            for symbol, err in fallback_errors.items():
                if symbol not in quote_frames:
                    errors[symbol] = err

        snapshots: dict[str, QuoteSnapshot] = {}
        for symbol, quote_df in quote_frames.items():
            try:
                snapshots[symbol] = self._build_snapshot(symbol=symbol, quote_df=quote_df)
            except Exception as err:
                errors[symbol] = err
        return snapshots, errors

    async def _fetch_from_fallbacks(
        self, symbols: list[str]
    ) -> tuple[dict[str, pd.DataFrame], dict[str, Exception]]:
        semaphore = asyncio.Semaphore(self._fallback_concurrency)

        async def fetch_symbol(
            symbol: str,
        ) -> tuple[str, pd.DataFrame | None, Exception | None]:
            last_error: Exception | None = None
            async with semaphore:
                for fetcher in self._fallback_fetchers:
                    try:
                        quote_df = await asyncio.to_thread(fetcher, symbol)
                    except Exception as err:  # noqa: PERF203
                        last_error = err
                        continue
                    if quote_df is not None and not quote_df.empty:
                        return symbol, quote_df, None
                    last_error = ValueError("empty quote data")
            return symbol, None, last_error or ValueError("no quote data")

        results = await asyncio.gather(
            *(fetch_symbol(symbol) for symbol in symbols)
        )
        quote_frames: dict[str, pd.DataFrame] = {}
        errors: dict[str, Exception] = {}
        for symbol, quote_df, err in results:
            if quote_df is not None:
                quote_frames[symbol] = quote_df
            elif err is not None:
                errors[symbol] = err
        return quote_frames, errors

    def _chunk_symbols(self, symbols: list[str]) -> list[list[str]]:
        return [
            symbols[index : index + self._batch_size]
            for index in range(0, len(symbols), self._batch_size)
        ]

    def _make_batch_fetcher(self, fetcher: QuoteFetcher) -> QuoteBatchFetcher:
        def inner(symbols: list[str]) -> dict[str, pd.DataFrame]:
            return {symbol: fetcher(symbol) for symbol in symbols}

        return inner

    @staticmethod
    def _eastmoney_single_fetch(symbol: str) -> pd.DataFrame:
        from akshare.stock.stock_ask_bid_em import stock_bid_ask_em

        return stock_bid_ask_em(symbol=symbol)

    @staticmethod
    def _xueqiu_single_fetch(symbol: str) -> pd.DataFrame:
        from akshare.stock.stock_xq import stock_individual_spot_xq

        return stock_individual_spot_xq(
            symbol=AStockQuoteSubscriptionService._to_xq_symbol(symbol)
        )

    @staticmethod
    def _eastmoney_batch_fetch(symbols: list[str]) -> dict[str, pd.DataFrame]:
        if not symbols:
            return {}

        url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
        params = {
            "ut": "f057cbcbce2a86e2866ab8877db1d059",
            "fltt": "2",
            "invt": "2",
            "fields": "f12,f14,f2,f4,f3,f5,f6,f17,f15,f16,f18,f8,f50,f71",
            "secids": ",".join(
                AStockQuoteSubscriptionService._to_em_secid(symbol)
                for symbol in symbols
            )
            + ",?v=08926209912590994",
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data_json = response.json()
        diff_items = data_json.get("data", {}).get("diff", [])

        quote_frames: dict[str, pd.DataFrame] = {}
        for item in diff_items:
            symbol = AStockQuoteSubscriptionService._normalize_symbol(item["f12"])
            raw = {
                "代码": symbol,
                "名称": item.get("f14"),
                "最新": item.get("f2"),
                "涨跌": item.get("f4"),
                "涨幅": item.get("f3"),
                "总手": item.get("f5"),
                "金额": item.get("f6"),
                "今开": item.get("f17"),
                "最高": item.get("f15"),
                "最低": item.get("f16"),
                "昨收": item.get("f18"),
                "换手": item.get("f8"),
                "量比": item.get("f50"),
                "均价": item.get("f71"),
            }
            quote_frames[symbol] = pd.DataFrame(
                list(raw.items()), columns=["item", "value"]
            )
        return quote_frames

    @classmethod
    def _build_snapshot(cls, symbol: str, quote_df: pd.DataFrame) -> QuoteSnapshot:
        if not {"item", "value"}.issubset(quote_df.columns):
            raise ValueError("fetcher 必须返回包含 item 和 value 列的 DataFrame")

        raw = {
            str(item): cls._normalize_scalar(value)
            for item, value in quote_df.loc[:, ["item", "value"]].itertuples(
                index=False, name=None
            )
        }
        return QuoteSnapshot(
            symbol=symbol,
            name=cls._first_value(raw, "名称"),
            price=cls._to_float(cls._first_value(raw, "最新", "最新价", "现价")),
            change=cls._to_float(cls._first_value(raw, "涨跌", "涨跌额")),
            change_pct=cls._to_float(cls._first_value(raw, "涨幅", "涨跌幅")),
            volume=cls._to_float(cls._first_value(raw, "总手", "成交量")),
            amount=cls._to_float(cls._first_value(raw, "金额", "成交额")),
            open_price=cls._to_float(cls._first_value(raw, "今开", "开盘")),
            high_price=cls._to_float(cls._first_value(raw, "最高")),
            low_price=cls._to_float(cls._first_value(raw, "最低")),
            prev_close=cls._to_float(cls._first_value(raw, "昨收")),
            turnover_rate=cls._to_float(
                cls._first_value(raw, "换手", "换手率", "周转率")
            ),
            source_time=cls._to_text(cls._first_value(raw, "时间")),
            captured_at=datetime.now(timezone.utc),
            raw=raw,
        )

    @staticmethod
    def _offer_snapshot(
        queue: asyncio.Queue[QuoteSnapshot], snapshot: QuoteSnapshot
    ) -> None:
        if queue.full():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        queue.put_nowait(snapshot)

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

    @staticmethod
    def _to_xq_symbol(symbol: str) -> str:
        normalized_symbol = AStockQuoteSubscriptionService._normalize_symbol(symbol)
        if normalized_symbol.startswith(("8", "4")):
            return f"BJ{normalized_symbol}"
        if normalized_symbol.startswith("6"):
            return f"SH{normalized_symbol}"
        return f"SZ{normalized_symbol}"

    @staticmethod
    def _to_em_secid(symbol: str) -> str:
        normalized_symbol = AStockQuoteSubscriptionService._normalize_symbol(symbol)
        market_code = "1" if normalized_symbol.startswith("6") else "0"
        return f"{market_code}.{normalized_symbol}"

    @staticmethod
    def _first_value(raw: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in raw:
                return raw[key]
        return None

    @staticmethod
    def _normalize_scalar(value: Any) -> Any:
        if hasattr(value, "item"):
            try:
                value = value.item()
            except ValueError:
                pass
        if isinstance(value, str):
            value = value.strip()
            return value or None
        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass
        return value

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
            if not value or value == "-":
                return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_text(value: Any) -> str | None:
        if value is None:
            return None
        return str(value)
