#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/25 15:30
Desc: iFinD 行情适配测试
"""

from datetime import datetime
import importlib.util
import os
import sys
import types
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


PROJECT_ROOT = Path(__file__).resolve().parents[1]
AKSHARE_ROOT = PROJECT_ROOT / "akshare"
SERVICE_ROOT = AKSHARE_ROOT / "service"

_ensure_package("akshare", AKSHARE_ROOT)
_ensure_package("akshare.service", SERVICE_ROOT)

QUOTE_IFIND_MODULE = _load_module(
    "akshare.service.quote_ifind",
    SERVICE_ROOT / "quote_ifind.py",
)
QUOTE_SUBSCRIPTION_MODULE = _load_module(
    "akshare.service.quote_subscription",
    SERVICE_ROOT / "quote_subscription.py",
)
QUOTE_WEBSOCKET_MODULE = _load_module(
    "akshare.service.quote_websocket",
    SERVICE_ROOT / "quote_websocket.py",
)

IFindQuoteBatchFetcher = QUOTE_IFIND_MODULE.IFindQuoteBatchFetcher


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]):
        self._responses = list(responses)
        self.calls = []
        self.closed = False

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "json": json,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return self._responses.pop(0)

    def close(self):
        self.closed = True


def test_ifind_batch_fetcher_refreshes_token_and_maps_change_ratio():
    session = _FakeSession(
        responses=[
            _FakeResponse(
                200,
                {
                    "errorcode": 0,
                    "errmsg": "success",
                    "data": {"access_token": "new-access-token"},
                },
            ),
            _FakeResponse(
                200,
                {
                    "errorcode": 0,
                    "errmsg": "Success!",
                    "tables": [
                        {
                            "thscode": "300033.SZ",
                            "time": ["2026-03-25 15:05:15"],
                            "table": {"latest": [311.55], "changeRatio": [1.23]},
                        }
                    ],
                },
            ),
        ]
    )
    fetcher = IFindQuoteBatchFetcher(
        access_token="",
        refresh_token="refresh-token",
        request_timeout=5,
        session=session,
        now_func=lambda: datetime(2026, 3, 25, 14, 30, 0),
    )

    result = fetcher(["300033"])

    assert list(result) == ["300033"]
    raw = dict(result["300033"].itertuples(index=False, name=None))
    assert raw["代码"] == "300033"
    assert raw["iFinD代码"] == "300033.SZ"
    assert raw["最新"] == 311.55
    assert raw["涨跌"] == raw["最新"] - raw["昨收"]
    assert raw["涨幅"] == 1.23
    assert raw["时间"] == "2026-03-25 15:05:15"
    assert session.calls[0]["url"].endswith("/get_access_token")
    assert session.calls[1]["url"].endswith("/real_time_quotation")
    assert session.calls[1]["headers"]["access_token"] == "new-access-token"


def test_ifind_batch_fetcher_uses_cached_frames_off_hours():
    trading_time = datetime(2026, 3, 25, 14, 30, 0)
    off_hours_time = datetime(2026, 3, 25, 20, 0, 0)
    current_time = {"value": trading_time}
    session = _FakeSession(
        responses=[
            _FakeResponse(
                200,
                {
                    "errorcode": 0,
                    "errmsg": "Success!",
                    "tables": [
                        {
                            "thscode": "300033.SZ",
                            "time": ["2026-03-25 14:30:00"],
                            "table": {"latest": [311.55], "changeRatio": [1.23]},
                        }
                    ],
                },
            )
        ]
    )
    fetcher = IFindQuoteBatchFetcher(
        access_token="access-token",
        request_timeout=5,
        session=session,
        now_func=lambda: current_time["value"],
    )

    first_result = fetcher(["300033"])
    current_time["value"] = off_hours_time
    second_result = fetcher(["300033"])

    assert len(session.calls) == 1
    assert dict(first_result["300033"].itertuples(index=False, name=None)) == dict(
        second_result["300033"].itertuples(index=False, name=None)
    )


def test_ifind_batch_fetcher_bootstraps_missing_symbol_off_hours():
    session = _FakeSession(
        responses=[
            _FakeResponse(
                200,
                {
                    "errorcode": 0,
                    "errmsg": "Success!",
                    "tables": [
                        {
                            "thscode": "300033.SZ",
                            "time": ["2026-03-25 20:00:00"],
                            "table": {"latest": [311.55], "changeRatio": [1.23]},
                        }
                    ],
                },
            )
        ]
    )
    fetcher = IFindQuoteBatchFetcher(
        access_token="access-token",
        request_timeout=5,
        session=session,
        now_func=lambda: datetime(2026, 3, 25, 20, 0, 0),
    )

    result = fetcher(["300033"])

    assert list(result) == ["300033"]
    assert len(session.calls) == 1
    assert session.calls[0]["json"]["codes"] == "300033.SZ"


def test_create_default_quote_service_supports_ifind_provider():
    original_values = {
        "QUOTE_PROVIDER": os.environ.get("QUOTE_PROVIDER"),
        "IFIND_ACCESS_TOKEN": os.environ.get("IFIND_ACCESS_TOKEN"),
        "IFIND_REFRESH_TOKEN": os.environ.get("IFIND_REFRESH_TOKEN"),
        "IFIND_REQUEST_TIMEOUT": os.environ.get("IFIND_REQUEST_TIMEOUT"),
        "IFIND_SKIP_OFF_HOURS_REQUESTS": os.environ.get(
            "IFIND_SKIP_OFF_HOURS_REQUESTS"
        ),
    }

    class _FakeIFindQuoteBatchFetcher:
        def __init__(
            self,
            access_token,
            refresh_token,
            request_timeout,
            skip_off_hours_requests,
        ):
            self.access_token = access_token
            self.refresh_token = refresh_token
            self.request_timeout = request_timeout
            self.skip_off_hours_requests = skip_off_hours_requests

    fake_module = types.ModuleType("akshare.service.quote_ifind")
    fake_module.IFindQuoteBatchFetcher = _FakeIFindQuoteBatchFetcher
    original_module = sys.modules.get("akshare.service.quote_ifind")
    sys.modules["akshare.service.quote_ifind"] = fake_module

    try:
        os.environ["QUOTE_PROVIDER"] = "ifind"
        os.environ["IFIND_ACCESS_TOKEN"] = "access-token"
        os.environ["IFIND_REFRESH_TOKEN"] = "refresh-token"
        os.environ["IFIND_REQUEST_TIMEOUT"] = "8"
        os.environ["IFIND_SKIP_OFF_HOURS_REQUESTS"] = "1"

        service = QUOTE_WEBSOCKET_MODULE._create_default_quote_service(3.0)

        assert isinstance(service._batch_fetcher, _FakeIFindQuoteBatchFetcher)
        assert service._batch_fetcher.access_token == "access-token"
        assert service._batch_fetcher.refresh_token == "refresh-token"
        assert service._batch_fetcher.request_timeout == 8.0
        assert service._batch_fetcher.skip_off_hours_requests is True
        assert service._fallback_fetchers == []
        assert service._suppress_duplicate is False
    finally:
        if original_module is not None:
            sys.modules["akshare.service.quote_ifind"] = original_module
        else:
            sys.modules.pop("akshare.service.quote_ifind", None)
        for key, value in original_values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
