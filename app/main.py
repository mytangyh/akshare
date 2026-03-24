#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/24 15:10
Desc: Docker 部署入口
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
AKSHARE_ROOT = PROJECT_ROOT / "akshare"
SERVICE_ROOT = AKSHARE_ROOT / "service"


def _ensure_package(name: str, path: Path) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    module.__path__ = [str(path)]
    sys.modules[name] = module


def _load_module(module_name: str, module_path: Path):
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _get_poll_interval() -> float:
    raw_value = os.getenv("QUOTE_POLL_INTERVAL", "3.0")
    try:
        poll_interval = float(raw_value)
    except ValueError as err:
        raise ValueError("QUOTE_POLL_INTERVAL 必须是数字") from err
    if poll_interval <= 0:
        raise ValueError("QUOTE_POLL_INTERVAL 必须大于 0")
    return poll_interval


_ensure_package("akshare", AKSHARE_ROOT)
_ensure_package("akshare.service", SERVICE_ROOT)
_load_module(
    "akshare.service.quote_subscription",
    SERVICE_ROOT / "quote_subscription.py",
)
quote_websocket_module = _load_module(
    "akshare.service.quote_websocket",
    SERVICE_ROOT / "quote_websocket.py",
)

app = quote_websocket_module.create_quote_websocket_app(
    poll_interval=_get_poll_interval()
)

