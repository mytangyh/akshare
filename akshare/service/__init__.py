#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2026/3/24 13:30
Desc: 服务层扩展
"""

from akshare.service.quote_subscription import (
    AStockQuoteSubscriptionService,
    QuoteSnapshot,
    QuoteSubscription,
)
from akshare.service.quote_futu import FutuQuoteBatchFetcher
from akshare.service.quote_ifind import IFindQuoteBatchFetcher
from akshare.service.quote_websocket import (
    create_quote_websocket_app,
    run_quote_websocket_app,
)
