import asyncio
import logging
import re
import unittest
from unittest.mock import AsyncMock, Mock, patch

import requests

from app.aggregator import Aggregator
from app.config import Config
from app.platforms.ibkr_client import IBKRClient
from app.platforms.kucoin_client import KucoinClient
from app.platforms.okx_client import OkxClient
from app.telegram_client import TelegramBot
from app.utils.logging_redaction import RedactionFilter


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


class KucoinClientTests(unittest.TestCase):
    def setUp(self):
        patches = [
            patch.object(Config, "KUCOIN_API_KEY", "key"),
            patch.object(Config, "KUCOIN_API_SECRET", "secret"),
            patch.object(Config, "KUCOIN_API_PASSPHRASE", "passphrase"),
        ]
        for config_patch in patches:
            config_patch.start()
            self.addCleanup(config_patch.stop)

    @patch("app.platforms.kucoin_client.requests.get")
    def test_sums_funding_trading_and_usdt_futures(self, get):
        requested_urls = []

        def response_for(url, **kwargs):
            requested_urls.append(url)
            if url.endswith("/api/v1/prices"):
                return FakeResponse(
                    {"code": "200000", "data": {"USDT": "1", "USDC": "1"}}
                )
            if url.endswith("/api/v1/accounts"):
                return FakeResponse(
                    {
                        "code": "200000",
                        "data": [
                            {"type": "main", "currency": "USDT", "balance": "100"},
                            {"type": "trade", "currency": "USDT", "balance": "50"},
                        ],
                    }
                )
            if url.endswith("/api/v1/contracts/active"):
                return FakeResponse(
                    {
                        "code": "200000",
                        "data": [{"settleCurrency": "USDT"}],
                    }
                )
            if url.endswith("/api/v1/account-overview?currency=USDT"):
                return FakeResponse(
                    {
                        "code": "200000",
                        "data": {"currency": "USDT", "accountEquity": "8000"},
                    }
                )
            if "account-overview?currency=" in url:
                return FakeResponse(
                    {"code": "400100", "msg": "unsupported currency", "data": None}
                )
            raise AssertionError(f"Unexpected URL: {url}")

        get.side_effect = response_for

        total = KucoinClient().get_balance_usd()

        self.assertEqual(total, 8150.0)
        self.assertTrue(
            any(url.endswith("account-overview?currency=USDT") for url in requested_urls)
        )

    @patch("app.platforms.kucoin_client.requests.get")
    def test_price_failure_does_not_return_a_partial_zero(self, get):
        get.return_value = FakeResponse({}, status_code=503)

        with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
            KucoinClient().get_balance_usd()


class OkxClientTests(unittest.TestCase):
    @patch("app.platforms.okx_client.FundingClient")
    def test_uses_total_asset_valuation(self, funding_client):
        sdk = funding_client.return_value
        sdk.get_asset_valuation.return_value = {
            "code": "0",
            "data": [
                {
                    "totalBal": "9123.45",
                    "details": {
                        "funding": "1000",
                        "trading": "7000",
                        "earn": "1123.45",
                    },
                }
            ],
            "msg": "",
        }

        with (
            patch.object(Config, "OKX_API_KEY", "key"),
            patch.object(Config, "OKX_API_SECRET", "secret"),
            patch.object(Config, "OKX_API_PASSPHRASE", "passphrase"),
        ):
            client = OkxClient()

        self.assertEqual(client.get_balance_usd(), 9123.45)
        sdk.get_asset_valuation.assert_called_once_with(ccy="USD")


class ErrorSanitizationTests(unittest.TestCase):
    def test_ibkr_http_error_does_not_expose_request_url(self):
        response = requests.Response()
        response.status_code = 401
        response.url = "https://example.test/GetStatement?t=FAKE_SECRET&q=123"
        error = requests.HTTPError(response=response)

        message = IBKRClient._safe_exception_message(error, "IBKR Flex query")

        self.assertEqual(message, "IBKR Flex query failed with HTTP 401")
        self.assertNotIn("FAKE_SECRET", message)
        self.assertNotIn("example.test", message)

    def test_logging_filter_redacts_percent_style_arguments(self):
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="request failed for token %s",
            args=("VERY_SECRET_TOKEN",),
            exc_info=None,
        )
        redactor = RedactionFilter([re.escape("VERY_SECRET_TOKEN")])

        redactor.filter(record)

        self.assertEqual(
            record.getMessage(), "request failed for token [REDACTED]"
        )


class CompletenessTests(unittest.TestCase):
    def test_aggregator_marks_configured_platform_failure_incomplete(self):
        aggregator = Aggregator.__new__(Aggregator)
        aggregator.kucoin = Mock()
        aggregator.kucoin.get_balance_usd.side_effect = RuntimeError("offline")
        aggregator.bybit = Mock()
        aggregator.okx = Mock()
        aggregator.tbank = Mock()
        aggregator.ibkr = Mock()

        with (
            patch.object(Config, "BYBIT_API_KEY", None),
            patch.object(Config, "OKX_API_KEY", None),
            patch.object(Config, "KUCOIN_API_KEY", "key"),
            patch.object(Config, "TBANK_API_TOKEN", None),
            patch.object(Config, "IBKR_FLEX_TOKEN", None),
            patch.object(Config, "IBKR_QUERY_ID", None),
        ):
            summary = aggregator.get_portfolio_summary()

        self.assertFalse(summary["is_complete"])
        self.assertIn("kucoin", summary["errors"])

    @patch("app.telegram_client.history_manager.save_snapshot")
    def test_partial_summary_is_not_saved(self, save_snapshot):
        bot = TelegramBot.__new__(TelegramBot)
        bot.aggregator = Mock()
        bot.aggregator.get_totals.return_value = (100.0, 9000.0)

        saved = bot._save_snapshot_if_complete(
            {"is_complete": False, "errors": {"kucoin": "offline"}}
        )

        self.assertFalse(saved)
        save_snapshot.assert_not_called()


class AsyncAggregationTests(unittest.IsolatedAsyncioTestCase):
    async def test_exchange_refresh_is_offloaded_from_event_loop(self):
        bot = TelegramBot.__new__(TelegramBot)
        bot._aggregation_lock = asyncio.Lock()
        bot.aggregator = Mock()
        expected = {"is_complete": True}

        with patch("app.telegram_client.asyncio.to_thread", new=AsyncMock(return_value=expected)) as to_thread:
            result = await bot._get_portfolio_summary()

        self.assertIs(result, expected)
        to_thread.assert_awaited_once_with(bot.aggregator.get_portfolio_summary)


if __name__ == "__main__":
    unittest.main()
