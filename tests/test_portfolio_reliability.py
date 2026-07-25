import asyncio
import io
import json
import logging
import os
import re
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import requests

from app.aggregator import Aggregator
from app.config import Config
from app.platforms.ibkr_client import IBKRClient
from app.platforms.kucoin_client import KucoinClient
from app.platforms.okx_client import OkxClient
from app.telegram_client import TelegramBot
from app import chart as chart_module
from app import history_manager, settings_manager
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

    def test_failed_platform_is_unavailable_and_total_is_clearly_partial(self):
        aggregator = Aggregator.__new__(Aggregator)
        summary = {
            "bybit_usd": 100.0,
            "okx_usd": 0.0,
            "kucoin_usd": 50.0,
            "crypto_usd": 150.0,
            "tbank_rub": 0.0,
            "tbank_usd": 0.0,
            "ibkr_usd": 200.0,
            "errors": {"okx": "offline"},
            "is_complete": False,
        }

        with (
            patch.object(Config, "BYBIT_API_KEY", "key"),
            patch.object(Config, "OKX_API_KEY", "key"),
            patch.object(Config, "KUCOIN_API_KEY", "key"),
            patch.object(Config, "TBANK_API_TOKEN", None),
            patch.object(Config, "IBKR_FLEX_TOKEN", "token"),
        ):
            message = aggregator.format_message(summary)

        self.assertIn("OKX: <b>Unavailable ⚠️</b>", message)
        self.assertIn("<b>PARTIAL TOTAL</b>", message)
        self.assertIn("Excludes unavailable data: OKX", message)
        self.assertNotIn("OKX: <code>$0</code>", message)

    def test_tbank_failure_does_not_display_fallback_rub_total(self):
        aggregator = Aggregator.__new__(Aggregator)
        summary = {
            "bybit_usd": 100.0,
            "okx_usd": 200.0,
            "kucoin_usd": 0.0,
            "crypto_usd": 300.0,
            "tbank_rub": 0.0,
            "tbank_usd": 0.0,
            "ibkr_usd": 0.0,
            "errors": {"tbank": "offline"},
            "is_complete": False,
        }

        with (
            patch.object(Config, "BYBIT_API_KEY", "key"),
            patch.object(Config, "OKX_API_KEY", "key"),
            patch.object(Config, "KUCOIN_API_KEY", None),
            patch.object(Config, "TBANK_API_TOKEN", "token"),
            patch.object(Config, "IBKR_FLEX_TOKEN", None),
        ):
            message = aggregator.format_message(summary)

        self.assertIn("T-Bank: <b>Unavailable ⚠️</b>", message)
        self.assertIn("RUB: <b>Unavailable ⚠️</b>", message)
        self.assertNotIn("RUB: <code>₽27 000</code>", message)


class HistoryUxTests(unittest.TestCase):
    @patch("app.history_manager._load")
    def test_performance_metrics_use_recent_daily_and_weekly_baselines(self, load):
        today = datetime.now().date()
        load.return_value = {
            (today - timedelta(days=1)).strftime("%d-%m-%Y"): {
                "USD": 100.0,
                "RUB": 9000.0,
            },
            (today - timedelta(days=7)).strftime("%d-%m-%Y"): {
                "USD": 80.0,
                "RUB": 7200.0,
            },
        }

        metrics = history_manager.get_performance_metrics(110.0, 9900.0)

        self.assertEqual([metric["label"] for metric in metrics], ["1D", "7D"])
        self.assertEqual(metrics[0]["usd_change"], 10.0)
        self.assertAlmostEqual(metrics[0]["percent_change"], 10.0)
        self.assertEqual(metrics[1]["usd_change"], 30.0)

    @patch("app.history_manager._load")
    def test_stale_history_is_not_presented_as_one_day_performance(self, load):
        stale_date = datetime.now().date() - timedelta(days=30)
        load.return_value = {
            stale_date.strftime("%d-%m-%Y"): {"USD": 100.0, "RUB": 9000.0}
        }

        self.assertEqual(history_manager.get_performance_metrics(110.0, 9900.0), [])


class SettingsPersistenceTests(unittest.TestCase):
    def test_schedule_settings_survive_reload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            settings_file = os.path.join(temp_dir, "bot_settings.json")
            with patch.object(settings_manager, "_SETTINGS_FILE", settings_file):
                settings_manager.save_schedule(240, False)
                loaded = settings_manager.load_settings(120)

                self.assertEqual(
                    loaded,
                    {
                        "poll_interval_minutes": 240,
                        "scheduled_reports_enabled": False,
                    },
                )
                with open(settings_file, "r", encoding="utf-8") as file:
                    self.assertEqual(json.load(file)["poll_interval_minutes"], 240)


class ChartUxTests(unittest.TestCase):
    def setUp(self):
        self.summary = {
            "bybit_usd": 100.0,
            "okx_usd": 200.0,
            "kucoin_usd": 50.0,
            "crypto_usd": 350.0,
            "tbank_usd": 300.0,
            "ibkr_usd": 500.0,
        }

    def test_allocation_chart_supports_both_groupings(self):
        for grouping in ("platform", "asset_class"):
            with self.subTest(grouping=grouping):
                buffer = chart_module.build_pie_chart(self.summary, grouping)
                self.assertEqual(buffer.read(8), b"\x89PNG\r\n\x1a\n")

    def test_trend_chart_renders_all_points(self):
        entries = [
            {"date": "03-01-2026", "USD": 120.0, "RUB": 10800.0},
            {"date": "02-01-2026", "USD": 90.0, "RUB": 8100.0},
            {"date": "01-01-2026", "USD": 100.0, "RUB": 9000.0},
        ]

        buffer = chart_module.build_portfolio_chart(entries)

        self.assertEqual(buffer.read(8), b"\x89PNG\r\n\x1a\n")


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

    async def test_command_menu_is_registered_for_authorized_chat(self):
        bot = TelegramBot.__new__(TelegramBot)
        bot.chat_id = "12345"
        application = Mock()
        application.bot.set_my_commands = AsyncMock()

        await bot._post_init(application)

        application.bot.set_my_commands.assert_awaited_once()
        commands = application.bot.set_my_commands.await_args.args[0]
        self.assertEqual(
            [command.command for command in commands],
            ["status", "history", "allocation", "settings", "export", "help"],
        )

    async def test_replacing_chart_photo_uses_multipart_attachment_uri(self):
        bot = TelegramBot.__new__(TelegramBot)
        query = Mock()
        query.message.photo = [Mock()]
        query.edit_message_media = AsyncMock()

        await bot._replace_query_with_photo(
            query,
            io.BytesIO(b"fake-png"),
            "Chart caption",
            bot._get_history_keyboard("RUB"),
            "portfolio_rub.png",
        )

        media = query.edit_message_media.await_args.kwargs["media"]
        self.assertTrue(media.media.attach_uri.startswith("attach://"))
        self.assertIsNotNone(media.media.attach_name)


class StatusPerformanceTests(unittest.TestCase):
    @patch("app.telegram_client.history_manager.get_performance_metrics")
    def test_complete_status_includes_daily_and_weekly_change(self, get_metrics):
        get_metrics.return_value = [
            {
                "label": "1D",
                "usd_change": 50.0,
                "percent_change": 5.0,
            },
            {
                "label": "7D",
                "usd_change": -20.0,
                "percent_change": -2.0,
            },
        ]
        bot = TelegramBot.__new__(TelegramBot)
        bot.aggregator = Mock()
        bot.aggregator.get_totals.return_value = (1050.0, 94500.0)

        text = bot._format_performance_context({"is_complete": True})

        self.assertIn("<b>PERFORMANCE</b>", text)
        self.assertIn("1D: <code>▲ $50 (5.0%)</code>", text)
        self.assertIn("7D: <code>▼ $20 (2.0%)</code>", text)

    def test_partial_status_does_not_compare_with_complete_history(self):
        bot = TelegramBot.__new__(TelegramBot)
        bot.aggregator = Mock()

        text = bot._format_performance_context({"is_complete": False})

        self.assertEqual(text, "")
        bot.aggregator.get_totals.assert_not_called()


if __name__ == "__main__":
    unittest.main()
