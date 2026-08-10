import asyncio
import csv
import io
import tempfile
import unittest
from contextlib import ExitStack
from datetime import date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from t_tech.invest import AccountType

from app.config import Config
from app.daily_snapshot import CSV_HEADERS, build_csv, build_snapshot
from app.platforms.cbr_fx_client import CBRFXClient
from app.platforms.tbank_client import TBankClient
from app.telegram_client import TelegramBot
from app import snapshot_database


def configured_source_patches():
    return (
        patch.object(Config, "BYBIT_API_KEY", "key"),
        patch.object(Config, "OKX_API_KEY", "key"),
        patch.object(Config, "KUCOIN_API_KEY", "key"),
        patch.object(Config, "TBANK_API_TOKEN", "token"),
        patch.object(Config, "IBKR_FLEX_TOKEN", "token"),
        patch.object(Config, "IBKR_QUERY_ID", "query"),
    )


class SnapshotCalculationTests(unittest.TestCase):
    def setUp(self):
        self.patches = configured_source_patches()
        for config_patch in self.patches:
            config_patch.start()
            self.addCleanup(config_patch.stop)
        self.timestamp = Config.get_timezone_obj().localize(
            datetime(2026, 8, 9, 17, 0, 0)
        )
        self.summary = {
            "tbank_main_rub": 1000.0,
            "tbank_iis_rub": 2000.0,
            "crypto_usd": 300.0,
            "ibkr_usd": 400.0,
            "errors": {},
        }
        self.fx_rates = {
            "rub_per_usd": 80.0,
            "rub_per_eur": 90.0,
            "usd_per_eur": 1.125,
        }

    def test_complete_snapshot_calculates_all_totals(self):
        row = build_snapshot(self.summary, self.fx_rates, self.timestamp)

        self.assertEqual(row["snapshot_at"], "2026-08-09T17:00:00+02:00")
        self.assertEqual(row["total_rub"], 59000.0)
        self.assertEqual(row["total_usd"], 737.5)
        self.assertEqual(row["total_eur"], 655.56)

    def test_failed_crypto_makes_combined_value_and_totals_null(self):
        self.summary["errors"] = {"okx": "offline"}

        row = build_snapshot(self.summary, self.fx_rates, self.timestamp)

        self.assertIsNone(row["crypto_usd"])
        self.assertEqual(row["ibkr_usd"], 400.0)
        self.assertEqual(row["rub_per_usd"], 80.0)
        self.assertIsNone(row["total_rub"])
        self.assertIsNone(row["total_usd"])
        self.assertIsNone(row["total_eur"])

    def test_failed_tbank_makes_both_tbank_values_null(self):
        self.summary["errors"] = {"tbank": "offline"}

        row = build_snapshot(self.summary, self.fx_rates, self.timestamp)

        self.assertIsNone(row["tbank_main_rub"])
        self.assertIsNone(row["tbank_iis_rub"])
        self.assertIsNone(row["total_rub"])

    def test_missing_fx_keeps_balances_and_blanks_rates_and_totals(self):
        row = build_snapshot(self.summary, None, self.timestamp)

        self.assertEqual(row["crypto_usd"], 300.0)
        self.assertIsNone(row["rub_per_usd"])
        self.assertIsNone(row["total_usd"])

    def test_failed_ibkr_is_null_and_does_not_become_zero(self):
        self.summary["errors"] = {"ibkr": "offline"}

        row = build_snapshot(self.summary, self.fx_rates, self.timestamp)

        self.assertIsNone(row["ibkr_usd"])
        self.assertIsNone(row["total_eur"])


class SnapshotDatabaseTests(unittest.TestCase):
    def test_database_persists_chronologically_and_rejects_duplicate_date(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_file = Path(temp_dir) / "portfolio.sqlite3"
            with patch.object(snapshot_database, "_DATABASE_FILE", database_file):
                snapshot_database.initialize_database()
                newer = self._row("2026-08-10T17:00:00+02:00", 20.0)
                older = self._row("2026-08-09T17:00:00+02:00", 10.0)

                self.assertTrue(snapshot_database.insert_snapshot(newer))
                self.assertTrue(snapshot_database.insert_snapshot(older))
                self.assertFalse(
                    snapshot_database.insert_snapshot(
                        self._row("2026-08-09T18:00:00+02:00", 999.0)
                    )
                )

                rows = snapshot_database.get_all_snapshots()
                self.assertEqual(
                    [row["snapshot_at"] for row in rows],
                    [older["snapshot_at"], newer["snapshot_at"]],
                )
                self.assertEqual(rows[0]["total_usd"], 10.0)
                self.assertTrue(
                    snapshot_database.has_snapshot_for_date(date(2026, 8, 9))
                )

    @staticmethod
    def _row(timestamp: str, total_usd: float) -> dict:
        return {
            "snapshot_at": timestamp,
            "tbank_main_rub": 1.0,
            "tbank_iis_rub": 2.0,
            "crypto_usd": 3.0,
            "ibkr_usd": 4.0,
            "rub_per_usd": 80.0,
            "rub_per_eur": 90.0,
            "usd_per_eur": 1.125,
            "total_rub": 563.0,
            "total_usd": total_usd,
            "total_eur": 6.25,
        }


class CsvExportTests(unittest.TestCase):
    def test_csv_uses_bom_semicolons_decimal_commas_and_blank_nulls(self):
        row = SnapshotDatabaseTests._row(
            "2026-08-09T17:00:00+02:00",
            7.04,
        )
        row["ibkr_usd"] = None
        row["total_rub"] = None

        payload = build_csv([row]).getvalue()
        self.assertTrue(payload.startswith(b"\xef\xbb\xbf"))
        decoded = payload.decode("utf-8-sig")
        self.assertIn("\r\n", decoded)

        records = list(csv.reader(io.StringIO(decoded), delimiter=";"))
        self.assertEqual(tuple(records[0]), CSV_HEADERS)
        self.assertEqual(records[1][0], "09.08.2026 17:00:00")
        self.assertEqual(records[1][1], "1,00")
        self.assertEqual(records[1][4], "")
        self.assertEqual(records[1][5], "80,0000")
        self.assertEqual(records[1][7], "1,125000")
        self.assertEqual(records[1][8], "")


class CBRFXClientTests(unittest.TestCase):
    def setUp(self):
        self.client = CBRFXClient()
        self.client.session = Mock()

    def _response(self, xml: bytes) -> Mock:
        response = Mock()
        response.content = xml
        response.raise_for_status = Mock()
        self.client.session.get.return_value = response
        return response

    def test_parses_weekend_effective_date_and_normalizes_nominals(self):
        self._response(
            b"""<?xml version="1.0" encoding="windows-1251"?>
            <ValCurs Date="08.08.2026">
              <Valute><CharCode>USD</CharCode><Nominal>10</Nominal><Value>800,0000</Value></Valute>
              <Valute><CharCode>EUR</CharCode><Nominal>1</Nominal><Value>90,0000</Value><VunitRate>90,0000</VunitRate></Valute>
            </ValCurs>"""
        )

        rates = self.client.get_rates(date(2026, 8, 9))

        self.assertEqual(rates["effective_date"], "2026-08-08")
        self.assertEqual(rates["rub_per_usd"], 80.0)
        self.assertEqual(rates["rub_per_eur"], 90.0)
        self.assertEqual(rates["usd_per_eur"], 1.125)
        self.client.session.get.assert_called_once_with(
            CBRFXClient.ENDPOINT,
            params={"date_req": "09/08/2026"},
            timeout=(10, 30),
        )

    def test_rejects_malformed_or_incomplete_xml(self):
        for xml, message in (
            (b"<not-closed", "not valid XML"),
            (
                b'<ValCurs Date="08.08.2026"><Valute><CharCode>USD</CharCode><Nominal>1</Nominal><Value>80,0</Value></Valute></ValCurs>',
                "missing required currencies: EUR",
            ),
        ):
            with self.subTest(message=message):
                self._response(xml)
                with self.assertRaisesRegex(ValueError, message):
                    self.client.get_rates(date(2026, 8, 9))


class TBankGroupingTests(unittest.TestCase):
    @patch("app.platforms.tbank_client.Client")
    def test_groups_main_and_iis_but_preserves_all_account_total(self, client_class):
        sdk = client_class.return_value.__enter__.return_value
        sdk.users.get_accounts.return_value.accounts = [
            Mock(id="main", name="Main", type=AccountType.ACCOUNT_TYPE_TINKOFF),
            Mock(id="iis", name="IIS", type=AccountType.ACCOUNT_TYPE_TINKOFF_IIS),
            Mock(id="box", name="Box", type=AccountType.ACCOUNT_TYPE_INVEST_BOX),
        ]

        def portfolio(account_id):
            amounts = {"main": 1000, "iis": 2000, "box": 3000}
            return Mock(
                total_amount_portfolio=Mock(
                    units=amounts[account_id],
                    nano=0,
                    currency="rub",
                )
            )

        sdk.operations.get_portfolio.side_effect = portfolio
        with (
            patch.object(Config, "TBANK_API_TOKEN", "token"),
            patch.object(TBankClient, "_get_usd_rub_rate", return_value=80.0),
        ):
            result = TBankClient().get_portfolio_summary()

        self.assertEqual(result["main_rub"], 1000.0)
        self.assertEqual(result["iis_rub"], 2000.0)
        self.assertEqual(result["total_rub"], 6000.0)
        self.assertEqual(result["total_usd"], 75.0)


class DatabaseScheduleTests(unittest.TestCase):
    def setUp(self):
        self.bot = TelegramBot.__new__(TelegramBot)
        self.bot.application = Mock()
        self.bot.application.job_queue = Mock()
        self.bot.application.job_queue.get_jobs_by_name.return_value = []
        self.bot.database_snapshot_job = AsyncMock()

    def test_schedules_daily_job_and_after_hour_catch_up(self):
        now = Config.get_timezone_obj().localize(datetime(2026, 8, 9, 18, 0))
        with (
            patch.object(Config, "DATABASE_SNAPSHOT_HOUR", 17),
            patch("app.telegram_client.datetime") as datetime_mock,
            patch.object(snapshot_database, "has_snapshot_for_date", return_value=False),
        ):
            datetime_mock.now.return_value = now
            self.bot._schedule_database_job()

        self.assertEqual(self.bot.application.job_queue.run_daily.call_count, 2)
        calls = {
            call.kwargs["name"]: call.kwargs
            for call in self.bot.application.job_queue.run_daily.call_args_list
        }
        daily = calls["daily_database_snapshot"]
        recovery = calls["daily_database_snapshot_recovery"]
        self.assertEqual((daily["time"].hour, daily["time"].minute), (17, 0))
        self.assertEqual((recovery["time"].hour, recovery["time"].minute), (17, 15))
        self.assertEqual(getattr(daily["time"].tzinfo, "zone", None), "Europe/Paris")
        self.assertEqual(
            daily["job_kwargs"],
            {
                "misfire_grace_time": 900,
                "coalesce": True,
                "max_instances": 1,
            },
        )
        self.bot.application.job_queue.run_once.assert_called_once()
        self.assertEqual(
            self.bot.application.job_queue.run_once.call_args.kwargs["job_kwargs"],
            daily["job_kwargs"],
        )

    def test_before_hour_does_not_schedule_catch_up(self):
        now = Config.get_timezone_obj().localize(datetime(2026, 8, 9, 16, 59))
        with (
            patch.object(Config, "DATABASE_SNAPSHOT_HOUR", 17),
            patch("app.telegram_client.datetime") as datetime_mock,
            patch.object(snapshot_database, "has_snapshot_for_date", return_value=False),
        ):
            datetime_mock.now.return_value = now
            self.bot._schedule_database_job()

        self.assertEqual(self.bot.application.job_queue.run_daily.call_count, 2)
        self.bot.application.job_queue.run_once.assert_not_called()


class DatabaseSnapshotJobTests(unittest.IsolatedAsyncioTestCase):
    async def test_job_saves_partial_row_once_without_sending_telegram_message(self):
        bot = TelegramBot.__new__(TelegramBot)
        bot._database_snapshot_lock = asyncio.Lock()
        bot._aggregation_lock = asyncio.Lock()
        bot.aggregator = Mock()
        bot.aggregator.get_portfolio_summary.return_value = {
            "tbank_main_rub": 1000.0,
            "tbank_iis_rub": 2000.0,
            "crypto_usd": 300.0,
            "ibkr_usd": 0.0,
            "errors": {"ibkr": "offline"},
        }
        bot.fx_client = Mock()
        bot.fx_client.get_rates.return_value = {
            "rub_per_usd": 80.0,
            "rub_per_eur": 90.0,
            "usd_per_eur": 1.125,
        }
        context = Mock()
        context.bot.send_message = AsyncMock()
        inserted_rows = []

        def insert(row):
            inserted_rows.append(row)
            return True

        with ExitStack() as stack:
            for config_patch in configured_source_patches():
                stack.enter_context(config_patch)
            stack.enter_context(
                patch.object(
                    snapshot_database,
                    "has_snapshot_for_date",
                    return_value=False,
                )
            )
            stack.enter_context(
                patch.object(snapshot_database, "insert_snapshot", side_effect=insert)
            )
            await bot.database_snapshot_job(context)

        self.assertEqual(len(inserted_rows), 1)
        self.assertIsNone(inserted_rows[0]["ibkr_usd"])
        self.assertIsNone(inserted_rows[0]["total_usd"])
        context.bot.send_message.assert_not_awaited()

    async def test_recovery_job_exits_before_scanning_when_row_exists(self):
        bot = TelegramBot.__new__(TelegramBot)
        bot._database_snapshot_lock = asyncio.Lock()
        bot.aggregator = Mock()
        bot.fx_client = Mock()

        with patch.object(
            snapshot_database,
            "has_snapshot_for_date",
            return_value=True,
        ):
            await bot.database_snapshot_job(Mock())

        bot.aggregator.get_portfolio_summary.assert_not_called()
        bot.fx_client.get_rates.assert_not_called()


class DatabaseCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_database_sends_clear_message(self):
        bot = TelegramBot.__new__(TelegramBot)
        bot.chat_id = "123"
        update = Mock()
        update.effective_chat.id = 123
        update.message.reply_text = AsyncMock()
        update.message.reply_document = AsyncMock()

        with patch.object(snapshot_database, "get_all_snapshots", return_value=[]):
            await bot.database_command(update, Mock())

        update.message.reply_text.assert_awaited_once()
        update.message.reply_document.assert_not_awaited()

    async def test_database_command_sends_dated_csv_attachment(self):
        bot = TelegramBot.__new__(TelegramBot)
        bot.chat_id = "123"
        update = Mock()
        update.effective_chat.id = 123
        update.message.reply_text = AsyncMock()
        update.message.reply_document = AsyncMock()
        rows = [
            SnapshotDatabaseTests._row(
                "2026-08-09T17:00:00+02:00",
                7.04,
            )
        ]

        with patch.object(snapshot_database, "get_all_snapshots", return_value=rows):
            await bot.database_command(update, Mock())

        update.message.reply_document.assert_awaited_once()
        document = update.message.reply_document.await_args.kwargs["document"]
        self.assertRegex(document.filename, r"portfolio_database_\d{4}-\d{2}-\d{2}\.csv")
        self.assertTrue(document.input_file_content.startswith(b"\xef\xbb\xbf"))


if __name__ == "__main__":
    unittest.main()
