import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import requests

from app.aggregator import Aggregator
from app.config import Config
from app.daily_snapshot import build_snapshot
from app.platforms.fx_client import FXClient
from app.platforms.tbank_client import TBankClient


class FXClientTests(unittest.TestCase):
    def setUp(self):
        self.today = datetime.now(Config.get_timezone_obj()).date()
        self.client = FXClient()
        self.client.session = Mock()
        self.client.cbr = Mock()
        self.client.cbr.get_rates.return_value = {
            "effective_date": self.today.isoformat(),
            "rub_per_usd": 80.0,
            "rub_per_eur": 90.0,
            "usd_per_eur": 1.125,
        }

    @staticmethod
    def response(base, price):
        # Related quotes must not be mistaken for the requested pair.
        return Mock(text=(
            '<div data-ticker="JPY" data-exchange="RUB" data-last-price="0.5"></div>'
            f'<div data-ticker="{base}" data-exchange="RUB" data-last-price="{price}"></div>'
        ))

    def test_google_quotes_are_primary_and_cross_rate_is_consistent(self):
        self.client.session.get.side_effect = [
            self.response("USD", "82.5"), self.response("EUR", "99.0")
        ]
        result = self.client.get_rates(self.today)
        self.assertEqual(result["source"], "Google Finance")
        self.assertEqual(result["rub_per_usd"], 82.5)
        self.assertEqual(result["rub_per_eur"], 99.0)
        self.assertEqual(result["usd_per_eur"], 1.2)
        self.client.cbr.get_rates.assert_not_called()
        self.assertIn("USD-RUB", self.client.session.get.call_args_list[0].args[0])

    def test_network_failure_uses_cbr(self):
        self.client.session.get.side_effect = requests.Timeout()
        result = self.client.get_rates(self.today)
        self.assertEqual(result["source"], "Bank of Russia")
        self.client.cbr.get_rates.assert_called_once_with(self.today)

    def test_current_google_markup_ignores_other_instruments(self):
        unrelated = '<span jsname="Pdsbrc"><span>626.53</span></span>'
        self.client.session.get.side_effect = [
            Mock(text=unrelated + (
                '<c-wiz data-p="%.@.[null,null,[&quot;USD&quot;,&quot;RUB&quot;]],null]">'
                '<div><span jsname="Pdsbrc"><span>85.1596</span></span></div>'
                '</c-wiz>' + unrelated
            )),
            self.response("EUR", "95.4426"),
        ]
        result = self.client.get_rates(self.today)
        self.assertEqual(result["rub_per_usd"], 85.1596)
        self.assertEqual(result["source"], "Google Finance")

    def test_bad_html_and_invalid_numbers_use_cbr(self):
        for price in ("0", "-1", "NaN", "Infinity", "invalid", "0.000001"):
            with self.subTest(price=price):
                self.client.session.get.side_effect = None
                self.client.session.get.return_value = self.response("USD", price)
                self.assertEqual(self.client.get_rates(self.today)["source"], "Bank of Russia")
        self.client.session.get.return_value = Mock(text="<html>Consent required</html>")
        self.assertEqual(self.client.get_rates(self.today)["source"], "Bank of Russia")

    def test_second_google_pair_failure_does_not_mix_providers(self):
        self.client.session.get.side_effect = [self.response("USD", "82.5"), requests.Timeout()]
        result = self.client.get_rates(self.today)
        self.assertEqual(result["rub_per_usd"], 80.0)
        self.assertEqual(result["rub_per_eur"], 90.0)

    def test_historical_request_uses_cbr(self):
        yesterday = self.today - timedelta(days=1)
        self.client.get_rates(yesterday)
        self.client.session.get.assert_not_called()
        self.client.cbr.get_rates.assert_called_once_with(yesterday)

    def test_both_providers_failing_raises_without_fixed_rate(self):
        self.client.session.get.side_effect = requests.Timeout()
        self.client.cbr.get_rates.side_effect = requests.ConnectionError()
        with self.assertRaises(requests.ConnectionError):
            self.client.get_rates(self.today)


class SharedConversionTests(unittest.TestCase):
    def setUp(self):
        self.aggregator = Aggregator.__new__(Aggregator)
        for name in ("bybit", "okx", "kucoin", "tbank", "ibkr", "fx_client"):
            setattr(self.aggregator, name, Mock())
        self.rates = {"rub_per_usd": 80.0, "rub_per_eur": 90.0, "usd_per_eur": 1.125}
        self.aggregator.fx_client.get_rates.return_value = self.rates
        self.aggregator.tbank.get_portfolio_summary.return_value = {
            "total_rub": 8000.0, "total_usd": 100.0,
            "main_rub": 8000.0, "iis_rub": 0.0,
        }
        self.aggregator.bybit.get_balance_usd.return_value = 200.0
        for name, value in (
            ("TBANK_API_TOKEN", "token"), ("BYBIT_API_KEY", "key"),
            ("OKX_API_KEY", None), ("KUCOIN_API_KEY", None),
            ("IBKR_FLEX_TOKEN", None), ("IBKR_QUERY_ID", None),
        ):
            config_patch = patch.object(Config, name, value)
            config_patch.start()
            self.addCleanup(config_patch.stop)

    def test_report_and_snapshot_reuse_single_rate_set(self):
        summary = self.aggregator.get_portfolio_summary()
        self.aggregator.fx_client.get_rates.assert_called_once()
        self.aggregator.tbank.get_portfolio_summary.assert_called_once_with(rub_per_usd=80.0)
        self.assertEqual(self.aggregator.get_totals(summary), (300.0, 24000.0))
        snapshot = build_snapshot(summary, summary["fx_rates"], datetime.now(Config.get_timezone_obj()))
        self.assertEqual(snapshot["total_usd"], 300.0)
        self.assertEqual(snapshot["total_rub"], 24000.0)
        message = self.aggregator.format_message(summary)
        self.assertIn("$300", message)
        self.assertIn("₽24 000", message)

    def test_conversion_without_tbank_still_uses_google_rate(self):
        with patch.object(Config, "TBANK_API_TOKEN", None):
            summary = self.aggregator.get_portfolio_summary()
        self.assertEqual(self.aggregator.get_totals(summary), (200.0, 16000.0))

    def test_fx_failure_preserves_balances_and_marks_totals_unavailable(self):
        self.aggregator.fx_client.get_rates.side_effect = requests.ConnectionError()
        summary = self.aggregator.get_portfolio_summary()
        self.assertEqual(summary["tbank_rub"], 8000.0)
        self.assertEqual(summary["crypto_usd"], 200.0)
        self.assertFalse(summary["is_complete"])
        self.assertEqual(self.aggregator.get_totals(summary), (None, None))
        self.assertIn("USD: <b>Unavailable", self.aggregator.format_message(summary))
        snapshot = build_snapshot(summary, summary["fx_rates"], datetime.now(Config.get_timezone_obj()))
        self.assertIsNone(snapshot["total_usd"])

    @patch("app.platforms.tbank_client.Client")
    def test_tbank_converts_usd_to_rub_and_back_using_shared_rate(self, client_class):
        sdk = client_class.return_value.__enter__.return_value
        sdk.users.get_accounts.return_value.accounts = [Mock(id="account", name="Account")]
        sdk.operations.get_portfolio.return_value = Mock(
            total_amount_portfolio=Mock(units=100, nano=0, currency="usd")
        )
        result = TBankClient().get_portfolio_summary(rub_per_usd=80.0)
        self.assertEqual(result["total_rub"], 8000.0)
        self.assertEqual(result["total_usd"], 100.0)
        sdk.market_data.get_last_prices.assert_not_called()


if __name__ == "__main__":
    unittest.main()
