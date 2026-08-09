import logging
from html import escape
from app.config import Config
from app.platforms.bybit_client import BybitClient
from app.platforms.okx_client import OkxClient
from app.platforms.kucoin_client import KucoinClient
from app.platforms.tbank_client import TBankClient
from app.platforms.ibkr_client import IBKRClient

logger = logging.getLogger(__name__)


class Aggregator:
    def __init__(self):
        self.bybit = BybitClient()
        self.okx = OkxClient()
        self.kucoin = KucoinClient()
        self.tbank = TBankClient()
        self.ibkr = IBKRClient()
        # FX and other platforms to be added later

    def get_portfolio_summary(self):
        summary = {
            "bybit_usd": 0.0,
            "okx_usd": 0.0,
            "kucoin_usd": 0.0,
            "tbank_rub": 0.0,
            "tbank_usd": 0.0,
            "tbank_main_rub": 0.0,
            "tbank_iis_rub": 0.0,
            "ibkr_usd": 0.0,
            "crypto_usd": 0.0,
            "errors": {},
            "is_complete": True,
        }

        # ByBit
        if Config.BYBIT_API_KEY:
            try:
                summary["bybit_usd"] = self.bybit.get_balance_usd()
            except Exception as e:
                summary["errors"]["bybit"] = str(e)
                logger.error(f"Bybit aggregation error: {e}")

        # OKX
        if Config.OKX_API_KEY:
            try:
                summary["okx_usd"] = self.okx.get_balance_usd()
            except Exception as e:
                summary["errors"]["okx"] = str(e)
                logger.error(f"OKX aggregation error: {e}")

        # KuCoin
        if Config.KUCOIN_API_KEY:
            try:
                summary["kucoin_usd"] = self.kucoin.get_balance_usd()
            except Exception as e:
                summary["errors"]["kucoin"] = str(e)
                logger.error(f"KuCoin aggregation error: {e}")

        # T-Bank
        if Config.TBANK_API_TOKEN:
            try:
                tbank_data = self.tbank.get_portfolio_summary()
                if "error" in tbank_data:
                    summary["errors"]["tbank"] = tbank_data["error"]
                else:
                    summary["tbank_rub"] = tbank_data.get("total_rub", 0.0)
                    summary["tbank_usd"] = tbank_data.get("total_usd", 0.0)
                    summary["tbank_main_rub"] = tbank_data.get("main_rub", 0.0)
                    summary["tbank_iis_rub"] = tbank_data.get("iis_rub", 0.0)
                    summary["tbank_accounts"] = tbank_data.get("accounts", [])
            except Exception as e:
                summary["errors"]["tbank"] = str(e)
                logger.error(f"T-Bank aggregation error: {e}")

        # IBKR Flex (Passive)
        if Config.IBKR_FLEX_TOKEN and Config.IBKR_QUERY_ID:
            try:
                ibkr_data = self.ibkr.get_portfolio_summary()
                summary["ibkr_usd"] = ibkr_data.get("total_usd", 0.0)
                if "error" in ibkr_data:
                    summary["errors"]["ibkr"] = ibkr_data["error"]
            except Exception as e:
                summary["errors"]["ibkr"] = str(e)
                logger.error(f"IBKR aggregation error: {e}")

        summary["crypto_usd"] = (
            summary["bybit_usd"] + summary["okx_usd"] + summary["kucoin_usd"]
        )
        summary["is_complete"] = not bool(summary["errors"])

        return summary

    def format_message(self, summary):
        from datetime import datetime

        current_date = datetime.now(Config.get_timezone_obj()).strftime("%d %b %Y")
        errors = summary.get("errors", {})

        # Helper for formatting: no decimals, space as thousand separator
        def fmt(val, currency="$"):
            # val is float or Decimal
            # {:,.0f} gives comma separator. We replace comma with space.
            s = f"{val:,.0f}".replace(",", " ")
            symbol = "$" if currency == "USD" else "₽"
            return f"{symbol}{s}"

        # T-Bank
        tbank_rub_val = summary.get("tbank_rub", 0.0)
        tbank_usd_val = summary.get("tbank_usd", 0.0)

        # We need an implied rate to calculate Total RUB for USD items
        implied_rate = 90.0
        if tbank_usd_val > 0:
            implied_rate = tbank_rub_val / tbank_usd_val

        # Crypto
        bybit_usd = summary.get("bybit_usd", 0.0)
        okx_usd = summary.get("okx_usd", 0.0)
        kucoin_usd = summary.get("kucoin_usd", 0.0)
        crypto_usd = summary.get("crypto_usd", 0.0)

        # IBKR
        ibkr_usd = summary.get("ibkr_usd", 0.0)

        # Totals
        grand_total_usd = crypto_usd + tbank_usd_val + ibkr_usd
        grand_total_rub = tbank_rub_val + ((crypto_usd + ibkr_usd) * implied_rate)

        # Build Message
        lines = []
        lines.append(f"📈 <b>Portfolio summary {current_date}</b>")
        lines.append("")

        if Config.TBANK_API_TOKEN:
            lines.append("<b>T-BANK RUB</b>")

            if "tbank" in errors:
                lines.append("T-Bank: <b>Unavailable ⚠️</b>")
            else:
                tbank_accounts = summary.get("tbank_accounts", [])
                for acc in tbank_accounts:
                    lines.append(
                        f"{escape(str(acc['name']))}: "
                        f"<code>{fmt(acc['rub'], 'RUB')}</code>"
                    )
                lines.append("Total T-BANK")
                lines.append(f"RUB: <code>{fmt(tbank_rub_val, 'RUB')}</code>")
                lines.append(f"USD: <code>{fmt(tbank_usd_val, 'USD')}</code>")
            lines.append("")

        lines.append("<b>CRYPTO USD</b>")

        crypto_sources = [
            ("bybit", "Bybit", bybit_usd, bool(Config.BYBIT_API_KEY)),
            ("okx", "OKX", okx_usd, bool(Config.OKX_API_KEY)),
            ("kucoin", "KuCoin", kucoin_usd, bool(Config.KUCOIN_API_KEY)),
        ]
        crypto_has_errors = False
        for key, name, value, configured in crypto_sources:
            if not configured:
                continue
            if key in errors:
                lines.append(f"{name}: <b>Unavailable ⚠️</b>")
                crypto_has_errors = True
            else:
                lines.append(f"{name}: <code>{fmt(value, 'USD')}</code>")

        crypto_label = "Partial crypto" if crypto_has_errors else "Total crypto"
        lines.append(f"{crypto_label}: <code>{fmt(crypto_usd, 'USD')}</code>")
        lines.append("")

        # IBKR Section
        if Config.IBKR_FLEX_TOKEN:
            lines.append("<b>STOCKS USD</b>")
            ibkr_line = f"IBKR: <code>{fmt(ibkr_usd, 'USD')}</code>"
            if "ibkr" in errors:
                ibkr_line = "IBKR: <b>Unavailable ⚠️</b>"
            lines.append(ibkr_line)
            lines.append("")

        total_label = "PARTIAL TOTAL" if errors else "TOTAL"
        lines.append(f"<b>{total_label}</b>")
        lines.append(f"USD: <code>{fmt(grand_total_usd, 'USD')}</code>")
        if "tbank" in errors:
            lines.append("RUB: <b>Unavailable ⚠️</b>")
        else:
            lines.append(f"RUB: <code>{fmt(grand_total_rub, 'RUB')}</code>")

        if errors:
            display_names = {
                "bybit": "Bybit",
                "okx": "OKX",
                "kucoin": "KuCoin",
                "tbank": "T-Bank",
                "ibkr": "IBKR",
            }
            excluded = ", ".join(
                display_names.get(key, key) for key in sorted(errors)
            )
            lines.append("")
            lines.append(f"⚠️ <b>Excludes unavailable data: {excluded}.</b>")
            lines.append("<i>History was not saved for this partial snapshot.</i>")

        return "\n".join(lines)

    def get_totals(self, summary) -> tuple[float, float]:
        """
        Return (grand_total_usd, grand_total_rub) from a summary dict.
        Uses the same logic as format_message so values are consistent.
        """
        tbank_rub_val = summary.get("tbank_rub", 0.0)
        tbank_usd_val = summary.get("tbank_usd", 0.0)
        crypto_usd = summary.get("crypto_usd", 0.0)
        ibkr_usd = summary.get("ibkr_usd", 0.0)

        implied_rate = 90.0
        if tbank_usd_val > 0:
            implied_rate = tbank_rub_val / tbank_usd_val

        grand_total_usd = crypto_usd + tbank_usd_val + ibkr_usd
        grand_total_rub = tbank_rub_val + ((crypto_usd + ibkr_usd) * implied_rate)
        return grand_total_usd, grand_total_rub
