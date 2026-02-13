import logging
from app.config import Config
from app.platforms.bybit_client import BybitClient
from app.platforms.okx_client import OkxClient
from app.platforms.tbank_client import TBankClient
from app.platforms.ibkr_client import IBKRClient

logger = logging.getLogger(__name__)


class Aggregator:
    def __init__(self):
        self.bybit = BybitClient()
        self.okx = OkxClient()
        self.tbank = TBankClient()
        self.ibkr = IBKRClient()
        # FX and other platforms to be added later

    def get_portfolio_summary(self):
        summary = {
            "bybit_usd": 0.0,
            "okx_usd": 0.0,
            "tbank_rub": 0.0,
            "tbank_usd": 0.0,
            "ibkr_usd": 0.0,
            "crypto_usd": 0.0,
            "errors": {},
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

        # T-Bank
        if Config.TBANK_API_TOKEN:
            try:
                tbank_data = self.tbank.get_portfolio_summary()
                if "error" in tbank_data:
                    summary["errors"]["tbank"] = tbank_data["error"]
                else:
                    summary["tbank_rub"] = tbank_data.get("total_rub", 0.0)
                    summary["tbank_usd"] = tbank_data.get("total_usd", 0.0)
            except Exception as e:
                summary["errors"]["tbank"] = str(e)
                logger.error(f"T-Bank aggregation error: {e}")

        # IBKR Flex (Passive)
        if Config.IBKR_FLEX_TOKEN and Config.IBKR_QUERY_ID:
            try:
                ibkr_data = self.ibkr.get_portfolio_summary()
                if "error" in ibkr_data:
                    summary["errors"]["ibkr"] = ibkr_data["error"]
                else:
                    summary["ibkr_usd"] = ibkr_data.get("total_usd", 0.0)
            except Exception as e:
                summary["errors"]["ibkr"] = str(e)
                logger.error(f"IBKR aggregation error: {e}")

        summary["crypto_usd"] = summary["bybit_usd"] + summary["okx_usd"]

        return summary

    def format_message(self, summary):
        # Template:
        # Portfolio summary {current date}
        #
        # <b>RUB</b>
        # T-BANK: <code>₽{amount_in_rub}</code> or <code>${amount_in_USD}</code>
        #
        # <b>CRYPTO USD</b>
        # ByBit: <code>${amount_in_usd}</code>
        # OKX: <code>${amount_in_usd}</code>
        # Total crypto: <code>${amount_in_usd}</code>
        #
        # <b>STOCKS USD</b>
        # IBKR: <code>${amount_in_usd}</code>
        #
        # TOTAL (USD): <code>${amount_in_usd}</code>
        # TOTAL (RUB): <code>₽{amount_in_rub}</code>

        from datetime import datetime

        current_date = datetime.now().strftime("%d-%m-%Y")

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
        crypto_usd = summary.get("crypto_usd", 0.0)

        # IBKR
        ibkr_usd = summary.get("ibkr_usd", 0.0)

        # Totals
        grand_total_usd = crypto_usd + tbank_usd_val + ibkr_usd
        grand_total_rub = tbank_rub_val + ((crypto_usd + ibkr_usd) * implied_rate)

        # Build Message
        lines = []
        lines.append(f"💵 <b>Portfolio summary {current_date}</b>")
        lines.append("")

        lines.append("<b>RUB</b>")

        tbank_line_base = f"T-BANK: <code>{fmt(tbank_rub_val, 'RUB')}</code> or <code>{fmt(tbank_usd_val, 'USD')}</code>"
        if "tbank" in summary["errors"]:
            tbank_line_base += f" (ERROR: {summary['errors']['tbank']})"
        lines.append(tbank_line_base)
        lines.append("")

        lines.append("<b>CRYPTO USD</b>")

        bybit_line = f"ByBit: <code>{fmt(bybit_usd, 'USD')}</code>"
        if "bybit" in summary["errors"]:
            bybit_line += f" (ERROR)"
        lines.append(bybit_line)

        okx_line = f"OKX: <code>{fmt(okx_usd, 'USD')}</code>"
        if "okx" in summary["errors"]:
            okx_line += f" (ERROR)"
        lines.append(okx_line)

        lines.append(f"Total crypto: <code>{fmt(crypto_usd, 'USD')}</code>")
        lines.append("")

        # IBKR Section
        if Config.IBKR_FLEX_TOKEN:
            lines.append("<b>STOCKS USD</b>")
            ibkr_line = f"IBKR: <code>{fmt(ibkr_usd, 'USD')}</code>"
            if "ibkr" in summary["errors"]:
                ibkr_line += f" (ERROR: {summary['errors']['ibkr']})"
            lines.append(ibkr_line)
            lines.append("")
        
        lines.append(f"<b>TOTAL</b>")
        lines.append(f"USD: <code>{fmt(grand_total_usd, 'USD')}</code>")
        lines.append(f"RUB: <code>{fmt(grand_total_rub, 'RUB')}</code>")

        return "\n".join(lines)
