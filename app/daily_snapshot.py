import csv
import io
from datetime import datetime

from app.config import Config


CSV_HEADERS = (
    f"Snapshot date/time ({Config.TIMEZONE})",
    "T-Bank Main (RUB)",
    "T-Bank IIS (RUB)",
    "Crypto (USD)",
    "IBKR (USD)",
    "RUB/USD",
    "RUB/EUR",
    "USD/EUR",
    "Total (RUB)",
    "Total (USD)",
    "Total (EUR)",
)

_AMOUNT_COLUMNS = {
    "tbank_main_rub",
    "tbank_iis_rub",
    "crypto_usd",
    "ibkr_usd",
    "total_rub",
    "total_usd",
    "total_eur",
}
_RATE_PRECISION = {
    "rub_per_usd": 4,
    "rub_per_eur": 4,
    "usd_per_eur": 6,
}


def build_snapshot(
    summary: dict,
    fx_rates: dict | None,
    snapshot_at: datetime,
) -> dict:
    """Build a nullable, internally consistent database row."""
    errors = summary.get("errors", {})

    if Config.TBANK_API_TOKEN and "tbank" in errors:
        tbank_main_rub = None
        tbank_iis_rub = None
    else:
        tbank_main_rub = round(float(summary.get("tbank_main_rub", 0.0)), 2)
        tbank_iis_rub = round(float(summary.get("tbank_iis_rub", 0.0)), 2)

    configured_crypto = (
        ("bybit", bool(Config.BYBIT_API_KEY)),
        ("okx", bool(Config.OKX_API_KEY)),
        ("kucoin", bool(Config.KUCOIN_API_KEY)),
    )
    crypto_failed = any(
        configured and platform in errors
        for platform, configured in configured_crypto
    )
    crypto_usd = (
        None
        if crypto_failed
        else round(float(summary.get("crypto_usd", 0.0)), 2)
    )

    ibkr_configured = bool(Config.IBKR_FLEX_TOKEN and Config.IBKR_QUERY_ID)
    ibkr_usd = (
        None
        if ibkr_configured and "ibkr" in errors
        else round(float(summary.get("ibkr_usd", 0.0)), 2)
    )

    rub_per_usd = None
    rub_per_eur = None
    usd_per_eur = None
    if fx_rates:
        rub_per_usd = round(float(fx_rates["rub_per_usd"]), 4)
        rub_per_eur = round(float(fx_rates["rub_per_eur"]), 4)
        usd_per_eur = round(float(fx_rates["usd_per_eur"]), 6)

    total_rub = None
    total_usd = None
    total_eur = None
    required = (
        tbank_main_rub,
        tbank_iis_rub,
        crypto_usd,
        ibkr_usd,
        rub_per_usd,
        rub_per_eur,
        usd_per_eur,
    )
    if all(value is not None for value in required):
        tbank_rub = tbank_main_rub + tbank_iis_rub
        foreign_usd = crypto_usd + ibkr_usd
        total_rub = round(tbank_rub + foreign_usd * rub_per_usd, 2)
        total_usd = round(tbank_rub / rub_per_usd + foreign_usd, 2)
        total_eur = round(
            tbank_rub / rub_per_eur + foreign_usd / usd_per_eur,
            2,
        )

    return {
        "snapshot_at": snapshot_at.isoformat(timespec="seconds"),
        "tbank_main_rub": tbank_main_rub,
        "tbank_iis_rub": tbank_iis_rub,
        "crypto_usd": crypto_usd,
        "ibkr_usd": ibkr_usd,
        "rub_per_usd": rub_per_usd,
        "rub_per_eur": rub_per_eur,
        "usd_per_eur": usd_per_eur,
        "total_rub": total_rub,
        "total_usd": total_usd,
        "total_eur": total_eur,
    }


def _format_decimal(value: float | None, precision: int) -> str:
    if value is None:
        return ""
    return f"{float(value):.{precision}f}".replace(".", ",")


def build_csv(rows: list[dict]) -> io.BytesIO:
    """Build an Excel-friendly UTF-8 semicolon CSV in memory."""
    text_buffer = io.StringIO(newline="")
    writer = csv.writer(text_buffer, delimiter=";", lineterminator="\r\n")
    writer.writerow(CSV_HEADERS)

    timezone = Config.get_timezone_obj()
    ordered_columns = (
        "tbank_main_rub",
        "tbank_iis_rub",
        "crypto_usd",
        "ibkr_usd",
        "rub_per_usd",
        "rub_per_eur",
        "usd_per_eur",
        "total_rub",
        "total_usd",
        "total_eur",
    )

    for row in rows:
        timestamp = datetime.fromisoformat(row["snapshot_at"])
        if timestamp.tzinfo is None:
            timestamp = timezone.localize(timestamp)
        else:
            timestamp = timestamp.astimezone(timezone)

        values = [timestamp.strftime("%d.%m.%Y %H:%M:%S")]
        for column in ordered_columns:
            precision = 2 if column in _AMOUNT_COLUMNS else _RATE_PRECISION[column]
            values.append(_format_decimal(row.get(column), precision))
        writer.writerow(values)

    payload = ("\ufeff" + text_buffer.getvalue()).encode("utf-8")
    output = io.BytesIO(payload)
    output.seek(0)
    return output
