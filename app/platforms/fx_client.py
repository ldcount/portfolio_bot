"""Shared Google Finance rates with official Bank of Russia fallback."""

import logging
import math
from datetime import date, datetime
from html.parser import HTMLParser

import requests

from app.config import Config
from app.platforms.cbr_fx_client import CBRFXClient

logger = logging.getLogger(__name__)


class _QuoteParser(HTMLParser):
    def __init__(self, base: str, quote: str):
        super().__init__()
        self.base = base
        self.quote = quote
        self.price = None
        self._scopes = []
        self._price_depth = 0
        self._price_text = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "c-wiz":
            prefix = f'%.@.[null,null,["{self.base}","{self.quote}"]]'
            self._scopes.append(attributes.get("data-p", "").startswith(prefix))
        if tag == "span":
            if self._price_depth:
                self._price_depth += 1
            elif (
                self.price is None
                and any(self._scopes)
                and attributes.get("jsname") == "Pdsbrc"
            ):
                self._price_depth = 1
        if (
            attributes.get("data-ticker") == self.base
            and attributes.get("data-exchange") == self.quote
            and "data-last-price" in attributes
        ):
            self.price = attributes["data-last-price"]

    def handle_data(self, data):
        if self._price_depth:
            self._price_text.append(data)

    def handle_endtag(self, tag):
        if tag == "c-wiz" and self._scopes:
            self._scopes.pop()
        if tag == "span" and self._price_depth:
            self._price_depth -= 1
            if self._price_depth == 0:
                self.price = "".join(self._price_text).strip().replace(",", "")


class FXClient:
    """Fetch one coherent set of rates per portfolio scan, without fixed defaults.

    Google Finance's public quote HTML is not a supported API. Missing markup,
    consent pages, invalid rates, and network failures trigger the CBR fallback.
    Historical dates use CBR because these Google pages only contain live quotes.
    """

    ENDPOINT = "https://www.google.com/finance/quote/{base}-{quote}"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/130.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        })
        self.cbr = CBRFXClient()

    def _get_google_rate(self, base: str, quote: str) -> float:
        response = self.session.get(
            self.ENDPOINT.format(base=base, quote=quote),
            params={"hl": "en"},
            timeout=(5, 10),
        )
        response.raise_for_status()
        parser = _QuoteParser(base, quote)
        parser.feed(response.text)
        if parser.price is None:
            raise ValueError(f"Google Finance quote missing for {base}/{quote}")
        rate = float(parser.price)
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError(f"Google Finance quote invalid for {base}/{quote}")
        return rate

    def get_rates(self, snapshot_date: date) -> dict:
        # Use the bot's timezone for deciding whether the requested date is live.
        if snapshot_date == datetime.now(Config.get_timezone_obj()).date():
            try:
                rub_per_usd = round(self._get_google_rate("USD", "RUB"), 4)
                rub_per_eur = round(self._get_google_rate("EUR", "RUB"), 4)
                if rub_per_usd <= 0 or rub_per_eur <= 0:
                    raise ValueError("Google Finance quote rounds to zero")
                logger.info("Loaded Google Finance exchange rates for %s", snapshot_date)
                return {
                    "effective_date": snapshot_date.isoformat(),
                    "source": "Google Finance",
                    "rub_per_usd": rub_per_usd,
                    "rub_per_eur": rub_per_eur,
                    "usd_per_eur": round(rub_per_eur / rub_per_usd, 6),
                }
            except (requests.RequestException, ValueError) as exc:
                logger.warning("Google Finance FX unavailable (%s); using CBR.", type(exc).__name__)

        rates = self.cbr.get_rates(snapshot_date)
        rates["source"] = "Bank of Russia"
        return rates
