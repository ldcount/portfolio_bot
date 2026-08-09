import logging
import xml.etree.ElementTree as ET
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import requests


logger = logging.getLogger(__name__)


class CBRFXClient:
    """Fetch official daily RUB exchange rates from the Bank of Russia."""

    ENDPOINT = "https://www.cbr.ru/scripts/XML_daily_eng.asp"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "portfolio-bot/1.0",
                "Accept": "application/xml, text/xml;q=0.9, */*;q=0.8",
            }
        )
        self.timeout = (10, 30)

    @staticmethod
    def _parse_decimal(value: str) -> Decimal:
        try:
            return Decimal(value.strip().replace(",", "."))
        except (AttributeError, InvalidOperation) as error:
            raise ValueError("Bank of Russia response contains an invalid rate") from error

    def get_rates(self, snapshot_date: date) -> dict:
        response = self.session.get(
            self.ENDPOINT,
            params={"date_req": snapshot_date.strftime("%d/%m/%Y")},
            timeout=self.timeout,
        )
        response.raise_for_status()

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as error:
            raise ValueError("Bank of Russia response is not valid XML") from error

        effective_text = root.attrib.get("Date")
        if not effective_text:
            raise ValueError("Bank of Russia response is missing its effective date")
        try:
            effective_date = datetime.strptime(effective_text, "%d.%m.%Y").date()
        except ValueError as error:
            raise ValueError("Bank of Russia response has an invalid effective date") from error
        if effective_date > snapshot_date:
            raise ValueError("Bank of Russia returned a future exchange rate")

        rates: dict[str, Decimal] = {}
        for currency in root.findall("Valute"):
            code = currency.findtext("CharCode")
            if code not in {"USD", "EUR"}:
                continue

            unit_rate = currency.findtext("VunitRate")
            if unit_rate:
                rates[code] = self._parse_decimal(unit_rate)
                continue

            value = self._parse_decimal(currency.findtext("Value", ""))
            nominal = self._parse_decimal(currency.findtext("Nominal", ""))
            if nominal <= 0:
                raise ValueError("Bank of Russia response contains an invalid nominal")
            rates[code] = value / nominal

        missing = {"USD", "EUR"} - rates.keys()
        if missing:
            raise ValueError(
                "Bank of Russia response is missing required currencies: "
                + ", ".join(sorted(missing))
            )

        rub_per_usd = rates["USD"]
        rub_per_eur = rates["EUR"]
        if rub_per_usd <= 0 or rub_per_eur <= 0:
            raise ValueError("Bank of Russia returned a non-positive exchange rate")

        result = {
            "effective_date": effective_date.isoformat(),
            "rub_per_usd": round(float(rub_per_usd), 4),
            "rub_per_eur": round(float(rub_per_eur), 4),
            "usd_per_eur": round(float(rub_per_eur / rub_per_usd), 6),
        }
        logger.info(
            "Loaded Bank of Russia exchange rates effective %s",
            result["effective_date"],
        )
        return result
