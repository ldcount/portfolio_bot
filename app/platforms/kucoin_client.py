import base64
import hashlib
import hmac
import logging
import time
from urllib.parse import urlencode

import requests

from app.config import Config

logger = logging.getLogger(__name__)


class KucoinClient:
    """Read and value all classic and futures wallets on the master account."""

    STABLECOINS_1_TO_1_USD = {
        "USD",
        "USDT",
        "USDC",
        "USDE",
        "USDD",
        "FDUSD",
        "PYUSD",
        "TUSD",
    }
    # Used if the public contracts endpoint cannot be reached. KuCoin uses XBT
    # as the settlement currency code for its coin-margined BTC futures wallet.
    FUTURES_FALLBACK_CURRENCIES = {"USDT", "USDC", "XBT"}

    def __init__(self):
        self.api_key = Config.KUCOIN_API_KEY
        self.api_secret = Config.KUCOIN_API_SECRET
        self.passphrase = Config.KUCOIN_API_PASSPHRASE
        self.base_url = "https://api.kucoin.com"
        self.futures_base_url = "https://api-futures.kucoin.com"

    def _get_headers(self, method: str, endpoint: str, body_str: str = "") -> dict:
        """Generate KuCoin v2 authentication headers for the exact request path."""
        if not self.api_key or not self.api_secret or not self.passphrase:
            raise ValueError("KuCoin credentials missing or incomplete")

        now = int(time.time() * 1000)
        str_to_sign = str(now) + method + endpoint + body_str
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode("utf-8"),
                str_to_sign.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")
        signed_passphrase = base64.b64encode(
            hmac.new(
                self.api_secret.encode("utf-8"),
                self.passphrase.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        return {
            "KC-API-KEY": self.api_key,
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": str(now),
            "KC-API-PASSPHRASE": signed_passphrase,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json",
        }

    def _get_json(
        self,
        base_url: str,
        path: str,
        params: dict | None = None,
        *,
        private: bool = False,
    ):
        """GET a KuCoin payload, signing the path including its query string."""
        query = urlencode(params or {})
        endpoint = f"{path}?{query}" if query else path
        headers = self._get_headers("GET", endpoint) if private else None

        try:
            response = requests.get(
                f"{base_url}{endpoint}", headers=headers, timeout=10
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"KuCoin request failed for {path}") from exc

        if response.status_code != 200:
            raise RuntimeError(
                f"KuCoin request failed for {path} with HTTP {response.status_code}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"KuCoin returned invalid JSON for {path}") from exc

        code = payload.get("code")
        if code != "200000":
            message = payload.get("msg") or "unknown API error"
            raise RuntimeError(f"KuCoin API error {code} for {path}: {message}")
        return payload.get("data")

    def _get_prices_usd(self) -> dict:
        prices = self._get_json(self.base_url, "/api/v1/prices")
        if not isinstance(prices, dict) or not prices:
            raise RuntimeError("KuCoin returned no USD prices")
        return prices

    @classmethod
    def _usd_price(cls, currency: str, prices: dict) -> float | None:
        currency = currency.upper()
        price_currency = "BTC" if currency == "XBT" else currency
        raw_price = prices.get(price_currency) or prices.get(currency)
        if raw_price not in (None, ""):
            try:
                return float(raw_price)
            except (TypeError, ValueError):
                return None
        if currency in cls.STABLECOINS_1_TO_1_USD:
            return 1.0
        return None

    def _get_futures_settlement_currencies(self) -> tuple[set[str], set[str]]:
        """Return currencies to query and those discovered from live contracts."""
        discovered: set[str] = set()
        try:
            contracts = self._get_json(
                self.futures_base_url, "/api/v1/contracts/active"
            )
            if isinstance(contracts, list):
                for contract in contracts:
                    currency = (
                        contract.get("settleCurrency")
                        or contract.get("settlementCurrency")
                    )
                    if currency:
                        discovered.add(str(currency).upper())
        except Exception as exc:
            logger.warning(
                "Could not discover KuCoin futures settlement currencies; "
                "using known defaults: %s",
                type(exc).__name__,
            )

        return discovered | self.FUTURES_FALLBACK_CURRENCIES, discovered

    def get_balance_usd(self) -> float:
        """
        Fetch total KuCoin USD value across funding/main, trading, margin, and
        every active futures settlement wallet on the master account.

        A missing price or an unreadable account section fails the valuation
        rather than silently reporting the affected holdings as zero.
        """
        if not self.api_key or not self.api_secret or not self.passphrase:
            raise RuntimeError("KuCoin credentials missing or incomplete")

        prices = self._get_prices_usd()
        total_usd = 0.0
        missing_prices: set[str] = set()

        accounts = self._get_json(
            self.base_url, "/api/v1/accounts", private=True
        )
        if not isinstance(accounts, list):
            raise RuntimeError("KuCoin account list has an unexpected format")

        # /api/v1/accounts returns all classic main/funding, trade, margin,
        # isolated-margin and legacy high-frequency spot account wallets.
        for account in accounts:
            currency = str(account.get("currency") or "").upper()
            try:
                balance = float(account.get("balance") or 0.0)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"KuCoin returned an invalid {currency or 'unknown'} balance"
                ) from exc
            if not currency or balance == 0.0:
                continue

            price = self._usd_price(currency, prices)
            if price is None:
                missing_prices.add(currency)
                continue
            total_usd += balance * price

        futures_currencies, discovered_currencies = (
            self._get_futures_settlement_currencies()
        )
        successful_futures_queries = 0
        required_futures_failures: list[str] = []

        for requested_currency in sorted(futures_currencies):
            try:
                futures_data = self._get_json(
                    self.futures_base_url,
                    "/api/v1/account-overview",
                    {"currency": requested_currency},
                    private=True,
                )
            except RuntimeError as exc:
                if requested_currency in discovered_currencies:
                    required_futures_failures.append(requested_currency)
                else:
                    logger.info(
                        "KuCoin futures wallet %s is not available: %s",
                        requested_currency,
                        exc,
                    )
                continue

            successful_futures_queries += 1
            if not isinstance(futures_data, dict):
                required_futures_failures.append(requested_currency)
                continue

            equity_currency = str(
                futures_data.get("currency") or requested_currency
            ).upper()
            try:
                equity = float(futures_data.get("accountEquity") or 0.0)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"KuCoin returned an invalid {equity_currency} futures equity"
                ) from exc

            if equity == 0.0:
                continue
            price = self._usd_price(equity_currency, prices)
            if price is None:
                missing_prices.add(equity_currency)
                continue
            total_usd += equity * price

        if not successful_futures_queries:
            raise RuntimeError(
                "KuCoin futures accounts could not be read; check API Futures permission"
            )
        if required_futures_failures:
            currencies = ", ".join(sorted(set(required_futures_failures)))
            raise RuntimeError(
                f"KuCoin futures valuation incomplete for: {currencies}"
            )
        if missing_prices:
            currencies = ", ".join(sorted(missing_prices))
            raise RuntimeError(f"KuCoin USD prices missing for: {currencies}")

        return total_usd
