import hmac
import hashlib
import base64
import time
import requests
import logging
from app.config import Config

logger = logging.getLogger(__name__)


class KucoinClient:
    def __init__(self):
        self.api_key = Config.KUCOIN_API_KEY
        self.api_secret = Config.KUCOIN_API_SECRET
        self.passphrase = Config.KUCOIN_API_PASSPHRASE
        self.base_url = "https://api.kucoin.com"
        self.futures_base_url = "https://api-futures.kucoin.com"

    def _get_headers(self, method: str, endpoint: str, body_str: str = "") -> dict:
        """
        Generates authentication headers required by the KuCoin API.
        """
        if not self.api_key or not self.api_secret or not self.passphrase:
            raise ValueError("KuCoin credentials missing or incomplete")

        now = int(time.time() * 1000)
        str_to_sign = str(now) + method + endpoint + body_str

        # Generate signature
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode("utf-8"),
                str_to_sign.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        # Generate signed passphrase
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

    def get_balance_usd(self) -> float:
        """
        Fetch the total KuCoin balance in USD across classic (main/trade/margin)
        and futures accounts, converting non-USD currencies using current ticker prices.
        """
        if not self.api_key or not self.api_secret or not self.passphrase:
            logger.error("KuCoin credentials missing or incomplete.")
            raise RuntimeError("KuCoin credentials missing or incomplete")

        # 1. Fetch current token prices in USD
        prices = {}
        try:
            r = requests.get(f"{self.base_url}/api/v1/prices", timeout=10)
            if r.status_code == 200:
                prices = r.json().get("data", {})
            else:
                logger.warning(
                    f"Failed to fetch KuCoin prices: {r.status_code} {r.text}"
                )
        except Exception as e:
            logger.warning(f"Error fetching KuCoin prices: {e}")

        total_usd = 0.0

        # 2. Fetch classic accounts balances (spot / funding / margin)
        try:
            endpoint = "/api/v1/accounts"
            headers = self._get_headers("GET", endpoint)
            r = requests.get(
                f"{self.base_url}{endpoint}", headers=headers, timeout=10
            )
            if r.status_code != 200:
                msg = f"KuCoin API Error (Classic): {r.status_code} {r.text}"
                logger.error(msg)
                raise RuntimeError(msg)

            res_data = r.json()
            code = res_data.get("code")
            if code != "200000":
                msg = f"KuCoin API Error (Classic): {res_data.get('msg')} (code {code})"
                logger.error(msg)
                raise RuntimeError(msg)

            accounts = res_data.get("data", [])
            for acc in accounts:
                currency = acc.get("currency", "").upper()
                balance = float(acc.get("balance", 0.0))
                if balance <= 0.0:
                    continue

                # Valuation logic
                if currency in (
                    "USD",
                    "USDT",
                    "USDC",
                    "USDE",
                    "USDD",
                    "FDUSD",
                    "PYUSD",
                    "TUSD",
                ):
                    price = 1.0
                else:
                    price_str = prices.get(currency)
                    price = float(price_str) if price_str else 0.0
                    if not price:
                        logger.warning(
                            f"KuCoin: price for {currency} not found or zero. Balance {balance} ignored."
                        )

                total_usd += balance * price
        except Exception as e:
            logger.error(f"Error fetching KuCoin accounts: {e}")
            raise

        # 3. Fetch futures balance (optional/graceful)
        try:
            endpoint = "/api/v1/account-overview"
            headers = self._get_headers("GET", endpoint)
            r = requests.get(
                f"{self.futures_base_url}{endpoint}", headers=headers, timeout=10
            )
            if r.status_code == 200:
                res_data = r.json()
                if res_data.get("code") == "200000":
                    fdata = res_data.get("data", {})
                    f_equity = float(fdata.get("accountEquity", 0.0))
                    f_currency = fdata.get("currency", "").upper()
                    if f_equity > 0.0:
                        if f_currency in (
                            "USD",
                            "USDT",
                            "USDC",
                            "USDE",
                            "USDD",
                            "FDUSD",
                            "PYUSD",
                            "TUSD",
                        ):
                            f_price = 1.0
                        else:
                            f_price_str = prices.get(f_currency)
                            f_price = float(f_price_str) if f_price_str else 0.0
                        total_usd += f_equity * f_price
            else:
                logger.info(
                    f"KuCoin Futures account not retrieved or not active (Status: {r.status_code})"
                )
        except Exception as e:
            logger.warning(f"KuCoin Futures fetch skipped/failed: {e}")

        return total_usd
