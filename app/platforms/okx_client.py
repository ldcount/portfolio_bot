import logging
from okx.restapi.Funding import FundingClient
from app.config import Config

logger = logging.getLogger(__name__)


class OkxClient:
    def __init__(self):
        self.api_key = Config.OKX_API_KEY
        self.api_secret = Config.OKX_API_SECRET
        self.passphrase = Config.OKX_API_PASSPHRASE
        # simulation=False for live, True for testnet.
        # For now assuming live as per PRD "default".
        self.simulation = False

        self.client = None

        if self.api_key and self.api_secret and self.passphrase:
            try:
                self.client = FundingClient(
                    apikey=self.api_key,
                    apisecret=self.api_secret,
                    passphrase=self.passphrase,
                    simulation=self.simulation,
                )
            except Exception as e:
                logger.error(f"Failed to initialize OKX client: {e}")
        else:
            logger.warning("OKX API credentials not found.")

    def get_balance_usd(self) -> float:
        """
        Fetch the total OKX valuation in USD across trading, funding, and Earn.
        """
        if not self.client:
            logger.error("OKX client not initialized.")
            raise RuntimeError("OKX client not initialized")

        try:
            result = self.client.get_asset_valuation(ccy="USD")

            code = result.get("code")
            if code != "0":
                msg = f"OKX API Error: {result.get('msg')} (code {code})"
                logger.error(msg)
                raise RuntimeError(msg)

            data = result.get("data", [])
            if not data:
                logger.warning("OKX: No data found in balance response.")
                return 0.0

            # totalBal includes trading, funding, and Earn account valuations.
            total_equity = data[0].get("totalBal")
            if total_equity in (None, ""):
                raise RuntimeError("OKX asset valuation response missing totalBal")

            return float(total_equity)

        except Exception as e:
            logger.error(f"Error fetching OKX balance: {e}")
            raise
