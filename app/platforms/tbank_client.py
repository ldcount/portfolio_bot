import logging
from typing import Dict, List
from decimal import Decimal
from t_tech.invest import Client, RequestError
from t_tech.invest.services import (
    InstrumentsService,
    MarketDataService,
    OperationsService,
    UsersService,
)
from t_tech.invest.schemas import PortfolioResponse, PositionsResponse
from app.config import Config

logger = logging.getLogger(__name__)


class TBankClient:
    def __init__(self):
        self.token = Config.TBANK_API_TOKEN
        self.client = None

        if not self.token:
            logger.warning("T-Bank API token not set.")
            return

    def _get_usd_rub_rate(self, client: Client) -> float:
        """
        Fetches the current USDRUB rate using the order book or last price.
        We look for the 'USD000UTSTOM' ticker or similar, or just 'USD' currency instrument.
        Common figi for USD/RUB: 'BBG0013HGFT4' (Tinkoff) but better to search by ticker 'USDRUB'
        """
        try:
            # Search for the instrument
            instruments: InstrumentsService = client.instruments
            # "USD000UTSTOM" is the usual ticker for TOM settlement
            # But let's try to find it dynamically or use a known FIGI.
            # BBG0013HGFT4 is widely known for USD/RUB
            usd_rub_figi = "BBG0013HGFT4"

            market_data: MarketDataService = client.market_data
            last_price_response = market_data.get_last_prices(figi=[usd_rub_figi])

            if last_price_response.last_prices:
                price_obj = last_price_response.last_prices[0].price
                # Price is Quotation (units, nano)
                price = price_obj.units + price_obj.nano / 1e9
                return float(price)

            logger.warning("Could not fetch USDRUB rate, defaulting to 90.0")
            return 90.0  # Fallback

        except Exception as e:
            logger.error(f"Error fetching FX rate: {e}")
            return 90.0

    def get_portfolio_summary(self) -> Dict:
        """
        Returns a dictionary with:
        - total_rub: Total portfolio value in RUB
        - total_usd: Total portfolio value in USD (converted)
        - accounts: List of per-account dicts [{"name": str, "rub": float}, ...]
        """
        if not self.token:
            return {"total_rub": 0.0, "total_usd": 0.0, "accounts": []}

        total_rub = 0.0
        accounts_list = []

        try:
            with Client(self.token) as client:
                # 1. Get Accounts
                users: UsersService = client.users
                accounts = users.get_accounts().accounts

                # 2. Get FX Rate
                usd_rub_rate = self._get_usd_rub_rate(client)
                if usd_rub_rate <= 0:
                    usd_rub_rate = 90.0

                # 3. Iterate accounts
                operations: OperationsService = client.operations

                for account in accounts:
                    # Get Portfolio
                    portfolio: PortfolioResponse = operations.get_portfolio(
                        account_id=account.id
                    )

                    account_rub = 0.0
                    if hasattr(portfolio, "total_amount_portfolio"):
                        val = portfolio.total_amount_portfolio
                        amount = val.units + val.nano / 1e9
                        currency = val.currency.upper()

                        if currency == "RUB":
                            account_rub = float(amount)
                        elif currency == "USD":
                            account_rub = float(amount) * usd_rub_rate

                    # Use account name if available, fallback to generic label
                    account_name = (
                        getattr(account, "name", None)
                        or f"Account {len(accounts_list) + 1}"
                    )
                    # Only show accounts with meaningful balances (>= 1000 RUB)
                    if account_rub >= 1000:
                        accounts_list.append(
                            {"name": account_name, "rub": round(account_rub, 2)}
                        )
                    total_rub += account_rub

        except RequestError as e:
            logger.error(f"T-Bank API Request Error: {e}")
            return {"total_rub": 0.0, "total_usd": 0.0, "accounts": [], "error": str(e)}
        except Exception as e:
            logger.error(f"T-Bank Client Error: {e}")
            return {"total_rub": 0.0, "total_usd": 0.0, "accounts": [], "error": str(e)}

        total_usd = total_rub / usd_rub_rate if usd_rub_rate > 0 else 0.0

        return {
            "total_rub": round(total_rub, 2),
            "total_usd": round(total_usd, 2),
            "accounts": accounts_list,
        }
