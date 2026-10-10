import logging
from typing import Dict
from t_tech.invest import AccountType, Client, RequestError
from t_tech.invest.services import (
    OperationsService,
    UsersService,
)
from t_tech.invest.schemas import PortfolioResponse
from app.config import Config

logger = logging.getLogger(__name__)


class TBankClient:
    def __init__(self):
        self.token = Config.TBANK_API_TOKEN
        self.client = None

        if not self.token:
            logger.warning("T-Bank API token not set.")
            return

    def get_portfolio_summary(self, rub_per_usd: float | None = None) -> Dict:
        """
        Returns a dictionary with:
        - total_rub: Total portfolio value in RUB
        - total_usd: Total value converted with rub_per_usd, or None without FX
        - main_rub: Regular brokerage accounts combined in RUB
        - iis_rub: IIS accounts combined in RUB
        - accounts: List of per-account dicts [{"name": str, "rub": float}, ...]
        """
        if not self.token:
            return {
                "total_rub": 0.0,
                "total_usd": 0.0,
                "main_rub": 0.0,
                "iis_rub": 0.0,
                "accounts": [],
            }

        total_rub = 0.0
        main_rub = 0.0
        iis_rub = 0.0
        accounts_list = []

        try:
            with Client(self.token, target=Config.TBANK_API_TARGET) as client:
                # 1. Get Accounts
                users: UsersService = client.users
                accounts = users.get_accounts().accounts

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
                            if rub_per_usd is None or rub_per_usd <= 0:
                                raise ValueError("FX unavailable for USD account valuation")
                            account_rub = float(amount) * rub_per_usd

                    # Use account name if available, fallback to generic label
                    account_name = (
                        getattr(account, "name", None)
                        or f"Account {len(accounts_list) + 1}"
                    )
                    account_type = getattr(account, "type", None)
                    if account_type == AccountType.ACCOUNT_TYPE_TINKOFF:
                        main_rub += account_rub
                    elif account_type == AccountType.ACCOUNT_TYPE_TINKOFF_IIS:
                        iis_rub += account_rub

                    # Only show accounts with meaningful balances (>= 1000 RUB)
                    if account_rub >= 1000:
                        accounts_list.append(
                            {"name": account_name, "rub": round(account_rub, 2)}
                        )
                    total_rub += account_rub

        except RequestError as e:
            logger.error(f"T-Bank API Request Error: {e}")
            return {
                "total_rub": 0.0,
                "total_usd": 0.0,
                "main_rub": 0.0,
                "iis_rub": 0.0,
                "accounts": [],
                "error": str(e),
            }
        except Exception as e:
            logger.error(f"T-Bank Client Error: {e}")
            return {
                "total_rub": 0.0,
                "total_usd": 0.0,
                "main_rub": 0.0,
                "iis_rub": 0.0,
                "accounts": [],
                "error": str(e),
            }

        total_usd = (
            total_rub / rub_per_usd
            if rub_per_usd is not None and rub_per_usd > 0
            else None
        )

        return {
            "total_rub": round(total_rub, 2),
            "total_usd": round(total_usd, 2) if total_usd is not None else None,
            "main_rub": round(main_rub, 2),
            "iis_rub": round(iis_rub, 2),
            "accounts": accounts_list,
        }
