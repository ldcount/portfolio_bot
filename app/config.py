import os
import pytz
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Telegram
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    # Schedule
    TIMEZONE = os.getenv("TIMEZONE", "Europe/Paris")
    POLL_INTERVAL_MINUTES = int(os.getenv("POLL_INTERVAL_MINUTES", 120))
    WINDOW_START_HOUR = int(os.getenv("WINDOW_START_HOUR", 8))
    WINDOW_END_HOUR = int(os.getenv("WINDOW_END_HOUR", 20))

    # FX
    FX_PROVIDER = os.getenv("FX_PROVIDER", "ECB")
    FX_TTL_MINUTES = int(os.getenv("FX_TTL_MINUTES", 60))

    # Bybit
    BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
    BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")

    # OKX
    OKX_API_KEY = os.getenv("OKX_API_KEY")
    OKX_API_SECRET = os.getenv("OKX_API_SECRET")
    OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE")

    # KuCoin
    KUCOIN_API_KEY = os.getenv("KUCOIN_API_KEY")
    KUCOIN_API_SECRET = os.getenv("KUCOIN_API_SECRET")
    KUCOIN_API_PASSPHRASE = os.getenv("KUCOIN_API_PASSPHRASE")

    # T-Bank
    TBANK_API_TOKEN = os.getenv("TBANK_API_TOKEN")
    TBANK_API_TARGET = os.getenv(
        "TBANK_API_TARGET", "invest-public-api.tbank.ru"
    )

    # IBKR (Flex Query)
    IBKR_FLEX_TOKEN = os.getenv("IBKR_FLEX_TOKEN")
    IBKR_QUERY_ID = os.getenv("IBKR_QUERY_ID")

    # Behavior
    INCLUDE_CRYPTO_BREAKDOWN = (
        os.getenv("INCLUDE_CRYPTO_BREAKDOWN", "true").lower() == "true"
    )
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def validate(cls):
        missing = []
        if not cls.TELEGRAM_BOT_TOKEN:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not cls.TELEGRAM_CHAT_ID:
            missing.append("TELEGRAM_CHAT_ID")

        if not cls.BYBIT_API_KEY:
            missing.append("BYBIT_API_KEY")
        if not cls.BYBIT_API_SECRET:
            missing.append("BYBIT_API_SECRET")
        if not cls.OKX_API_KEY:
            missing.append("OKX_API_KEY")
        if not cls.OKX_API_SECRET:
            missing.append("OKX_API_SECRET")
        if not cls.OKX_API_PASSPHRASE:
            missing.append("OKX_API_PASSPHRASE")

        if cls.KUCOIN_API_KEY or cls.KUCOIN_API_SECRET or cls.KUCOIN_API_PASSPHRASE:
            if not cls.KUCOIN_API_KEY:
                missing.append("KUCOIN_API_KEY")
            if not cls.KUCOIN_API_SECRET:
                missing.append("KUCOIN_API_SECRET")
            if not cls.KUCOIN_API_PASSPHRASE:
                missing.append("KUCOIN_API_PASSPHRASE")

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

    @classmethod
    def get_timezone_obj(cls):
        return pytz.timezone(cls.TIMEZONE)
