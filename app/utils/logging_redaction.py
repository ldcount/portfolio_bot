import logging
import re
from app.config import Config


class RedactionFilter(logging.Filter):
    def __init__(self, patterns=None):
        super().__init__()
        self.patterns = patterns or []

    def filter(self, record):
        try:
            message = record.getMessage()
        except Exception:
            return True

        for pattern in self.patterns:
            message = re.sub(pattern, "[REDACTED]", message)

        record.msg = message
        record.args = ()
        return True


def setup_logging():
    level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)

    # keys to redact
    secrets = [
        Config.TELEGRAM_BOT_TOKEN,
        Config.BYBIT_API_KEY,
        Config.BYBIT_API_SECRET,
        Config.OKX_API_KEY,
        Config.OKX_API_SECRET,
        Config.OKX_API_PASSPHRASE,
        Config.KUCOIN_API_KEY,
        Config.KUCOIN_API_SECRET,
        Config.KUCOIN_API_PASSPHRASE,
        Config.TBANK_API_TOKEN,
        Config.IBKR_FLEX_TOKEN,
        Config.IBKR_QUERY_ID,
    ]
    # Filter out None values
    secrets = [s for s in secrets if s]

    # Simple heuristic to avoid redacting common words if a secret is suspiciously short or empty
    # But API keys are usually long enough.

    patterns = [re.escape(s) for s in secrets]

    redaction_filter = RedactionFilter(patterns)

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    handler.addFilter(redaction_filter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = [handler]  # Replace existing handlers

    # Suppress chatty libraries if needed
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)

    return root_logger
