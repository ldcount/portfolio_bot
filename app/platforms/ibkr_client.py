import logging
import json
import os
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from requests.exceptions import ConnectionError, Timeout
from app.config import Config

logger = logging.getLogger(__name__)


class IBKRClient:
    def __init__(self):
        self.token = Config.IBKR_FLEX_TOKEN
        self.query_id = Config.IBKR_QUERY_ID
        self.base_url = "https://www.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest"
        self.download_url = "https://www.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement"
        self.cache_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "data",
            "ibkr_cache.json",
        )

        if not self.token or not self.query_id:
            logger.warning("IBKR Flex credentials not set.")

    def get_portfolio_summary(self) -> dict:
        """
        Fetches the portfolio summary via Flex Query.
        Retries up to 3 times on transient network/DNS errors.
        Returns:
            {"total_usd": float, "error": str|None}
        """
        if not self.token or not self.query_id:
            return {"total_usd": 0.0}

        cached = self._load_cache()
        if cached and not self._should_refresh_cache(cached):
            logger.info(
                "Using cached IBKR Flex result from %s (report date: %s)",
                cached.get("fetched_at", "?"),
                cached.get("report_date", "?"),
            )
            return {
                "total_usd": cached.get("total_usd", 0.0),
                "report_date": cached.get("report_date"),
            }

        last_error: Exception | None = None
        for attempt in range(3):
            try:
                result = self._fetch_report()
                if "error" not in result:
                    self._save_cache(result)
                return result
            except (ConnectionError, Timeout, OSError) as e:
                last_error = e
                if attempt < 2:
                    wait = 2 ** (attempt + 1)  # 2 s, then 4 s
                    logger.warning(
                        f"IBKR network error (attempt {attempt + 1}/3), retrying in {wait}s: {e}"
                    )
                    time.sleep(wait)
            except Exception as e:
                # Non-retryable error (e.g. bad XML, HTTP 4xx) — fail immediately
                logger.error(f"IBKR Flex Query Error: {e}")
                return {"total_usd": 0.0, "error": str(e)}

        logger.error(f"IBKR: all 3 attempts failed: {last_error}")
        if cached:
            logger.warning("Falling back to cached IBKR Flex result after fetch failure.")
            return {
                "total_usd": cached.get("total_usd", 0.0),
                "report_date": cached.get("report_date"),
                "error": f"Using cached IBKR data after fetch failure: {last_error}",
            }
        return {"total_usd": 0.0, "error": str(last_error)}

    def _load_cache(self) -> dict | None:
        if not os.path.exists(self.cache_file):
            return None
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to read IBKR cache: {e}")
            return None

    def _save_cache(self, result: dict) -> None:
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        payload = {
            "total_usd": result.get("total_usd", 0.0),
            "report_date": result.get("report_date"),
            "fetched_at": self._now().isoformat(),
        }
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to write IBKR cache: {e}")

    def _should_refresh_cache(self, cached: dict) -> bool:
        fetched_at = cached.get("fetched_at")
        if not fetched_at:
            return True

        try:
            fetched_dt = datetime.fromisoformat(fetched_at)
        except ValueError:
            return True

        now = self._now()
        refresh_hour = Config.WINDOW_START_HOUR

        if fetched_dt.date() == now.date():
            return False

        if now.hour < refresh_hour:
            return False

        return True

    def _now(self) -> datetime:
        return datetime.now(Config.get_timezone_obj())

    def _fetch_report(self) -> dict:
        """Single attempt to fetch the IBKR Flex report. Raises on network errors."""
        # Step 1: Request the report
        logger.info("Requesting IBKR Flex Report...")
        resp = requests.get(
            self.base_url,
            params={"t": self.token, "q": self.query_id, "v": "3"},
            timeout=10,
        )
        resp.raise_for_status()

        # Parse step 1 XML
        root = ET.fromstring(resp.content)
        status = root.find("Status")

        if status is not None and status.text == "Success":
            ref_code = root.find("ReferenceCode").text
            base_url = root.find("Url").text

            logger.info(f"IBKR Report generated. Reference: {ref_code}. Downloading...")

            # Step 2: Download the report
            dl_resp = requests.get(
                base_url,
                params={"t": self.token, "q": ref_code, "v": "3"},
                timeout=30,
            )
            dl_resp.raise_for_status()

            # Parse step 2 XML (Actual Report)
            return self._parse_report(dl_resp.content)

        else:
            error_code = root.find("ErrorCode")
            error_msg = root.find("ErrorMessage")
            msg = f"IBKR Error {error_code.text if error_code is not None else '?'}: {error_msg.text if error_msg is not None else '?'}"
            logger.error(msg)
            return {"total_usd": 0.0, "error": msg}

    def _parse_report(self, xml_content) -> dict:
        """
        Parses the Flex Query XML response.
        We look for 'NAV' or 'NetLiquidation' in 'AccountInformation' or 'EquitySummaryByReportDateInBase'.
        Expected structure (based on user XML):
        <FlexQueryResponse ...>
            <FlexStatements ...>
                <FlexStatement ...>
                    <EquitySummaryInBase>
                        <EquitySummaryByReportDateInBase total="236979.953968903" reportDate="09/02/2026"/>
                        <EquitySummaryByReportDateInBase total="236373.493968903" reportDate="10/02/2026"/>
                    </EquitySummaryInBase>
                </FlexStatement>
            </FlexStatements>
        </FlexQueryResponse>
        """
        try:
            root = ET.fromstring(xml_content)

            flex_stmt = root.find(".//FlexStatement")
            if flex_stmt is None:
                return {"total_usd": 0.0, "error": "No FlexStatement found"}

            acc_info = flex_stmt.find(".//AccountInformation")
            equity_summary = flex_stmt.find(".//EquitySummaryInBase")

            nav = 0.0
            report_date = None
            found = False

            # Strategy 1: Look for AccountInformation -> NetLiquidation (if present)
            if acc_info is not None:
                for attr in [
                    "netLiquidation",
                    "nav",
                    "totalNetAssetValue",
                    "equityWithLoanValue",
                ]:
                    if attr in acc_info.attrib:
                        nav = float(acc_info.attrib[attr])
                        found = True
                        break

            # Strategy 2: Look for EquitySummaryInBase -> EquitySummaryByReportDateInBase
            if not found and equity_summary is not None:
                # Find all children: EquitySummaryByReportDateInBase
                entries = equity_summary.findall(".//EquitySummaryByReportDateInBase")
                if entries:
                    # Sort by reportDate just in case (format usually DD/MM/YYYY or YYYYMMDD)
                    # Let's try to parse date or just take the last one as they are usually chronological
                    # simple last one strategy is best as it's the latest report generated
                    last_entry = entries[-1]

                    if "total" in last_entry.attrib:
                        nav = float(last_entry.attrib["total"])
                        report_date = last_entry.attrib.get("reportDate")
                        found = True
                    elif "netLiquidation" in last_entry.attrib:
                        nav = float(last_entry.attrib["netLiquidation"])
                        report_date = last_entry.attrib.get("reportDate")
                        found = True

            if not found:
                # Last resort: log all tags to help user debug
                # Collect tags from flex_stmt children
                tags_found = [elem.tag for elem in flex_stmt]
                logger.warning(
                    f"Could not find NAV in IBKR report. Tags in FlexStatement: {tags_found}"
                )
                if equity_summary is not None:
                    # Log attributes of EquitySummaryInBase itself? No, incorrect. Just log entries if any.
                    pass

                return {"total_usd": 0.0, "error": "NAV not found in report"}

            if report_date is None and acc_info is not None:
                report_date = acc_info.attrib.get("fromDate") or acc_info.attrib.get(
                    "date"
                )

            return {"total_usd": nav, "report_date": report_date}

        except Exception as e:
            logger.error(f"Error parsing IBKR XML: {e}")
            return {"total_usd": 0.0, "error": f"Parse Error: {e}"}
