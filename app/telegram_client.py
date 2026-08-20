import asyncio
import logging
from datetime import datetime, time as datetime_time, timedelta

from telegram import (
    BotCommand,
    BotCommandScopeChat,
    InputFile,
    InputMediaPhoto,
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.error import NetworkError, TimedOut
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler

from app.config import Config
from app.aggregator import Aggregator
from app.daily_snapshot import build_csv, build_snapshot
from app import history_manager
from app import chart as chart_module
from app import settings_manager
from app import snapshot_database
from app.platforms.cbr_fx_client import CBRFXClient

logger = logging.getLogger(__name__)

_DATABASE_JOB_GRACE_SECONDS = 15 * 60
_DATABASE_RECOVERY_DELAY_MINUTES = 15


class TelegramBot:
    def __init__(self):
        self.token = Config.TELEGRAM_BOT_TOKEN
        self.chat_id = Config.TELEGRAM_CHAT_ID
        if not self.token:
            logger.warning("Telegram token not set.")
            return

        self.application = (
            Application.builder().token(self.token).post_init(self._post_init).build()
        )
        self.aggregator = Aggregator()
        self.fx_client = CBRFXClient()
        self._aggregation_lock = asyncio.Lock()
        self._database_snapshot_lock = asyncio.Lock()

        try:
            snapshot_database.initialize_database()
        except Exception as exc:
            logger.error("Could not initialize the portfolio snapshot database: %s", exc)

        persisted_settings = settings_manager.load_settings()
        self.daily_report_time = persisted_settings["daily_report_time"]
        self.scheduled_reports_enabled = persisted_settings[
            "scheduled_reports_enabled"
        ]

        # Add command handlers
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(
            CommandHandler("performance", self.performance_command)
        )
        self.application.add_handler(
            CommandHandler("allocation", self.pie_chart_command)
        )
        self.application.add_handler(CommandHandler("settings", self.settings_command))
        self.application.add_handler(CommandHandler("export", self.export_command))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
        self.application.add_error_handler(self.error_handler)

        # Add scheduled job
        if self.application.job_queue:
            self._schedule_job()
            self._schedule_database_job()
        else:
            logger.warning("JobQueue not available.")

    # ------------------------------------------------------------------
    # Scheduling helpers
    # ------------------------------------------------------------------

    async def _post_init(self, application: Application) -> None:
        """Register Telegram's visible slash-command menu."""
        commands = [
            BotCommand("status", "Show the current portfolio"),
            BotCommand("performance", "Show 30-day performance"),
            BotCommand("allocation", "Show portfolio allocation"),
            BotCommand("settings", "Change automatic reports"),
            BotCommand("export", "Export full portfolio history"),
            BotCommand("help", "Show all commands"),
        ]
        await application.bot.set_my_commands(
            commands,
            scope=BotCommandScopeChat(chat_id=int(self.chat_id)),
        )

    def _schedule_job(self):
        """Schedule (or reschedule) the daily portfolio report."""
        if not self.application.job_queue:
            logger.warning("JobQueue not available; schedule change was saved only.")
            return

        # Remove any existing jobs with our name to avoid duplicates
        current_jobs = self.application.job_queue.get_jobs_by_name("portfolio_snapshot")
        for job in current_jobs:
            job.schedule_removal()

        if not self.scheduled_reports_enabled:
            logger.info("Automatic portfolio reports are disabled.")
            return

        hour, minute = (int(part) for part in self.daily_report_time.split(":"))
        report_time = datetime_time(
            hour=hour,
            minute=minute,
            tzinfo=Config.get_timezone_obj(),
        )
        self.application.job_queue.run_daily(
            self.scheduled_job,
            time=report_time,
            chat_id=self.chat_id,
            name="portfolio_snapshot",
        )
        logger.info(
            "Daily portfolio report scheduled for %s %s.",
            self.daily_report_time,
            Config.TIMEZONE,
        )

    def _set_schedule(self, report_time: str, enabled: bool = True) -> None:
        """Apply and persist an automatic-report setting."""
        settings_manager.save_schedule(report_time, enabled)
        self.daily_report_time = report_time
        self.scheduled_reports_enabled = enabled
        self._schedule_job()

    def _schedule_database_job(self) -> None:
        """Schedule the independent silent database snapshot and startup catch-up."""
        if not self.application.job_queue:
            logger.warning("JobQueue not available; daily database job was not scheduled.")
            return

        job_queue = self.application.job_queue
        for job_name in (
            "daily_database_snapshot",
            "daily_database_snapshot_recovery",
            "daily_database_snapshot_catchup",
        ):
            for job in job_queue.get_jobs_by_name(job_name):
                job.schedule_removal()

        timezone = Config.get_timezone_obj()
        job_options = {
            "misfire_grace_time": _DATABASE_JOB_GRACE_SECONDS,
            "coalesce": True,
            "max_instances": 1,
        }
        snapshot_time = datetime_time(
            hour=Config.DATABASE_SNAPSHOT_HOUR,
            minute=0,
            tzinfo=timezone,
        )
        job_queue.run_daily(
            self.database_snapshot_job,
            time=snapshot_time,
            name="daily_database_snapshot",
            job_kwargs=job_options,
        )
        recovery_time = datetime_time(
            hour=Config.DATABASE_SNAPSHOT_HOUR,
            minute=_DATABASE_RECOVERY_DELAY_MINUTES,
            tzinfo=timezone,
        )
        job_queue.run_daily(
            self.database_snapshot_job,
            time=recovery_time,
            name="daily_database_snapshot_recovery",
            job_kwargs=job_options,
        )

        now = datetime.now(timezone)
        catch_up_due = (
            now.hour >= Config.DATABASE_SNAPSHOT_HOUR
            and not snapshot_database.has_snapshot_for_date(now.date())
        )
        if catch_up_due:
            job_queue.run_once(
                self.database_snapshot_job,
                when=1,
                name="daily_database_snapshot_catchup",
                job_kwargs=job_options,
            )
            logger.info("Scheduled same-day database snapshot catch-up.")

        logger.info(
            "Daily database snapshot scheduled for %02d:00 %s "
            "with a %d-minute recovery check.",
            Config.DATABASE_SNAPSHOT_HOUR,
            Config.TIMEZONE,
            _DATABASE_RECOVERY_DELAY_MINUTES,
        )

    # ------------------------------------------------------------------
    # Global error handler
    # ------------------------------------------------------------------

    async def error_handler(
        self, update: object, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Log errors. Swallow transient network errors silently."""
        err = context.error
        if isinstance(err, (NetworkError, TimedOut)):
            logger.warning(f"Transient Telegram network error (ignored): {err}")
        else:
            logger.error(f"Unhandled exception in update handler: {err}", exc_info=err)

    # ------------------------------------------------------------------
    # Auth helper
    # ------------------------------------------------------------------

    def _is_authorized(self, update: Update) -> bool:
        return str(update.effective_chat.id) == str(self.chat_id)

    async def _get_portfolio_summary(self) -> dict:
        """Run blocking exchange SDKs off the Telegram event loop."""
        async with self._aggregation_lock:
            return await asyncio.to_thread(self.aggregator.get_portfolio_summary)

    def _save_snapshot_if_complete(self, summary: dict) -> bool:
        """Persist history only when every configured platform succeeded."""
        if not summary.get("is_complete", True):
            logger.warning(
                "Skipping portfolio snapshot because one or more platforms failed: %s",
                ", ".join(sorted(summary.get("errors", {}))),
            )
            return False

        usd, rub = self.aggregator.get_totals(summary)
        history_manager.save_snapshot(usd, rub)
        return True

    # ------------------------------------------------------------------
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start — onboarding experience."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        msg = (
            "👋 <b>Welcome to your Portfolio Tracker!</b>\n\n"
            "I monitor your balances across Crypto, T-Bank, and IBKR and provide "
            "regular summaries. Choose a view below."
        )
        await update.message.reply_text(
            msg, parse_mode="HTML", reply_markup=self._get_home_keyboard()
        )

    def _get_home_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📊 Portfolio", callback_data="show_status")],
                [
                    InlineKeyboardButton("📈 Performance", callback_data="show_history"),
                    InlineKeyboardButton("🥧 Allocation", callback_data="show_allocation_platform"),
                ],
                [InlineKeyboardButton("⚙️ Settings", callback_data="show_settings")],
            ]
        )

    def _get_status_keyboard(self, loading: bool = False) -> InlineKeyboardMarkup:
        refresh = InlineKeyboardButton(
            "⏳ Refreshing…" if loading else "🔄 Refresh",
            callback_data="noop" if loading else "refresh_status",
        )
        return InlineKeyboardMarkup(
            [
                [refresh, InlineKeyboardButton("⚙️ Settings", callback_data="show_settings")],
                [
                    InlineKeyboardButton("📈 Performance", callback_data="show_history"),
                    InlineKeyboardButton("🥧 Allocation", callback_data="show_allocation_platform"),
                ],
            ]
        )

    def _get_retry_keyboard(self, callback_data: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🔄 Try again", callback_data=callback_data)],
                [InlineKeyboardButton("🏠 Portfolio", callback_data="show_status")],
            ]
        )

    def _get_history_keyboard(self, currency: str = "USD") -> InlineKeyboardMarkup:
        currency = currency.upper()
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "USD ✓" if currency == "USD" else "USD",
                        callback_data="history_currency_usd",
                    ),
                    InlineKeyboardButton(
                        "RUB ✓" if currency == "RUB" else "RUB",
                        callback_data="history_currency_rub",
                    ),
                ],
                [InlineKeyboardButton("📅 Daily values", callback_data="show_daily_values")],
                [
                    InlineKeyboardButton("🏠 Portfolio", callback_data="show_status"),
                    InlineKeyboardButton("🥧 Allocation", callback_data="show_allocation_platform"),
                ],
            ]
        )

    def _get_allocation_keyboard(self, grouping: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Platform ✓" if grouping == "platform" else "Platform",
                        callback_data="show_allocation_platform",
                    ),
                    InlineKeyboardButton(
                        "Asset class ✓" if grouping == "asset_class" else "Asset class",
                        callback_data="show_allocation_asset",
                    ),
                ],
                [
                    InlineKeyboardButton("🏠 Portfolio", callback_data="show_status"),
                    InlineKeyboardButton("📈 Performance", callback_data="show_history"),
                ],
            ]
        )

    def _get_settings_keyboard(self) -> InlineKeyboardMarkup:
        def label(report_time: str) -> str:
            selected = (
                self.scheduled_reports_enabled
                and self.daily_report_time == report_time
            )
            text = f"Send at {report_time}"
            return f"{text} ✓" if selected else text

        if self.scheduled_reports_enabled:
            toggle = InlineKeyboardButton("Pause", callback_data="schedule_pause")
        else:
            toggle = InlineKeyboardButton(
                f"Resume at {self.daily_report_time}",
                callback_data="schedule_resume",
            )
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(label("12:30"), callback_data="schedule_1230"),
                    InlineKeyboardButton(label("20:30"), callback_data="schedule_2030"),
                ],
                [toggle],
                [InlineKeyboardButton("🏠 Portfolio", callback_data="show_status")],
            ]
        )

    def _format_performance_context(self, summary: dict) -> str:
        if not summary.get("is_complete", True):
            return ""
        total_usd, total_rub = self.aggregator.get_totals(summary)
        metrics = history_manager.get_performance_metrics(total_usd, total_rub)
        if not metrics:
            return ""

        lines = ["", "", "<b>PERFORMANCE</b>"]
        for metric in metrics:
            change = metric["usd_change"]
            arrow = "▲" if change > 0 else "▼" if change < 0 else "•"
            amount = f"${abs(change):,.0f}".replace(",", " ")
            percentage = metric["percent_change"]
            pct = f"{abs(percentage):.1f}%" if percentage is not None else "n/a"
            lines.append(f"{metric['label']}: <code>{arrow} {amount} ({pct})</code>")
        return "\n".join(lines)

    def _format_status_message(self, summary: dict, timestamp: bool = True) -> str:
        message = self.aggregator.format_message(summary)
        message += self._format_performance_context(summary)
        if timestamp:
            now = datetime.now(Config.get_timezone_obj()).strftime("%H:%M:%S")
            message += f"\n\n<i>Last updated: {now}</i>"
        return message

    async def _load_status_into_message(self, message, context) -> dict:
        summary = await self._get_portfolio_summary()
        text = self._format_status_message(summary)
        await message.edit_text(
            text=text,
            parse_mode="HTML",
            reply_markup=self._get_status_keyboard(),
        )
        context.chat_data["last_status_html"] = text
        self._save_snapshot_if_complete(summary)
        return summary

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status — fetch and send current portfolio snapshot."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        logger.info("/status from %s", update.effective_chat.id)
        status_msg = None
        for attempt in range(4):
            try:
                status_msg = await update.message.reply_text("⏳ Fetching balances…")
                break
            except (NetworkError, TimedOut) as exc:
                if attempt == 3:
                    logger.warning("/status placeholder failed after 4 attempts: %s", exc)
                    return
                await asyncio.sleep(2**attempt)

        try:
            await self._load_status_into_message(status_msg, context)
        except Exception as exc:
            logger.error("Error in /status: %s", exc)
            await status_msg.edit_text(
                "⚠️ <b>Could not refresh the portfolio.</b>\nPlease try again.",
                parse_mode="HTML",
                reply_markup=self._get_retry_keyboard("show_status"),
            )

    def _settings_text(self) -> str:
        if self.scheduled_reports_enabled:
            status = "<b>Active</b>"
            schedule = f"Daily at <b>{self.daily_report_time}</b>"
        else:
            status = "<b>Paused</b>"
            schedule = f"Resume time: <b>{self.daily_report_time}</b>"
        return (
            "⚙️ <b>Automatic reports</b>\n\n"
            f"Status: {status}\n"
            f"Schedule: {schedule}\n"
            f"Timezone: <b>{Config.TIMEZONE}</b>\n\n"
            "Choose a report time or pause delivery. This setting is saved "
            "across bot restarts."
        )

    async def settings_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return
        await update.message.reply_text(
            self._settings_text(),
            parse_mode="HTML",
            reply_markup=self._get_settings_keyboard(),
        )

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return
        msg = (
            "📋 <b>Available commands</b>\n\n"
            "/status — fetch and generate the current portfolio report\n"
            "/performance — 30-day performance chart and daily values\n"
            "/allocation — allocation by platform or asset class\n"
            "/settings — configure or pause the daily automatic report\n"
            "/export — download the full portfolio history as CSV\n"
            "/help — show this message"
        )
        await update.message.reply_text(
            msg, parse_mode="HTML", reply_markup=self._get_home_keyboard()
        )

    def _history_caption(self, entries: list[dict], currency: str) -> str:
        currency = currency.upper()
        symbol = "$" if currency == "USD" else "₽"
        newest = entries[0]
        chronological = list(reversed(entries))
        current = float(newest[currency])
        first = float(chronological[0][currency])
        values = [float(entry[currency]) for entry in entries]
        fmt = lambda value: f"{symbol}{value:,.0f}".replace(",", " ")

        latest_date = datetime.strptime(newest["date"], "%d-%m-%Y")
        dated_entries = [
            (datetime.strptime(entry["date"], "%d-%m-%Y"), entry)
            for entry in entries
        ]

        def change_line(label: str, baseline_value: float) -> str:
            change = current - baseline_value
            percentage = change / baseline_value * 100 if baseline_value else 0.0
            arrow = "▲" if change > 0 else "▼" if change < 0 else "•"
            return (
                f"{label}: <code>{arrow} {fmt(abs(change))} "
                f"({abs(percentage):.1f}%)</code>"
            )

        lines = [
            f"📈 <b>Portfolio performance · {currency}</b>",
            "",
            f"Current: <code>{fmt(current)}</code>",
        ]
        for label, days in (("7D", 7), ("30D", 30)):
            target = latest_date - timedelta(days=days)
            candidates = [item for item in dated_entries if item[0] <= target]
            if candidates:
                _, baseline = max(candidates, key=lambda item: item[0])
                lines.append(change_line(label, float(baseline[currency])))

        if len(lines) == 3:
            lines.append(change_line("Period", first))
        lines.extend(
            [
                f"Range: <code>{fmt(min(values))}–{fmt(max(values))}</code>",
                f"{chronological[0]['date']} → {newest['date']}",
            ]
        )
        return "\n".join(lines)

    def _daily_values_text(self, entries: list[dict]) -> str:
        lines = ["📅 <b>Daily portfolio values</b>", ""]
        for entry in entries:
            usd = f"${entry['USD']:,.0f}".replace(",", " ")
            rub = f"₽{entry['RUB']:,.0f}".replace(",", " ")
            lines.append(f"<b>{entry['date']}</b>  <code>{usd}</code> · <code>{rub}</code>")
        return "\n".join(lines)

    async def _replace_query_with_photo(
        self, query, buffer, caption: str, reply_markup: InlineKeyboardMarkup, filename: str
    ):
        if query.message.photo:
            media = InputMediaPhoto(
                # editMessageMedia embeds the file reference inside JSON, so the
                # multipart upload must be addressed via an attach:// URI.
                media=InputFile(buffer, filename=filename, attach=True),
                caption=caption,
                parse_mode="HTML",
            )
            return await query.edit_message_media(media=media, reply_markup=reply_markup)

        chat_id = query.message.chat_id
        try:
            await query.message.delete()
        except Exception as exc:
            logger.debug("Could not delete previous navigation message: %s", exc)
        return await query.get_bot().send_photo(
            chat_id=chat_id,
            photo=InputFile(buffer, filename=filename),
            caption=caption,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )

    async def _replace_query_with_text(
        self, query, text: str, reply_markup: InlineKeyboardMarkup | None = None
    ):
        if not query.message.photo:
            return await query.edit_message_text(
                text=text, parse_mode="HTML", reply_markup=reply_markup
            )
        chat_id = query.message.chat_id
        try:
            await query.message.delete()
        except Exception as exc:
            logger.debug("Could not delete previous navigation message: %s", exc)
        return await query.get_bot().send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )

    async def _set_query_progress(self, query, text: str) -> None:
        if query.message.photo:
            await query.edit_message_caption(caption=text)
        else:
            await query.edit_message_text(text=text)

    async def _send_history_screen(self, message, currency: str = "USD") -> None:
        progress = await message.reply_text("⏳ Building performance chart…")
        entries = history_manager.get_history(30)
        if not entries:
            await progress.edit_text(
                "No portfolio history recorded yet. Data is saved after a complete snapshot."
            )
            return
        try:
            color = "#4A90D9" if currency == "USD" else "#D64541"
            buffer = await asyncio.to_thread(
                chart_module.build_portfolio_chart, entries, currency, color
            )
            caption = self._history_caption(entries, currency)
            await message.reply_photo(
                photo=InputFile(buffer, filename=f"portfolio_{currency.lower()}.png"),
                caption=caption,
                parse_mode="HTML",
                reply_markup=self._get_history_keyboard(currency),
            )
        except (TimedOut, NetworkError) as exc:
            logger.warning(
                "Telegram did not confirm performance chart delivery; "
                "the photo may still arrive: %s",
                exc,
            )
            try:
                await progress.edit_text(
                    "⚠️ Telegram did not confirm the chart upload. "
                    "It may still arrive; retry if it does not."
                )
            except Exception as edit_exc:
                logger.warning(
                    "Could not update performance chart progress message: %s",
                    edit_exc,
                )
            return
        except Exception as exc:
            logger.error("History chart failed: %s", exc)
            try:
                await progress.edit_text(
                    "⚠️ <b>Could not build the performance chart.</b>",
                    parse_mode="HTML",
                    reply_markup=self._get_retry_keyboard("show_history"),
                )
            except Exception as edit_exc:
                logger.warning(
                    "Could not update performance chart failure message: %s",
                    edit_exc,
                )
            return

        try:
            await progress.delete()
        except Exception as exc:
            logger.debug("Could not remove performance chart progress message: %s", exc)

    async def _show_history_callback(self, query, currency: str = "USD") -> None:
        entries = history_manager.get_history(30)
        if not entries:
            await self._replace_query_with_text(
                query,
                "No portfolio history recorded yet. Data is saved after a complete snapshot.",
                self._get_retry_keyboard("show_status"),
            )
            return
        try:
            await self._set_query_progress(query, "⏳ Building performance chart…")
            color = "#4A90D9" if currency == "USD" else "#D64541"
            buffer = await asyncio.to_thread(
                chart_module.build_portfolio_chart, entries, currency, color
            )
            await self._replace_query_with_photo(
                query,
                buffer,
                self._history_caption(entries, currency),
                self._get_history_keyboard(currency),
                f"portfolio_{currency.lower()}.png",
            )
        except Exception as exc:
            logger.error("History chart failed: %s", exc)
            await self._replace_query_with_text(
                query,
                "⚠️ <b>Could not build the performance chart.</b>",
                self._get_retry_keyboard("show_history"),
            )

    async def performance_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return
        await self._send_history_screen(update.message, "USD")

    def _allocation_caption(self, summary: dict, grouping: str) -> str:
        title = "platform" if grouping == "platform" else "asset class"
        caption = f"🥧 <b>Portfolio allocation by {title}</b>"
        if not summary.get("is_complete", True):
            caption += "\n⚠️ Based on partial data; unavailable sources are excluded."
        return caption

    async def _send_allocation_screen(self, message, context, grouping="platform"):
        progress = await message.reply_text("⏳ Building allocation chart…")
        try:
            summary = await self._get_portfolio_summary()
            context.chat_data["allocation_summary"] = summary
            buffer = await asyncio.to_thread(
                chart_module.build_pie_chart, summary, grouping
            )
            await progress.delete()
            await message.reply_photo(
                photo=InputFile(buffer, filename="portfolio_allocation.png"),
                caption=self._allocation_caption(summary, grouping),
                parse_mode="HTML",
                reply_markup=self._get_allocation_keyboard(grouping),
            )
        except Exception as exc:
            logger.error("Allocation chart failed: %s", exc)
            await progress.edit_text(
                "⚠️ <b>Could not build the allocation chart.</b>",
                parse_mode="HTML",
                reply_markup=self._get_retry_keyboard("show_allocation_platform"),
            )

    async def _show_allocation_callback(self, query, context, grouping="platform"):
        try:
            await self._set_query_progress(query, "⏳ Building allocation chart…")
            summary = context.chat_data.get("allocation_summary")
            if summary is None:
                summary = await self._get_portfolio_summary()
                context.chat_data["allocation_summary"] = summary
            buffer = await asyncio.to_thread(
                chart_module.build_pie_chart, summary, grouping
            )
            await self._replace_query_with_photo(
                query,
                buffer,
                self._allocation_caption(summary, grouping),
                self._get_allocation_keyboard(grouping),
                "portfolio_allocation.png",
            )
        except Exception as exc:
            logger.error("Allocation chart failed: %s", exc)
            await self._replace_query_with_text(
                query,
                "⚠️ <b>Could not build the allocation chart.</b>",
                self._get_retry_keyboard("show_allocation_platform"),
            )

    async def pie_chart_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return
        await self._send_allocation_screen(update.message, context)

    async def export_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /export — send all SQLite snapshots as a semicolon CSV."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        try:
            rows = await asyncio.to_thread(snapshot_database.get_all_snapshots)
            if not rows:
                await update.message.reply_text(
                    "No daily database snapshots recorded yet. "
                    f"The first one will be saved at "
                    f"{Config.DATABASE_SNAPSHOT_HOUR:02d}:00."
                )
                return

            csv_buffer = await asyncio.to_thread(build_csv, rows)
            today = datetime.now(Config.get_timezone_obj()).strftime("%Y-%m-%d")
            await update.message.reply_document(
                document=InputFile(
                    csv_buffer,
                    filename=f"portfolio_database_{today}.csv",
                ),
                caption="Daily portfolio database history",
            )
        except (TimedOut, NetworkError) as exc:
            logger.warning("Telegram network error sending CSV export: %s", exc)
        except Exception as exc:
            logger.error("CSV export failed: %s", exc)
            await update.message.reply_text(
                "⚠️ Could not send the portfolio history."
            )

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle button clicks from inline keyboards."""
        query = update.callback_query

        if not self._is_authorized(update):
            await query.answer("Unauthorized.", show_alert=True)
            return

        data = query.data
        await query.answer("Refreshing…" if data == "refresh_status" else None)

        if data == "noop":
            return

        if data == "refresh_status":
            previous_text = context.chat_data.get("last_status_html")
            if previous_text is None and query.message.text:
                previous_text = query.message.text_html
            await query.edit_message_reply_markup(
                reply_markup=self._get_status_keyboard(loading=True)
            )
            try:
                summary = await self._get_portfolio_summary()
                msg = self._format_status_message(summary)
                await query.edit_message_text(
                    text=msg,
                    parse_mode="HTML",
                    reply_markup=self._get_status_keyboard(),
                )
                context.chat_data["last_status_html"] = msg
                context.chat_data.pop("allocation_summary", None)
                self._save_snapshot_if_complete(summary)
            except Exception as exc:
                logger.error("Error refreshing status via callback: %s", exc)
                if previous_text:
                    error_text = (
                        previous_text
                        + "\n\n⚠️ <b>Refresh failed.</b> Previous values are shown."
                    )
                else:
                    error_text = "⚠️ <b>Could not refresh the portfolio.</b>"
                await query.edit_message_text(
                    text=error_text,
                    parse_mode="HTML",
                    reply_markup=self._get_retry_keyboard("refresh_status"),
                )

        elif data == "show_status":
            loading_message = await self._replace_query_with_text(
                query,
                "⏳ Fetching balances…",
                None,
            )
            try:
                await self._load_status_into_message(loading_message, context)
                context.chat_data.pop("allocation_summary", None)
            except Exception as exc:
                logger.error("Could not open portfolio view: %s", exc)
                await loading_message.edit_text(
                    "⚠️ <b>Could not refresh the portfolio.</b>",
                    parse_mode="HTML",
                    reply_markup=self._get_retry_keyboard("show_status"),
                )

        elif data in {"show_history", "history_currency_usd"}:
            await self._show_history_callback(query, "USD")

        elif data == "history_currency_rub":
            await self._show_history_callback(query, "RUB")

        elif data == "show_daily_values":
            entries = history_manager.get_history(30)
            if entries:
                await self._replace_query_with_text(
                    query,
                    self._daily_values_text(entries),
                    InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("📈 Back to chart", callback_data="show_history")],
                            [InlineKeyboardButton("🏠 Portfolio", callback_data="show_status")],
                        ]
                    ),
                )
            else:
                await self._replace_query_with_text(
                    query,
                    "No portfolio history recorded yet.",
                    self._get_retry_keyboard("show_status"),
                )

        elif data in {"show_pie_chart", "show_allocation_platform"}:
            await self._show_allocation_callback(query, context, "platform")

        elif data == "show_allocation_asset":
            await self._show_allocation_callback(query, context, "asset_class")

        elif data == "show_settings":
            await self._replace_query_with_text(
                query, self._settings_text(), self._get_settings_keyboard()
            )

        elif data.startswith("schedule_"):
            if data == "schedule_pause":
                self._set_schedule(self.daily_report_time, False)
            elif data == "schedule_resume":
                self._set_schedule(self.daily_report_time, True)
            elif data == "schedule_1230":
                self._set_schedule("12:30", True)
            elif data == "schedule_2030":
                self._set_schedule("20:30", True)
            else:
                logger.warning("Ignoring unknown schedule callback: %s", data)
                return
            await self._replace_query_with_text(
                query, self._settings_text(), self._get_settings_keyboard()
            )

    # ------------------------------------------------------------------
    # Daily database job
    # ------------------------------------------------------------------

    @staticmethod
    def _failed_database_summary(error_message: str) -> dict:
        errors = {}
        configured_sources = (
            ("bybit", bool(Config.BYBIT_API_KEY)),
            ("okx", bool(Config.OKX_API_KEY)),
            ("kucoin", bool(Config.KUCOIN_API_KEY)),
            ("tbank", bool(Config.TBANK_API_TOKEN)),
            (
                "ibkr",
                bool(Config.IBKR_FLEX_TOKEN and Config.IBKR_QUERY_ID),
            ),
        )
        for source, configured in configured_sources:
            if configured:
                errors[source] = error_message
        return {"errors": errors}

    async def database_snapshot_job(self, context: ContextTypes.DEFAULT_TYPE):
        """Silently save one nullable SQLite snapshot for the current local date."""
        async with self._database_snapshot_lock:
            now = datetime.now(Config.get_timezone_obj())
            already_saved = await asyncio.to_thread(
                snapshot_database.has_snapshot_for_date,
                now.date(),
            )
            if already_saved:
                logger.info("Database snapshot already exists for %s; skipping.", now.date())
                return

            try:
                summary = await self._get_portfolio_summary()
            except Exception as exc:
                logger.error("Daily database account scan failed: %s", exc)
                summary = self._failed_database_summary(type(exc).__name__)

            fx_rates = None
            try:
                fx_rates = await asyncio.to_thread(
                    self.fx_client.get_rates,
                    now.date(),
                )
            except Exception as exc:
                logger.error(
                    "Daily database exchange-rate scan failed (%s).",
                    type(exc).__name__,
                )

            snapshot = build_snapshot(summary, fx_rates, now)
            inserted = await asyncio.to_thread(
                snapshot_database.insert_snapshot,
                snapshot,
            )
            if inserted:
                logger.info("Daily database snapshot saved for %s.", now.date())
            else:
                logger.info(
                    "Daily database snapshot for %s was already inserted.",
                    now.date(),
                )

    # ------------------------------------------------------------------
    # Daily Telegram report
    # ------------------------------------------------------------------

    async def scheduled_job(self, context: ContextTypes.DEFAULT_TYPE):
        """Fetch and send the configured daily portfolio report."""
        chat_id = context.job.chat_id
        logger.info("Running daily portfolio report...")
        try:
            summary = await self._get_portfolio_summary()
            msg = self._format_status_message(summary, timestamp=False)
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
            logger.info("Scheduled report sent.")

            # Save today's snapshot (overwrites — last run of day wins)
            self._save_snapshot_if_complete(summary)
        except Exception as e:
            logger.error(f"Error in scheduled job: {e}")

    # ------------------------------------------------------------------
    # Entrypoint
    # ------------------------------------------------------------------

    def run(self):
        """Start the bot."""
        if not self.application:
            logger.error("Application not initialized.")
            return

        logger.info("Starting Telegram Bot polling...")
        self.application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            bootstrap_retries=5,  # retry connecting to Telegram on startup (was: 0 = crash immediately)
        )
