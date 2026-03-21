import asyncio
import logging
import os
from datetime import datetime, timedelta

from telegram import InputFile, Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import NetworkError, TimedOut
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler

from app.config import Config
from app.aggregator import Aggregator
from app import history_manager
from app import chart as chart_module

logger = logging.getLogger(__name__)

# Absolute path to the history JSON — used by /export
_HISTORY_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "portfolio_history.json",
)


class TelegramBot:
    def __init__(self):
        self.token = Config.TELEGRAM_BOT_TOKEN
        self.chat_id = Config.TELEGRAM_CHAT_ID
        if not self.token:
            logger.warning("Telegram token not set.")
            return

        self.application = Application.builder().token(self.token).build()
        self.aggregator = Aggregator()

        # Current poll interval (minutes) — can be changed at runtime via /frequency
        self.poll_interval_minutes = Config.POLL_INTERVAL_MINUTES

        # Add command handlers
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(
            CommandHandler("frequency", self.frequency_command)
        )
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("history", self.history_command))
        self.application.add_handler(
            CommandHandler("rub_chart", self.rub_chart_command)
        )
        self.application.add_handler(
            CommandHandler("pie_chart", self.pie_chart_command)
        )
        self.application.add_handler(CommandHandler("export", self.export_command))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
        self.application.add_error_handler(self.error_handler)

        # Add scheduled job
        if self.application.job_queue:
            self._schedule_job()
        else:
            logger.warning("JobQueue not available.")

    # ------------------------------------------------------------------
    # Scheduling helpers
    # ------------------------------------------------------------------

    def _seconds_until_next_slot(self) -> float:
        """
        Compute seconds until the next 8AM-anchored slot.

        Slots are:  08:00, 08:00 + interval, 08:00 + 2*interval, …
        If the current time is before 08:00 today, the first slot IS 08:00.
        If no slot remains within today, the next slot is 08:00 tomorrow.
        """
        tz = Config.get_timezone_obj()
        now = datetime.now(tz)
        anchor = now.replace(
            hour=Config.WINDOW_START_HOUR, minute=0, second=0, microsecond=0
        )
        interval_sec = self.poll_interval_minutes * 60

        if now < anchor:
            # Before the anchor today — first slot is the anchor itself
            delay = (anchor - now).total_seconds()
        else:
            elapsed = (now - anchor).total_seconds()
            slots_passed = int(elapsed // interval_sec)
            next_slot = anchor + timedelta(seconds=(slots_passed + 1) * interval_sec)
            delay = (next_slot - now).total_seconds()

        return max(delay, 1.0)  # never zero to avoid immediate double-fire

    def _schedule_job(self):
        """Schedule (or reschedule) the repeating portfolio snapshot job."""
        interval_sec = self.poll_interval_minutes * 60
        first_sec = self._seconds_until_next_slot()

        # Remove any existing jobs with our name to avoid duplicates
        current_jobs = self.application.job_queue.get_jobs_by_name("portfolio_snapshot")
        for job in current_jobs:
            job.schedule_removal()

        self.application.job_queue.run_repeating(
            self.scheduled_job,
            interval=interval_sec,
            first=first_sec,
            chat_id=self.chat_id,
            name="portfolio_snapshot",
        )
        next_dt = datetime.now(Config.get_timezone_obj()) + timedelta(seconds=first_sec)
        logger.info(
            f"Scheduled job every {self.poll_interval_minutes} min. "
            f"Next fire at {next_dt.strftime('%H:%M')} "
            f"({Config.WINDOW_START_HOUR}:00–{Config.WINDOW_END_HOUR}:00 window)"
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

    # ------------------------------------------------------------------
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start — onboarding experience."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        msg = (
            "👋 <b>Welcome to your Portfolio Tracker!</b>\n\n"
            "I monitor your balances across various platforms (Crypto, T-Bank, IBKR) "
            "and provide regular summaries.\n\n"
            "What would you like to see?"
        )
        await update.message.reply_text(
            msg, parse_mode="HTML", reply_markup=self._get_status_keyboard()
        )

    def _get_status_keyboard(self) -> InlineKeyboardMarkup:
        """Helper to return the standard inline keyboard for the status message."""
        keyboard = [
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="refresh_status"),
            ],
            [
                InlineKeyboardButton("📈 30-Day Trend", callback_data="show_history"),
                InlineKeyboardButton("🥧 Allocation", callback_data="show_pie_chart"),
            ],
        ]
        return InlineKeyboardMarkup(keyboard)

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status — fetch and send current portfolio snapshot."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        logger.info(f"/status from {update.effective_chat.id}")

        # Send placeholder — retry on transient network errors so the command
        # doesn't silently vanish if the connection blips at this exact moment.
        status_msg = None
        for attempt in range(4):
            try:
                status_msg = await update.message.reply_text("Fetching data...")
                break
            except (NetworkError, TimedOut) as e:
                if attempt == 3:
                    logger.warning(
                        f"/status: could not send placeholder after 4 attempts: {e}"
                    )
                    return
                await asyncio.sleep(2**attempt)  # 1 s, then 2 s

        try:
            summary = self.aggregator.get_portfolio_summary()
            msg = self.aggregator.format_message(summary)
            # Add timestamp to show when it was last generated
            now = datetime.now(Config.get_timezone_obj()).strftime("%H:%M:%S")
            msg += f"\n\n<i>Last updated: {now}</i>"

            await status_msg.edit_text(
                text=msg, parse_mode="HTML", reply_markup=self._get_status_keyboard()
            )

            # Save snapshot on manual request
            usd, rub = self.aggregator.get_totals(summary)
            history_manager.save_snapshot(usd, rub)
        except Exception as e:
            logger.error(f"Error in /status: {e}")
            await status_msg.edit_text(f"Error fetching status: {e}")

    async def frequency_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """Handle /frequency <minutes> — update the scheduled scan interval."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        args = context.args
        if not args or len(args) != 1:
            await update.message.reply_text(
                "Usage: /frequency <minutes>\nExample: /frequency 60"
            )
            return

        try:
            minutes = int(args[0])
            if minutes < 1:
                raise ValueError("Must be >= 1")
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid value. Please provide a positive integer.\nExample: /frequency 60"
            )
            return

        self.poll_interval_minutes = minutes
        self._schedule_job()

        tz = Config.get_timezone_obj()
        next_dt = datetime.now(tz) + timedelta(seconds=self._seconds_until_next_slot())
        logger.info(f"/frequency set to {minutes} min by {update.effective_chat.id}")
        await update.message.reply_text(
            f"✅ Frequency updated to every <b>{minutes} minute(s)</b>.\n"
            f"Next snapshot at <b>{next_dt.strftime('%H:%M')}</b> "
            f"(anchored to {Config.WINDOW_START_HOUR:02d}:00).",
            parse_mode="HTML",
        )

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help — list all available commands."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        msg = (
            "📋 <b>Available commands</b>\n\n"
            "/status — fetch the current portfolio snapshot\n"
            "/frequency &lt;minutes&gt; — set how often the bot sends automatic snapshots "
            f"(current: every {self.poll_interval_minutes} min, anchored to "
            f"{Config.WINDOW_START_HOUR:02d}:00)\n"
            "/history — view portfolio values for the last 30 days + trend chart\n"
            "/rub_chart — send the last 30 days trend chart in RUB\n"
            "/pie_chart — send a pie chart of current allocation by platform\n"
            "/export — download raw portfolio history as a JSON file\n"
            "/help — show this help message"
        )
        await update.message.reply_text(msg, parse_mode="HTML")

    async def history_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /history — show portfolio snapshots for the last 30 days."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        await self._send_history(update.message.reply_text, update.message.reply_photo)

    async def _send_history(self, reply_text, reply_photo):
        """Internal logic for sending history, usable by both commands and callbacks."""
        entries = history_manager.get_history(30)
        if not entries:
            await reply_text(
                "No portfolio history recorded yet. "
                "Data is saved automatically on each scheduled snapshot."
            )
            return

        # --- Text summary ---
        lines = ["📅 <b>Portfolio history (last 30 days)</b>\n"]
        for e in entries:
            usd_fmt = f"${e['USD']:,.0f}".replace(",", " ")
            rub_fmt = f"₽{e['RUB']:,.0f}".replace(",", " ")
            lines.append(
                f"<b>{e['date']}</b>  <code>{usd_fmt}</code> => <code>{rub_fmt}</code>"
            )

        await reply_text("\n".join(lines), parse_mode="HTML")

        # --- Trend chart image ---
        try:
            buf = await asyncio.to_thread(chart_module.build_portfolio_chart, entries)
            await reply_photo(
                photo=buf,
                caption="📈 Portfolio USD trend",
            )
        except RuntimeError as e:
            logger.warning(f"Chart skipped (matplotlib unavailable): {e}")
        except (TimedOut, NetworkError) as e:
            logger.warning(
                f"Telegram network error while sending chart (photo may still arrive): {e}"
            )
        except Exception as e:
            logger.error(f"Chart generation failed: {e}")
            await reply_text("⚠️ Could not generate chart.")

    async def rub_chart_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """Handle /rub_chart — send the RUB trend chart for the last 30 days."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        await self._send_rub_chart(
            update.message.reply_text, update.message.reply_photo
        )

    async def _send_rub_chart(self, reply_text, reply_photo):
        """Internal logic for sending the RUB chart, usable by commands and callbacks."""
        entries = history_manager.get_history(30)
        if not entries:
            await reply_text(
                "No portfolio history recorded yet. "
                "Data is saved automatically on each scheduled snapshot."
            )
            return

        try:
            buf = await asyncio.to_thread(
                chart_module.build_portfolio_chart,
                entries,
                "RUB",
                "#D64541",
            )
            await reply_photo(
                photo=buf,
                caption="📈 Portfolio RUB trend",
            )
        except RuntimeError as e:
            logger.warning(f"RUB chart skipped (matplotlib unavailable): {e}")
        except (TimedOut, NetworkError) as e:
            logger.warning(
                f"Telegram network error while sending RUB chart (photo may still arrive): {e}"
            )
        except Exception as e:
            logger.error(f"RUB chart generation failed: {e}")
            await reply_text("⚠️ Could not generate chart.")

    async def pie_chart_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """Handle /pie_chart — send a pie chart showing allocation by platform."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        await self._send_pie_chart(
            update.message.reply_text, update.message.reply_photo
        )

    async def _send_pie_chart(self, reply_text, reply_photo):
        """Internal logic for sending pie chart, usable by both commands and callbacks."""
        await reply_text("Generating pie chart…")
        try:
            summary = self.aggregator.get_portfolio_summary()
            buf = await asyncio.to_thread(chart_module.build_pie_chart, summary)
            await reply_photo(
                photo=buf,
                caption="🥧 Portfolio allocation by platform",
            )
        except RuntimeError as e:
            logger.warning(f"Pie chart skipped (matplotlib unavailable): {e}")
            await reply_text("⚠️ matplotlib is not installed.")
        except ValueError as e:
            await reply_text(f"⚠️ {e}")
        except (TimedOut, NetworkError) as e:
            logger.warning(f"Telegram network error sending pie chart: {e}")
        except Exception as e:
            logger.error(f"Pie chart generation failed: {e}")
            await reply_text("⚠️ Could not generate pie chart.")

    async def export_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /export — send portfolio_history.json as a file attachment."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        if not os.path.exists(_HISTORY_FILE):
            await update.message.reply_text(
                "No history file found yet. It is created after the first scheduled snapshot."
            )
            return

        try:
            with open(_HISTORY_FILE, "rb") as f:
                await update.message.reply_document(
                    document=InputFile(f, filename="portfolio_history.json"),
                    caption="📦 Raw portfolio history (DD-MM-YYYY → USD / RUB)",
                )
        except (TimedOut, NetworkError) as e:
            logger.warning(f"Telegram network error sending export: {e}")
        except Exception as e:
            logger.error(f"Export failed: {e}")
            await update.message.reply_text("⚠️ Could not send history file.")

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle button clicks from inline keyboards."""
        query = update.callback_query

        # We must answer the callback query, even if empty, or the button will show a loading spinner
        if not self._is_authorized(update):
            await query.answer("Unauthorized.", show_alert=True)
            return

        data = query.data

        if data == "refresh_status":
            await query.answer("Refreshing data...")
            try:
                summary = self.aggregator.get_portfolio_summary()
                msg = self.aggregator.format_message(summary)
                now = datetime.now(Config.get_timezone_obj()).strftime("%H:%M:%S")
                msg += f"\n\n<i>Last updated: {now}</i>"

                await query.edit_message_text(
                    text=msg,
                    parse_mode="HTML",
                    reply_markup=self._get_status_keyboard(),
                )

                # Save snapshot on manual refresh
                usd, rub = self.aggregator.get_totals(summary)
                history_manager.save_snapshot(usd, rub)
            except Exception as e:
                logger.error(f"Error refreshing status via callback: {e}")
                # We append the error so they know it failed, but keep the keyboard so they can try again later
                error_msg = f"<b>⚠️ Error refreshing data: {e}</b>"
                # If they quickly click refresh twice and the text is identical, Telegram throws a BadRequest.
                # Adding the exact time prevents identical texts.
                import time

                try:
                    await query.edit_message_text(
                        text=error_msg
                        + f"\n<i>Failed at {time.strftime('%H:%M:%S')}</i>",
                        parse_mode="HTML",
                        reply_markup=self._get_status_keyboard(),
                    )
                except Exception:
                    pass

        elif data == "show_history":
            await query.answer()
            await self._send_history(
                query.message.reply_text, query.message.reply_photo
            )

        elif data == "show_pie_chart":
            await query.answer()
            await self._send_pie_chart(
                query.message.reply_text, query.message.reply_photo
            )

    # ------------------------------------------------------------------
    # Scheduled job
    # ------------------------------------------------------------------

    async def scheduled_job(self, context: ContextTypes.DEFAULT_TYPE):
        """Periodic portfolio report — only fires within the configured time window."""
        now = datetime.now(Config.get_timezone_obj())
        if not (Config.WINDOW_START_HOUR <= now.hour <= Config.WINDOW_END_HOUR):
            logger.info("Outside configured time window. Skipping report.")
            return

        chat_id = context.job.chat_id
        logger.info("Running scheduled report...")
        try:
            summary = self.aggregator.get_portfolio_summary()
            msg = self.aggregator.format_message(summary)
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
            logger.info("Scheduled report sent.")

            # Save today's snapshot (overwrites — last run of day wins)
            usd, rub = self.aggregator.get_totals(summary)
            history_manager.save_snapshot(usd, rub)
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
