import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from datetime import datetime
from app.config import Config
from app.aggregator import Aggregator
from app import history_manager
from app import chart as chart_module

logger = logging.getLogger(__name__)


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
        self.application.add_handler(
            CommandHandler("frequency", self.frequency_command)
        )
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("history", self.history_command))

        # Add scheduled job
        if self.application.job_queue:
            self._schedule_job()
        else:
            logger.warning("JobQueue not available.")

    # ------------------------------------------------------------------
    # Scheduling helpers
    # ------------------------------------------------------------------

    def _schedule_job(self):
        """Schedule (or reschedule) the repeating portfolio snapshot job."""
        interval_sec = self.poll_interval_minutes * 60
        # Remove any existing jobs with our name to avoid duplicates
        current_jobs = self.application.job_queue.get_jobs_by_name("portfolio_snapshot")
        for job in current_jobs:
            job.schedule_removal()

        self.application.job_queue.run_repeating(
            self.scheduled_job,
            interval=interval_sec,
            first=10,
            chat_id=self.chat_id,
            name="portfolio_snapshot",
        )
        logger.info(
            f"Scheduled job every {self.poll_interval_minutes} min "
            f"({Config.WINDOW_START_HOUR}:00–{Config.WINDOW_END_HOUR}:00)"
        )

    # ------------------------------------------------------------------
    # Auth helper
    # ------------------------------------------------------------------

    def _is_authorized(self, update: Update) -> bool:
        return str(update.effective_chat.id) == str(self.chat_id)

    # ------------------------------------------------------------------
    # Command handlers
    # ------------------------------------------------------------------

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status — fetch and send current portfolio snapshot."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        logger.info(f"/status from {update.effective_chat.id}")
        await update.message.reply_text("Fetching data...")
        try:
            summary = self.aggregator.get_portfolio_summary()
            msg = self.aggregator.format_message(summary)
            await update.message.reply_text(msg, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error in /status: {e}")
            await update.message.reply_text(f"Error fetching status: {e}")

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

        logger.info(f"/frequency set to {minutes} min by {update.effective_chat.id}")
        await update.message.reply_text(
            f"✅ Portfolio scan frequency updated to every <b>{minutes} minute(s)</b>.",
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
            f"(current: every {self.poll_interval_minutes} min)\n"
            "/history — view portfolio values for the last 30 days\n"
            "/help — show this help message"
        )
        await update.message.reply_text(msg, parse_mode="HTML")

    async def history_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /history — show portfolio snapshots for the last 30 days."""
        if not self._is_authorized(update):
            await update.message.reply_text("Unauthorized access.")
            return

        entries = history_manager.get_history(30)
        if not entries:
            await update.message.reply_text(
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
                f"<b>{e['date']}</b>  USD: <code>{usd_fmt}</code>  RUB: <code>{rub_fmt}</code>"
            )

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

        # --- Chart image ---
        try:
            buf = chart_module.build_portfolio_chart(entries)
            await update.message.reply_photo(
                photo=buf,
                caption="📈 Portfolio USD trend",
            )
        except RuntimeError as e:
            # matplotlib not installed — skip chart silently
            logger.warning(f"Chart skipped (matplotlib unavailable): {e}")
        except Exception as e:
            logger.error(f"Chart generation failed: {e}")
            await update.message.reply_text("⚠️ Could not generate chart.")

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
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)
