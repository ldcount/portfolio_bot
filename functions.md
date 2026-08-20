# Functions Documentation

## App

### `main.py`
- `main()`: Entry point. Validates config and starts the `TelegramBot` polling loop.

### `config.py`
- `Config.validate()`: Checks if essential environment variables are set. Raises `ValueError` if missing.
- `Config.get_timezone_obj()`: Returns a `pytz.timezone` object based on the `TIMEZONE` env var.

### `aggregator.py`
- `Aggregator.get_portfolio_summary()`: Fetches balances from all configured platforms. Returns a dictionary with individual and total values in USD, plus an error dictionary.
- `Aggregator.format_message(summary)`: Formats a portfolio snapshot, marking failed sources unavailable and labeling totals as partial when necessary.
- `Aggregator.get_totals(summary)`: Returns the portfolio totals in USD and RUB using the same conversion logic as the message formatter.

### `history_manager.py`
- `save_snapshot(usd, rub)`: Saves or replaces the current day's complete portfolio snapshot.
- `get_history(days)`: Returns the newest saved daily snapshots, sorted newest-first.
- `get_performance_metrics(current_usd, current_rub)`: Calculates 1-day and 7-day changes when recent comparison snapshots are available.

### `snapshot_database.py`
- `initialize_database()`: Creates the SQLite snapshot table and its one-row-per-local-date constraint.
- `insert_snapshot(snapshot)`: Inserts a snapshot without replacing an existing row for that date.
- `has_snapshot_for_date(snapshot_date)`: Checks whether a daily row already exists.
- `get_all_snapshots()`: Returns every SQLite snapshot in chronological order.

### `daily_snapshot.py`
- `build_snapshot(summary, fx_rates, snapshot_at)`: Applies nullable-source rules and calculates RUB, USD, and EUR totals.
- `build_csv(rows)`: Produces an in-memory UTF-8 semicolon CSV with decimal commas.

### `settings_manager.py`
- `load_settings()`: Loads the persisted daily report time and enabled state, migrating legacy interval settings to 20:30.
- `save_schedule(report_time, enabled)`: Atomically persists the automatic-report time and enabled state.

### `chart.py`
- `build_portfolio_chart(entries, currency, line_color)`: Builds a mobile-friendly trend PNG with all data points and focused annotations.
- `build_pie_chart(summary, grouping)`: Builds a donut allocation PNG grouped by platform or asset class.

### `telegram_client.py`
- `TelegramBot.__init__()`: Initializes the application, command handlers, persisted schedule, inline navigation, and scheduled job.
- `TelegramBot.status_command(update, context)`: Fetches balances and updates a progress message with the portfolio card.
- `TelegramBot.performance_command(update, context)`: Opens the chart-first 30-day history view with currency and daily-value controls.
- `TelegramBot.pie_chart_command(update, context)`: Opens allocation by platform with an asset-class toggle.
- `TelegramBot.settings_command(update, context)`: Opens persistent daily-report time and pause/resume controls.
- `TelegramBot.export_command(update, context)`: Sends all detailed daily snapshots as a CSV attachment.
- `TelegramBot.handle_callback(update, context)`: Handles refreshes and single-message navigation between portfolio views.
- `TelegramBot.database_snapshot_job(context)`: Silently stores the independent daily SQLite snapshot.
- `TelegramBot.scheduled_job(context)`: Fetches data and sends the configured daily report to the authorized chat.
- `TelegramBot.run()`: Starts the bot polling loop using `run_polling()`.

## Platforms

### `bybit_client.py`
- `BybitClient.get_balance_usd()`: Connects to ByBit via `pybit`. Fetches the Unified Trading Account wallet balance and returns the total equity in USD.

### `okx_client.py`
- `OkxClient.get_balance_usd()`: Connects to OKX via `okx-sdk`. Fetches the total asset valuation (`totalBal`) in USD across trading, funding, and Earn.

### `kucoin_client.py`
- `KucoinClient.__init__()`: Initializes the KuCoin client with credentials.
- `KucoinClient._get_headers(method, endpoint, body_str)`: Generates signed authentication headers required by the KuCoin API (HMAC-SHA256 signature and encrypted passphrase).
- `KucoinClient.get_balance_usd()`: Queries classic balances (funding/main, trading, and margin), discovers futures settlement currencies, fetches each futures wallet with a correctly signed currency query, and converts the combined equity to USD.

### `cbr_fx_client.py`
- `CBRFXClient.get_rates(snapshot_date)`: Fetches official Bank of Russia RUB/USD and RUB/EUR daily rates and derives USD/EUR.
