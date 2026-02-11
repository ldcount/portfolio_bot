# Telegram Portfolio Bot (Bybit + OKX + T‑Bank + IBKR Flex)

This project is a Telegram bot that sends your portfolio snapshot on a schedule.

It pulls balances from:
- **Bybit** (USD equity)
- **OKX** (USD equity)
- **T‑Bank / T‑Invest** (RUB total portfolio, converted to USD)
- **IBKR** via **Flex Web Service** (USD total/NAV from a Flex query)

Then it sends one formatted Telegram message with:
- platform balances,
- crypto subtotal,
- grand total in USD,
- grand total in RUB.

---

## 1) How it works (high level)

1. Telegram bot receives `/status` command **or** runs on a schedule (`POLL_INTERVAL_MINUTES`).
2. The app requests balances from each configured platform.
3. Results are aggregated in `app/aggregator.py`.
4. A single HTML-formatted message is sent to your configured Telegram chat.

Main entrypoint: `app/main.py`.

---

## 2) Configuration via `.env`

Create a `.env` file in the project root. All settings are loaded from environment variables.

### Required to run the bot core

- `TELEGRAM_BOT_TOKEN` — from **@BotFather**.
- `TELEGRAM_CHAT_ID` — the chat ID where messages will be sent.
- `BYBIT_API_KEY`
- `BYBIT_API_SECRET`
- `OKX_API_KEY`
- `OKX_API_SECRET`
- `OKX_API_PASSPHRASE`

> Note: current validation enforces Bybit and OKX credentials. T‑Bank and IBKR are optional in code and used only if configured.

### Optional / recommended

- `TBANK_API_TOKEN` — T‑Bank read-only token.
- `IBKR_FLEX_TOKEN` — IBKR Flex Web Service token.
- `IBKR_QUERY_ID` — ID of your saved Flex query.
- `TIMEZONE` (default: `Europe/Paris`)
- `POLL_INTERVAL_MINUTES` (default: `120`)
- `WINDOW_START_HOUR` (default: `8`)
- `WINDOW_END_HOUR` (default: `20`)
- `LOG_LEVEL` (default: `INFO`)

---

## 3) `.env` template

You can copy this directly:

```dotenv
# Telegram
TELEGRAM_BOT_TOKEN=1234567890:replace_with_botfather_token
TELEGRAM_CHAT_ID=123456789

# Schedule
TIMEZONE=Europe/Paris
POLL_INTERVAL_MINUTES=120
WINDOW_START_HOUR=8
WINDOW_END_HOUR=20

# Optional FX settings (currently reserved)
FX_PROVIDER=ECB
FX_TTL_MINUTES=60

# Bybit (required by current validation)
BYBIT_API_KEY=replace_with_bybit_readonly_key
BYBIT_API_SECRET=replace_with_bybit_readonly_secret

# OKX (required by current validation)
OKX_API_KEY=replace_with_okx_readonly_key
OKX_API_SECRET=replace_with_okx_readonly_secret
OKX_API_PASSPHRASE=replace_with_okx_api_passphrase

# T-Bank (optional)
TBANK_API_TOKEN=replace_with_tbank_token

# IBKR Flex (optional)
IBKR_FLEX_TOKEN=replace_with_ibkr_flex_token
IBKR_QUERY_ID=replace_with_ibkr_query_id

# Behavior
INCLUDE_CRYPTO_BREAKDOWN=true
LOG_LEVEL=INFO
```

---

## 4) Where to get each key/token

## 4.1 Telegram
1. Open Telegram and message **@BotFather**.
2. Run `/newbot`, choose name + username.
3. Copy the bot token to `TELEGRAM_BOT_TOKEN`.
4. Add bot to your chat (or message it directly).
5. Get chat ID:
   - easiest: send a message to the bot and call `getUpdates`,
   - or use any known Telegram chat ID bot utility.
6. Put result into `TELEGRAM_CHAT_ID`.

## 4.2 Bybit (read-only)
1. Login to Bybit.
2. Go to **API Management**.
3. Create API key with **Read** permissions only.
4. Save key and secret into `.env`:
   - `BYBIT_API_KEY`
   - `BYBIT_API_SECRET`

## 4.3 OKX (read-only)
1. Login to OKX.
2. Go to **API** page.
3. Create API key with **Read** permission only.
4. Set and save passphrase.
5. Put in `.env`:
   - `OKX_API_KEY`
   - `OKX_API_SECRET`
   - `OKX_API_PASSPHRASE`

## 4.4 T‑Bank (optional)
1. Use T‑Bank Invest Open API cabinet / developer portal.
2. Generate token with read-only portfolio scopes.
3. Put into `TBANK_API_TOKEN`.

## 4.5 IBKR Flex Web Service (optional)
You need **two values**:
- `IBKR_FLEX_TOKEN`
- `IBKR_QUERY_ID`

How to get them is in the next section.

---

## 5) IBKR: how to create a Flex Web Query (step-by-step)

This project uses **IBKR Flex Web Service**, not Client Portal Gateway.

### Step A — open Flex Queries area
1. Login to IBKR Account Management / Client Portal.
2. Navigate to **Performance & Reports → Flex Queries**.

### Step B — create a query template
1. Create a new **Activity Flex Query** (or equivalent account statement query).
2. Include fields that let the bot derive account total value. The parser supports:
   - `AccountInformation` attributes like `netLiquidation` / `nav`, or
   - `EquitySummaryInBase -> EquitySummaryByReportDateInBase total`.
3. Save the query and note its **Query ID**.

### Step C — enable Flex Web Service token
1. In Flex Web Service section, generate/enable token.
2. Copy token (`IBKR_FLEX_TOKEN`).
3. Token can expire/rotate, so replace it in `.env` when needed.

### Step D — test manually (recommended)
Use two-step IBKR API flow:

1) Request report generation:
```text
https://www.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest?t=<TOKEN>&q=<QUERY_ID>&v=3
```

2) Read `ReferenceCode` from response and download report:
```text
https://www.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement?t=<TOKEN>&q=<REFERENCE_CODE>&v=3
```

If XML is returned and includes your account summary/NAV, configuration is correct.

---

## 6) Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # if you create .env.example, otherwise create manually
python -m app.main
```

Try in Telegram:
- `/status`

---

## 7) Complete Ubuntu VPS deployment algorithm (private server)

Use this exact sequence on a clean Ubuntu VPS.

### 7.1 Initial server hardening
1. SSH into VPS.
2. Create non-root user.
3. Add SSH key auth.
4. Disable password auth in `sshd_config`.
5. Enable firewall:
   - allow SSH only,
   - outbound HTTPS allowed.
6. Keep system updated:
   - `apt update && apt upgrade -y`

### 7.2 Install runtime dependencies
1. Install packages:
   - `python3`
   - `python3-venv`
   - `python3-pip`
   - `git`
2. Clone repository into `/opt/telegram-portfolio-bot` (or your path).

### 7.3 Prepare application
1. Enter project directory.
2. Create virtualenv and activate.
3. `pip install -r requirements.txt`
4. Create `.env` with production secrets.
5. Restrict file permissions:
   - `chmod 600 .env`

### 7.4 Create systemd service
Create `/etc/systemd/system/portfolio-bot.service`:

```ini
[Unit]
Description=Telegram Portfolio Bot
After=network.target

[Service]
Type=simple
User=your_linux_user
WorkingDirectory=/opt/telegram-portfolio-bot
EnvironmentFile=/opt/telegram-portfolio-bot/.env
ExecStart=/opt/telegram-portfolio-bot/.venv/bin/python -m app.main
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 7.5 Enable and start
```bash
sudo systemctl daemon-reload
sudo systemctl enable portfolio-bot
sudo systemctl start portfolio-bot
```

### 7.6 Verify
```bash
sudo systemctl status portfolio-bot
journalctl -u portfolio-bot -f
```

### 7.7 Operations playbook (ongoing)
- Update code:
  1. `git pull`
  2. `pip install -r requirements.txt`
  3. `sudo systemctl restart portfolio-bot`
- Rotate secrets:
  1. Edit `.env`
  2. `sudo systemctl restart portfolio-bot`
- If message stops:
  - check Telegram token,
  - check chat ID,
  - check IBKR token expiration,
  - inspect logs for API permission errors.

---

## 8) Security checklist

- Use **read-only** API keys on all platforms.
- Never commit `.env`.
- Rotate keys periodically.
- Do not reuse exchange passwords as API passphrases.
- Keep VPS patched and SSH locked down.
- Consider running bot under dedicated Linux user.

---

## 9) Useful project files

- `app/main.py` — app entrypoint
- `app/config.py` — env config + defaults + validation
- `app/telegram_client.py` — command handling + scheduled sending
- `app/aggregator.py` — combines platform balances
- `app/platforms/*.py` — platform-specific integrations
- `requirements.txt` — dependencies

