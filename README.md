A Telegram bot that monitors 24h price pumps on USDT linear futures across multiple exchanges (Binance, Bybit, MEXC). It detects coins where the last 24 hours' price and volume meet specified thresholds and sends alerts to subscribed chats.

- 🔍 Scans USDT linear futures on:
  - Binance (futures)
  - Bybit (USDT perpetual)
  - MEXC (USDT perpetual)
- 📈 Signal conditions:
  - 24h price growth ≥ 30% (`MIN_PRICE_RATIO = 1.30`)
  - 24h volume ≥ 20M USD (`MIN_VOL_USD_24H = 20_000_000`)
- 🧠 Deduplication:
  - Only the **best exchange per coin** (max price growth) is shown
- 🆕 New vs old:
  - New coins (not seen in the previous hour for this chat) are flagged with `🆕` and listed first
- ⏰ Scheduling:
  - Global worker runs every minute and scans on each closed H1 candle
  - Per-chat workers send signals to chats when a new hour is available
  - Heartbeat message every hour to confirm the bot is alive
