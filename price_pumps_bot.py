import os
import time
import asyncio
import logging
from typing import Dict, List, Set

import ccxt
import pandas as pd
import pytz
from datetime import datetime

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

TG_TOKEN = os.getenv("TG_TOKEN", "")

PROXY_HOST = os.getenv("PROXY_HOST", "")
PROXY_PORT = os.getenv("PROXY_PORT", "")
PROXY_USER = os.getenv("PROXY_USER", "")
PROXY_PASS = os.getenv("PROXY_PASS", "")

if PROXY_HOST and PROXY_PORT:
    if PROXY_USER and PROXY_PASS:
        PROXY_URL = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    else:
        PROXY_URL = f"http://{PROXY_HOST}:{PROXY_PORT}"
else:
    PROXY_URL = None

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
LOG = logging.getLogger("pricepump")

MIN_PRICE_RATIO = 1.30
MIN_VOL_USD_24H = 20_000_000
HEARTBEAT_INTERVAL = 1 * 3600

AUTO_STATE: Dict[int, dict] = {}

GLOBAL_STATE: Dict[str, object] = {
    "last_hour_ts": 0,
    "hits": [],
}

COMMON = {"enableRateLimit": True, "timeout": 15000}

EX_BYBIT = ccxt.bybit(COMMON)
EX_BYBIT.options.setdefault("defaultType", "swap")

EX_BINANCE = ccxt.binance(COMMON)
EX_BINANCE.options.setdefault("defaultType", "future")

EX_MEXC = ccxt.mexc(COMMON)
EX_MEXC.options.setdefault("defaultType", "swap")

EXCHANGES = {
    "Bybit": EX_BYBIT,
    "Binance": EX_BINANCE,
    "MEXC": EX_MEXC,
}

EXCHANGE_URLS = {
    "Binance": "https://www.binance.com/en/futures/{symbol}",
    "Bybit": "https://www.bybit.com/trade/usdt/{symbol}",
    "MEXC": "https://www.mexc.com/exchange/{symbol}",
}


def get_moscow_time() -> datetime:
    tz = pytz.timezone("Europe/Moscow")
    return datetime.now(tz)


def get_base_symbol(symbol: str) -> str:
    return symbol.split("/")[0]


def format_symbol_for_exchange(ex_name: str, symbol: str) -> str:
    if ex_name == "Binance":
        return symbol.replace("/", "").replace(":", "")
    elif ex_name == "Bybit":
        return symbol.replace("/", "").replace(":", "")
    elif ex_name == "MEXC":
        return symbol.replace("/", "_").replace(":", "_")
    else:
        return symbol.replace("/", "")


def get_exchange_url(ex_name: str, symbol: str) -> str:
    formatted_symbol = format_symbol_for_exchange(ex_name, symbol)
    if ex_name in EXCHANGE_URLS:
        return EXCHANGE_URLS[ex_name].format(symbol=formatted_symbol)
    return "#"


def get_usdt_futures(ex):
    out = []
    markets = ex.load_markets()
    for s, m in markets.items():
        if (m.get("swap") or m.get("future")) and m.get("linear") and m.get("quote") == "USDT":
            out.append(s)
    return out


def fetch_price_24h(ex, symbol):
    try:
        data = ex.fetch_ohlcv(symbol, "1h", limit=24)
    except Exception as e:
        LOG.warning(f"{ex.id} {symbol} fetch_ohlcv error: {e}")
        return None

    if not data or len(data) < 2:
        return None

    df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])

    first = df.iloc[0]
    last = df.iloc[-1]

    first_close = float(first["close"])
    last_close = float(last["close"])
    if first_close <= 0:
        return None

    price_ratio = last_close / first_close

    df["usd_vol"] = df["volume"] * df["close"]
    vol_24h = float(df["usd_vol"].sum())

    return {
        "price_ratio": price_ratio,
        "vol_24h": vol_24h,
    }


def symbol_link(ex_name, symbol, is_new=False):
    base = symbol.split("/")[0] + "USDT"
    prefix = ex_name.upper()
    tv_url = f"https://www.tradingview.com/chart/?symbol={prefix}:{base}.P"
    exchange_url = get_exchange_url(ex_name, symbol)

    if is_new:
        return f"🆕 <b>{base}</b> (<a href=\"{tv_url}\">TV</a> • <a href=\"{exchange_url}\">{ex_name}</a>)"
    return f"<b>{base}</b> (<a href=\"{tv_url}\">TV</a> • <a href=\"{exchange_url}\">{ex_name}</a>)"


def deduplicate_by_best(hits: List[dict]) -> List[dict]:
    best_by_coin: Dict[str, dict] = {}

    for hit in hits:
        base_coin = hit["base_coin"]
        if base_coin not in best_by_coin:
            best_by_coin[base_coin] = hit
        else:
            if hit["price_ratio"] > best_by_coin[base_coin]["price_ratio"]:
                best_by_coin[base_coin] = hit

    return list(best_by_coin.values())


async def scan_exchange(ex_name, ex) -> List[dict]:
    results: List[dict] = []
    symbols = get_usdt_futures(ex)

    for s in symbols:
        data = await asyncio.to_thread(fetch_price_24h, ex, s)
        if not data:
            continue

        if data["price_ratio"] >= MIN_PRICE_RATIO and data["vol_24h"] >= MIN_VOL_USD_24H:
            base_coin = get_base_symbol(s)
            results.append({
                "exchange": ex_name,
                "symbol": s,
                "base_coin": base_coin,
                "price_ratio": data["price_ratio"],
                "vol_24h": data["vol_24h"],
            })

    return results


async def global_scan_worker(context: ContextTypes.DEFAULT_TYPE):
    if not AUTO_STATE:
        return

    now = int(time.time())
    hour_sec = 3600
    cur_hour_index = now // hour_sec
    last_closed_hour_ts = (cur_hour_index - 1) * hour_sec

    global_last = GLOBAL_STATE.get("last_hour_ts", 0)
    if global_last == last_closed_hour_ts:
        return

    LOG.info("[GLOBAL] Detected closed H1 candle, starting global scan")

    all_hits: List[dict] = []
    scan_start = time.time()

    for name, ex in EXCHANGES.items():
        try:
            hits = await scan_exchange(name, ex)
            all_hits.extend(hits)
            LOG.info(f"[GLOBAL] {name}: found {len(hits)} hits before deduplication")
        except Exception as e:
            LOG.exception(f"[GLOBAL] Error while scanning {name}: {e}")

    if not all_hits:
        LOG.info("[GLOBAL] No hits, updating GLOBAL_STATE with empty result")
        GLOBAL_STATE["last_hour_ts"] = last_closed_hour_ts
        GLOBAL_STATE["hits"] = []
        return

    unique_hits = deduplicate_by_best(all_hits)
    scan_duration = time.time() - scan_start

    GLOBAL_STATE["last_hour_ts"] = last_closed_hour_ts
    GLOBAL_STATE["hits"] = unique_hits

    LOG.info(
        f"[GLOBAL] Unique coins: {len(unique_hits)} | Scan time: {scan_duration:.2f}s"
    )


async def autoscan_worker(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.chat_id

    state = AUTO_STATE.get(chat_id)
    if not state:
        try:
            job.schedule_removal()
        except Exception:
            pass
        return

    global_hour = GLOBAL_STATE.get("last_hour_ts", 0)
    global_hits: List[dict] = GLOBAL_STATE.get("hits", [])

    if global_hour == 0:
        return

    chat_last_hour = state.get("last_hour_ts", 0)
    if chat_last_hour == global_hour:
        return

    state["last_hour_ts"] = global_hour

    if not global_hits:
        LOG.info(f"[{chat_id}] No signals for this hour")
        return

    previous_symbols: Set[str] = state.get("previous_symbols", set())

    new_hits = []
    old_hits = []

    for h in global_hits:
        base_coin = h["base_coin"]
        if base_coin not in previous_symbols:
            new_hits.append(h)
        else:
            old_hits.append(h)

    new_hits.sort(key=lambda x: x["price_ratio"], reverse=True)
    old_hits.sort(key=lambda x: x["price_ratio"], reverse=True)
    sorted_hits = new_hits + old_hits

    current_symbols = {h["base_coin"] for h in global_hits}
    state["previous_symbols"] = current_symbols

    msk = get_moscow_time()
    hour = msk.hour

    text = (
        f"🕐 <b>Scan {hour:02d}:00 MSK</b>\n"
        f"📊 Found: {len(sorted_hits)} coins (🆕 new: {len(new_hits)})\n"
        "📈 <b>24h price growth ≥ 30%</b>\n"
        "💰 24h volume ≥ 20M$\n"
        "🎯 Best exchange per coin is shown\n\n"
    )

    for h in sorted_hits:
        ratio = h["price_ratio"]
        pct = (ratio - 1.0) * 100
        is_new = h in new_hits
        link = symbol_link(h["exchange"], h["symbol"], is_new=is_new)

        text += (
            f"• {link}\n"
            f"  📈 <b>{ratio:.2f}×</b> (+{pct:.0f}%)\n"
            f"  💰 24h volume: <b>{h['vol_24h']:,.0f}$</b>\n\n"
        )

    try:
        await context.bot.send_message(
            chat_id,
            text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        LOG.info(
            f"[{chat_id}] Sent {len(sorted_hits)} signals "
            f"(new: {len(new_hits)}, repeated: {len(old_hits)})"
        )
    except Exception as e:
        LOG.error(f"[{chat_id}] Error sending message: {e}")


async def heartbeat_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.chat_id

    if chat_id not in AUTO_STATE:
        try:
            job.schedule_removal()
        except Exception:
            pass
        return

    try:
        await context.bot.send_message(
            chat_id,
            "🧪 Heartbeat: bot is running.",
        )
    except Exception as e:
        LOG.error(e)


async def start_autoscan(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    old = AUTO_STATE.get(chat_id)
    if old and "tasks" in old:
        for job in old["tasks"]:
            try:
                job.schedule_removal()
            except Exception:
                pass

    global_hour = GLOBAL_STATE.get("last_hour_ts", 0)

    AUTO_STATE[chat_id] = {
        "previous_symbols": set(),
        "last_hour_ts": global_hour,
        "tasks": [],
    }

    job_auto = context.job_queue.run_repeating(
        autoscan_worker,
        interval=60,
        first=10,
        chat_id=chat_id,
    )

    job_hb = context.job_queue.run_repeating(
        heartbeat_job,
        interval=HEARTBEAT_INTERVAL,
        first=HEARTBEAT_INTERVAL,
        chat_id=chat_id,
    )

    AUTO_STATE[chat_id]["tasks"] = [job_auto, job_hb]

    LOG.info(f"[{chat_id}] Autoscan subscribed for this chat")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    now = get_moscow_time()

    txt = (
        "🤖 <b>24h Price Pump Monitor Bot</b>\n\n"
        "📌 <b>Signal conditions:</b>\n"
        "• 24h price growth > 30%\n"
        "• 24h volume ≥ 20M$\n\n"
        "🎯 <b>Output features:</b>\n"
        "• 🆕 New coins are shown first (per chat)\n"
        "• Best exchange is selected per coin\n"
        "• Coins are sorted by price growth strength\n\n"
        "🔗 <b>Links:</b>\n"
        "• TV — TradingView chart\n"
        "• Exchange — direct trading link\n\n"
        "⏰ <b>Autoscan:</b>\n"
        "• Global market scan runs once per closed H1 candle\n"
        "• Your chat receives signals when a new hour is available\n\n"
        f"🕐 <b>Current time: {now.strftime('%H:%M:%S')} MSK</b>\n\n"
        "<b>Commands:</b>\n"
        "/autostop — stop autoscan\n"
        "/autostart — start autoscan\n"
        "/status — current status"
    )

    await update.message.reply_text(txt, parse_mode=ParseMode.HTML)

    if chat_id not in AUTO_STATE:
        await start_autoscan(chat_id, context)
        await update.message.reply_text(
            "✅ Autoscan enabled.\n\n"
            "ℹ️ <i>On the first run all coins in the first signal will be marked as new for this chat.</i>",
            parse_mode=ParseMode.HTML,
        )


async def cmd_autostart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await start_autoscan(chat_id, context)
    await update.message.reply_text(
        "✅ Autoscan enabled.\n\n"
        "ℹ️ <i>Coin history for this chat has been reset.</i>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_autostop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    old = AUTO_STATE.get(chat_id)
    if old and "tasks" in old:
        for job in old["tasks"]:
            try:
                job.schedule_removal()
            except Exception:
                pass

    if chat_id in AUTO_STATE:
        del AUTO_STATE[chat_id]

    await update.message.reply_text("⛔ Autoscan stopped.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    now = get_moscow_time()

    if chat_id in AUTO_STATE:
        prev_count = len(AUTO_STATE[chat_id].get("previous_symbols", []))
        last_hour_ts = AUTO_STATE[chat_id].get("last_hour_ts", 0)
        if last_hour_ts:
            msk_tz = pytz.timezone("Europe/Moscow")
            last_dt = datetime.fromtimestamp(last_hour_ts, msk_tz)
            last_hour_str = last_dt.strftime("%d.%m %H:%M")
        else:
            last_hour_str = "—"

        txt = (
            f"🟢 <b>Autoscan is active</b>\n"
            f"Tracked coins in the last hour: {prev_count}\n"
            f"Last processed hour: {last_hour_str} MSK\n"
            f"Current time: {now.strftime('%H:%M:%S')} MSK"
        )
    else:
        txt = (
            f"🔴 <b>Autoscan is disabled</b>\n"
            f"Current time: {now.strftime('%H:%M:%S')} MSK\n\n"
            "Use /autostart to enable it."
        )

    await update.message.reply_text(txt, parse_mode=ParseMode.HTML)


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = get_moscow_time()
    await update.message.reply_text(
        "🤖 <b>Bot commands:</b>\n\n"
        "/start — description and auto start\n"
        "/autostart — enable autoscan\n"
        "/autostop — disable autoscan\n"
        "/status — current status\n\n"
        f"🕐 {now.strftime('%H:%M:%S')} MSK",
        parse_mode=ParseMode.HTML,
    )


def main():
    if not TG_TOKEN:
        LOG.error("TG_TOKEN environment variable is not set. Exiting.")
        raise SystemExit(1)

    builder = Application.builder().token(TG_TOKEN)
    if PROXY_URL:
        builder.proxy(PROXY_URL)
    app = builder.build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("autostart", cmd_autostart))
    app.add_handler(CommandHandler("autostop", cmd_autostop))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    app.job_queue.run_repeating(
        global_scan_worker,
        interval=60,
        first=10,
        name="global_price_scan",
    )

    LOG.info("PricePumpBot started (global scan + per-chat history)")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
