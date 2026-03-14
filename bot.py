"""
CIPHER Telegram Bot v3
CoinGecko Pro + DeFiLlama + Alternative.me + Groq (Llama 3.3 70B)
Commands: /cipher, /btc, /dominance, /etf, /trending, /defi, /fear, /macro, /watchlist, /setup, /ask
Multi-user with subscription support
"""

import os, json, logging, asyncio, httpx
from pathlib import Path
from datetime import datetime, timezone
from groq import Groq
from telegram import Update, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters, ConversationHandler, CallbackQueryHandler,
)
from telegram.constants import ParseMode

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
GROQ_KEY        = os.getenv("GROQ_API_KEY", "")
COINGECKO_KEY   = os.getenv("COINGECKO_API_KEY", "")
OWNER_ID        = int(os.getenv("ALLOWED_USER_ID", "1953473977"))

CG_BASE         = "https://pro-api.coingecko.com/api/v3"
CG_HEADERS      = {"x-cg-pro-api-key": COINGECKO_KEY}
DEFILLAMA_BASE  = "https://api.llama.fi"
FEAR_GREED_URL  = "https://api.alternative.me/fng/?limit=3"

DB_FILE         = Path("cipher_db.json")
WAITING_SETUP   = 1
WAITING_WATCHLIST = 2
WAITING_ASK     = 3

# ── Database ──────────────────────────────────────────────────────────────────
def load_db() -> dict:
    if DB_FILE.exists():
        try:
            return json.loads(DB_FILE.read_text())
        except Exception:
            pass
    return {"users": {}, "subscriptions": {}}

def save_db(db: dict):
    DB_FILE.write_text(json.dumps(db, indent=2))

def get_user(user_id: int) -> dict:
    db = load_db()
    uid = str(user_id)
    if uid not in db["users"]:
        db["users"][uid] = {
            "custom_instructions": "",
            "watchlist": ["bitcoin", "ethereum"],
            "joined": datetime.now(timezone.utc).isoformat(),
            "plan": "free",  # free | pro
        }
        save_db(db)
    return db["users"][uid]

def save_user(user_id: int, data: dict):
    db = load_db()
    db["users"][str(user_id)] = data
    save_db(db)

def is_pro(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    user = get_user(user_id)
    return user.get("plan") == "pro"

def is_authorized(update: Update) -> bool:
    """Free tier: anyone can use basic commands. Pro: gated features."""
    return True  # Open registration — use plan gating for premium features

# ── CIPHER System Prompt ──────────────────────────────────────────────────────
CIPHER_SYSTEM = """You are CIPHER — crypto research intelligence system for professional analysts.

You receive LIVE structured data fetched from CoinGecko Pro, DeFiLlama, and other on-chain sources.
Your job: interpret what the numbers MEAN for price, risk, and opportunity — not describe them.

ANALYSIS RULES:
- NEVER lead with lagging indicators (RSI, MACD, Bollinger Bands, MAs alone)
- ALWAYS interpret: exchange flows, funding rates, OI, liquidation levels, stablecoin flows, ETF data
- Require 2+ confirming signals before flagging a trade setup
- Every trade setup MUST have an invalidation level — no invalidation = no trade
- Conflicting signals = NEUTRAL + wait
- End every report with clear next action: trade / monitor / flat / wait

TONE: Senior analyst — direct, specific, no hype, no filler words.
Use plain text. Telegram renders better without markdown headers.
Use emoji section markers (📊 🔗 📈 🧠 📐 ⚡ 🎯) for structure."""

# ── HTTP Helpers ──────────────────────────────────────────────────────────────
async def fetch(url: str, headers: dict = {}, params: dict = {}) -> dict | list | None:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=headers, params=params)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        logger.warning(f"Fetch error [{url[:60]}]: {e}")
        return None

async def fetch_cg(endpoint: str, params: dict = {}) -> dict | list | None:
    return await fetch(f"{CG_BASE}{endpoint}", CG_HEADERS, params)

# ── Data Fetchers ─────────────────────────────────────────────────────────────

async def data_market_snapshot(coin_ids: list = None) -> str:
    ids = ",".join(coin_ids) if coin_ids else "bitcoin,ethereum,solana,binancecoin,ripple,cardano,avalanche-2,chainlink"
    coins, global_data = await asyncio.gather(
        fetch_cg("/coins/markets", {
            "vs_currency": "usd", "ids": ids,
            "order": "market_cap_desc",
            "price_change_percentage": "1h,24h,7d",
            "sparkline": "false",
        }),
        fetch_cg("/global"),
    )
    lines = [f"LIVE MARKET DATA | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]
    if coins:
        for c in coins:
            ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
            ch24h = c.get("price_change_percentage_24h") or 0
            ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
            lines.append(
                f"{c['symbol'].upper():6} ${c['current_price']:>12,.4f} | "
                f"1h:{ch1h:+6.2f}% 24h:{ch24h:+6.2f}% 7d:{ch7d:+6.2f}% | "
                f"Vol: ${c['total_volume']:>14,.0f} | MCap: ${c['market_cap']:>16,.0f}"
            )
    if global_data and "data" in global_data:
        g = global_data["data"]
        lines.append(f"\nBTC Dom: {g['market_cap_percentage'].get('btc',0):.2f}% | "
                     f"ETH Dom: {g['market_cap_percentage'].get('eth',0):.2f}% | "
                     f"Total MC: ${g['total_market_cap'].get('usd',0):,.0f} | "
                     f"24h MC Change: {g.get('market_cap_change_percentage_24h_usd',0):+.2f}%")
    return "\n".join(lines)

async def data_dominance() -> str:
    global_data, coins = await asyncio.gather(
        fetch_cg("/global"),
        fetch_cg("/coins/markets", {
            "vs_currency": "usd",
            "ids": "bitcoin,ethereum,tether,binancecoin,solana,ripple,usd-coin,staked-ether,tron,avalanche-2",
            "order": "market_cap_desc",
            "price_change_percentage": "24h,7d",
        }),
    )
    lines = [f"DOMINANCE & ROTATION DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if global_data and "data" in global_data:
        g = global_data["data"]
        dom = g.get("market_cap_percentage", {})
        lines.append("Market Cap Dominance:")
        for sym, pct in sorted(dom.items(), key=lambda x: -x[1])[:8]:
            lines.append(f"  {sym.upper():8} {pct:.2f}%")
        lines.append(f"\nTotal MC: ${g['total_market_cap'].get('usd',0):,.0f}")
        lines.append(f"Total Vol 24h: ${g['total_volume'].get('usd',0):,.0f}")
        lines.append(f"Active Cryptos: {g.get('active_cryptocurrencies','N/A')}")
        lines.append(f"Markets: {g.get('markets','N/A')}")
    if coins:
        lines.append("\nTop 10 by MCap — Price Performance:")
        for c in coins[:10]:
            ch24 = c.get("price_change_percentage_24h") or 0
            ch7d = c.get("price_change_percentage_7d_in_currency") or 0
            lines.append(f"  {c['symbol'].upper():8} ${c['current_price']:>12,.4f} | 24h:{ch24:+6.2f}% | 7d:{ch7d:+6.2f}%")
    return "\n".join(lines)

async def data_trending() -> str:
    trending, top_gainers = await asyncio.gather(
        fetch_cg("/search/trending"),
        fetch_cg("/coins/markets", {
            "vs_currency": "usd",
            "order": "percent_change_24h_desc",
            "per_page": "20",
            "page": "1",
            "price_change_percentage": "1h,24h",
        }),
    )
    lines = [f"TRENDING & NARRATIVE DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if trending:
        lines.append("Trending on CoinGecko (search volume):")
        for i, item in enumerate(trending.get("coins", [])[:7], 1):
            c = item["item"]
            lines.append(f"  {i}. {c['name']} ({c['symbol'].upper()}) | Rank #{c.get('market_cap_rank','?')} | Score: {c.get('score',0)}")
        if trending.get("nfts"):
            lines.append("\nTrending NFTs:")
            for n in trending["nfts"][:3]:
                lines.append(f"  {n['name']} | Floor: {n.get('floor_price_in_native_currency','?')} ETH")
    if top_gainers:
        lines.append("\nTop 24h Gainers (CoinGecko top 20 by MCap):")
        gainers = sorted(top_gainers, key=lambda x: x.get("price_change_percentage_24h") or 0, reverse=True)[:5]
        for c in gainers:
            ch24 = c.get("price_change_percentage_24h") or 0
            lines.append(f"  {c['symbol'].upper():8} +{ch24:.2f}% | ${c['current_price']:,.4f} | MCap: ${c['market_cap']:,.0f}")
        lines.append("\nTop 24h Losers:")
        losers = sorted(top_gainers, key=lambda x: x.get("price_change_percentage_24h") or 0)[:5]
        for c in losers:
            ch24 = c.get("price_change_percentage_24h") or 0
            lines.append(f"  {c['symbol'].upper():8} {ch24:.2f}% | ${c['current_price']:,.4f} | MCap: ${c['market_cap']:,.0f}")
    return "\n".join(lines)

async def data_defi() -> str:
    tvl, protocols, chains = await asyncio.gather(
        fetch(f"{DEFILLAMA_BASE}/tvl"),
        fetch(f"{DEFILLAMA_BASE}/protocols"),
        fetch(f"{DEFILLAMA_BASE}/v2/chains"),
    )
    lines = [f"DEFI TVL DATA (DeFiLlama) | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if tvl:
        lines.append(f"Total DeFi TVL: ${float(tvl):,.0f}")
    if protocols:
        lines.append("\nTop 10 Protocols by TVL:")
        sorted_p = sorted(protocols, key=lambda x: x.get("tvl") or 0, reverse=True)[:10]
        for p in sorted_p:
            ch1d = p.get("change_1d") or 0
            ch7d = p.get("change_7d") or 0
            lines.append(
                f"  {p['name']:20} TVL: ${p.get('tvl',0):>14,.0f} | "
                f"1d:{ch1d:+6.2f}% | 7d:{ch7d:+6.2f}% | Chain: {p.get('chain','multi')}"
            )
    if chains:
        lines.append("\nTop 10 Chains by TVL:")
        sorted_c = sorted(chains, key=lambda x: x.get("tvlPrevDay") or x.get("tvl") or 0, reverse=True)[:10]
        for c in sorted_c:
            lines.append(f"  {c.get('name','?'):15} TVL: ${c.get('tvl', c.get('tvlPrevDay',0)):>14,.0f}")
    return "\n".join(lines)

async def data_fear_greed() -> str:
    fg, global_data, stables = await asyncio.gather(
        fetch(FEAR_GREED_URL),
        fetch_cg("/global"),
        fetch_cg("/coins/markets", {
            "vs_currency": "usd",
            "ids": "tether,usd-coin,dai,first-digital-usd",
            "order": "market_cap_desc",
        }),
    )
    lines = [f"SENTIMENT DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if fg and "data" in fg:
        lines.append("Fear & Greed Index (Alternative.me):")
        for entry in fg["data"][:3]:
            ts = datetime.fromtimestamp(int(entry["timestamp"]), tz=timezone.utc).strftime("%b %d")
            lines.append(f"  {ts}: {entry['value']:>3}/100 — {entry['value_classification']}")
    if global_data and "data" in global_data:
        g = global_data["data"]
        lines.append(f"\nMarket MC 24h Change: {g.get('market_cap_change_percentage_24h_usd',0):+.2f}%")
        lines.append(f"BTC Dominance: {g['market_cap_percentage'].get('btc',0):.2f}%")
    if stables:
        lines.append("\nStablecoin Market Caps (supply proxy):")
        total_stable = 0
        for s in stables:
            lines.append(f"  {s['symbol'].upper():8} MCap: ${s['market_cap']:>16,.0f} | Vol 24h: ${s['total_volume']:>14,.0f}")
            total_stable += s.get("market_cap", 0)
        lines.append(f"  Total Stablecoin MCap: ${total_stable:,.0f}")
    return "\n".join(lines)

async def data_etf() -> str:
    # CoinGecko Pro ETF-related: institutional BTC/ETH data + GBTC proxy
    btc_detail, eth_detail = await asyncio.gather(
        fetch_cg("/coins/bitcoin", {"localization":"false","tickers":"false","market_data":"true","community_data":"false","developer_data":"false"}),
        fetch_cg("/coins/ethereum", {"localization":"false","tickers":"false","market_data":"true","community_data":"false","developer_data":"false"}),
    )
    lines = [f"ETF & INSTITUTIONAL PROXY DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    lines.append("NOTE: Direct ETF flow data (BlackRock, Fidelity) requires Bloomberg/SoSoValue premium.")
    lines.append("Below is institutional proxy data from CoinGecko Pro:\n")
    for name, data in [("BTC", btc_detail), ("ETH", eth_detail)]:
        if data:
            md = data.get("market_data", {})
            lines.append(f"{name} Institutional Proxy:")
            lines.append(f"  Price:     ${md.get('current_price',{}).get('usd',0):,.2f}")
            lines.append(f"  MCap:      ${md.get('market_cap',{}).get('usd',0):,.0f}")
            lines.append(f"  Vol 24h:   ${md.get('total_volume',{}).get('usd',0):,.0f}")
            lines.append(f"  ATH:       ${md.get('ath',{}).get('usd',0):,.0f} ({md.get('ath_change_percentage',{}).get('usd',0):.1f}% from ATH)")
            lines.append(f"  ATH Date:  {md.get('ath_date',{}).get('usd','?')[:10]}")
            lines.append(f"  Circulating Supply: {md.get('circulating_supply',0):,.0f}")
            lines.append(f"  Max Supply: {md.get('max_supply') or 'unlimited'}")
            vol = md.get('total_volume',{}).get('usd',0)
            mc  = md.get('market_cap',{}).get('usd',1)
            lines.append(f"  Vol/MCap Ratio: {vol/mc:.4f} (>0.1 = high institutional activity)")
            lines.append("")
    lines.append("For live ETF flow: track sosovalue.org/assets/eth-etf or bloomberg terminal.")
    return "\n".join(lines)

async def data_macro() -> str:
    # Pull upcoming events from CoinGecko events + static high-impact calendar
    events = await fetch_cg("/events", {"upcoming_events_only": "true", "per_page": "10"})
    lines = [f"MACRO & EVENT CALENDAR | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]
    lines.append("HIGH-IMPACT RECURRING EVENTS (check ForexFactory/Investing.com for exact dates):")
    lines.append("  🔴 FOMC Meeting + Press Conference — Fed rate decision, critical for crypto")
    lines.append("  🔴 US CPI Release (monthly) — inflation data, risk-on/off trigger")
    lines.append("  🔴 US NFP / Jobs Report (1st Friday of month) — macro risk sentiment")
    lines.append("  🟡 BTC Options Expiry (every Friday, large monthly on last Friday) — Deribit")
    lines.append("  🟡 Fed Speaker Appearances — watch for forward guidance changes")
    lines.append("  🟡 US PPI Release — producer inflation, leads CPI")
    lines.append("  🟡 JOLTS / ADP Employment — leading NFP indicator")
    lines.append("  🟢 Token Unlocks — check tokenunlocks.app for asset-specific schedules")
    lines.append("  🟢 Protocol Governance Votes — check snapshot.org")
    lines.append("")
    if events and "data" in events:
        lines.append("UPCOMING CRYPTO EVENTS (CoinGecko):")
        for e in events["data"][:8]:
            date = e.get("start_date","?")[:10]
            lines.append(f"  {date} | {e.get('title','?')} | {e.get('type','?')} | {e.get('coin',{}).get('name','General')}")
    lines.append("\nSOURCES FOR LIVE MACRO CALENDAR:")
    lines.append("  ForexFactory.com — economic calendar with impact ratings")
    lines.append("  Investing.com/economic-calendar — full global macro events")
    lines.append("  CMEGroup FedWatch — FOMC rate probability tracker")
    return "\n".join(lines)

async def data_watchlist(coin_ids: list) -> str:
    if not coin_ids:
        return "Watchlist is empty. Use /watchlist to add coins."
    coins = await fetch_cg("/coins/markets", {
        "vs_currency": "usd",
        "ids": ",".join(coin_ids),
        "order": "market_cap_desc",
        "price_change_percentage": "1h,24h,7d,30d",
        "sparkline": "false",
    })
    lines = [f"WATCHLIST DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if coins:
        for c in coins:
            ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
            ch24h = c.get("price_change_percentage_24h") or 0
            ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
            ch30d = c.get("price_change_percentage_30d_in_currency") or 0
            ath_pct = c.get("ath_change_percentage") or 0
            lines.append(f"━━ {c['name']} ({c['symbol'].upper()}) ━━")
            lines.append(f"  Price:  ${c['current_price']:,.6f}")
            lines.append(f"  1h:     {ch1h:+.2f}%")
            lines.append(f"  24h:    {ch24h:+.2f}%")
            lines.append(f"  7d:     {ch7d:+.2f}%")
            lines.append(f"  30d:    {ch30d:+.2f}%")
            lines.append(f"  Vol:    ${c['total_volume']:,.0f}")
            lines.append(f"  MCap:   ${c['market_cap']:,.0f} | Rank #{c.get('market_cap_rank','?')}")
            lines.append(f"  vs ATH: {ath_pct:.1f}%")
            lines.append("")
    return "\n".join(lines)

async def data_btc_full() -> str:
    btc = await fetch_cg("/coins/bitcoin", {
        "localization":"false","tickers":"false",
        "market_data":"true","community_data":"true","developer_data":"false",
    })
    if not btc:
        return "BTC data unavailable."
    md = btc.get("market_data", {})
    cd = btc.get("community_data", {})
    lines = [f"BTC FULL SNAPSHOT | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    lines.append(f"Price:       ${md.get('current_price',{}).get('usd',0):,.2f}")
    lines.append(f"1h:          {(md.get('price_change_percentage_1h_in_currency',{}) or {}).get('usd',0):+.2f}%")
    lines.append(f"24h:         {md.get('price_change_percentage_24h',0):+.2f}%")
    lines.append(f"7d:          {(md.get('price_change_percentage_7d_in_currency',{}) or {}).get('usd',0):+.2f}%")
    lines.append(f"30d:         {(md.get('price_change_percentage_30d_in_currency',{}) or {}).get('usd',0):+.2f}%")
    lines.append(f"1y:          {(md.get('price_change_percentage_1y_in_currency',{}) or {}).get('usd',0):+.2f}%")
    lines.append(f"24h Range:   ${md.get('low_24h',{}).get('usd',0):,.0f} – ${md.get('high_24h',{}).get('usd',0):,.0f}")
    lines.append(f"Vol 24h:     ${md.get('total_volume',{}).get('usd',0):,.0f}")
    lines.append(f"MCap:        ${md.get('market_cap',{}).get('usd',0):,.0f}")
    lines.append(f"ATH:         ${md.get('ath',{}).get('usd',0):,.0f} on {(md.get('ath_date',{}) or {}).get('usd','?')[:10]}")
    lines.append(f"vs ATH:      {(md.get('ath_change_percentage',{}) or {}).get('usd',0):.2f}%")
    lines.append(f"ATL:         ${md.get('atl',{}).get('usd',0):,.2f}")
    lines.append(f"Circulating: {md.get('circulating_supply',0):,.0f} BTC")
    lines.append(f"Max Supply:  21,000,000 BTC")
    lines.append(f"% Mined:     {md.get('circulating_supply',0)/21000000*100:.2f}%")
    if cd:
        lines.append(f"\nCommunity:")
        lines.append(f"  Twitter followers: {cd.get('twitter_followers',0):,}")
        lines.append(f"  Reddit subscribers: {cd.get('reddit_subscribers',0):,}")
    return "\n".join(lines)

# ── Groq Analysis ─────────────────────────────────────────────────────────────
async def call_claude(prompt: str, custom_instructions: str = "", max_tokens: int = 1500) -> str:
    """Named call_claude for compatibility — uses Groq Llama 3.3 70B under the hood."""
    client = Groq(api_key=GROQ_KEY)
    system = CIPHER_SYSTEM
    if custom_instructions.strip():
        system += f"\n\nCUSTOM ANALYST INSTRUCTIONS:\n{custom_instructions}"
    loop = asyncio.get_event_loop()
    def _call():
        return client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.3,  # Lower = more analytical, less creative
        )
    response = await loop.run_in_executor(None, _call)
    return response.choices[0].message.content.strip() or "⚠️ CIPHER returned no output."

# ── Send helper ────────────────────────────────────────────────────────────────
async def send_long(update: Update, text: str):
    for i in range(0, len(text), 4000):
        await update.message.reply_text(text[i:i+4000])

# ── Command Handlers ───────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    get_user(user.id)  # Initialize user record
    plan = "⭐ PRO" if is_pro(user.id) else "🆓 Free"
    await update.message.reply_text(
        f"⚡ *CIPHER Intelligence — Online*\n"
        f"Welcome {user.first_name} | Plan: {plan}\n\n"
        "*Commands:*\n"
        "/cipher — Full 30-min intelligence cycle\n"
        "/btc — Deep BTC snapshot\n"
        "/dominance — BTC dom + altcoin rotation\n"
        "/trending — Trending coins + narratives\n"
        "/defi — DeFi TVL breakdown\n"
        "/fear — Sentiment + Fear & Greed\n"
        "/etf — ETF & institutional proxy data\n"
        "/macro — Event calendar\n"
        "/watchlist — Your tracked coins\n"
        "/ask [question] — Ask anything with live data\n"
        "/setup — Custom instructions\n"
        "/help — Full command list",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📡 *CIPHER — All Commands*\n\n"
        "`/cipher` — Full cycle: market + on-chain + derivatives + macro + trade setup\n"
        "`/btc` — Deep BTC analysis with all timeframes\n"
        "`/dominance` — BTC dominance + altcoin rotation signals\n"
        "`/trending` — Top trending coins + gainers/losers narrative\n"
        "`/defi` — DeFi TVL by protocol + chain (DeFiLlama live)\n"
        "`/fear` — Fear & Greed + stablecoin supply + sentiment\n"
        "`/etf` — Institutional + ETF proxy data\n"
        "`/macro` — High-impact event calendar\n"
        "`/watchlist` — View/manage your tracked coins\n"
        "`/ask [question]` — Any question with live market context\n"
        "`/setup` — Set custom instructions for CIPHER\n\n"
        "Free-text messages also trigger CIPHER analysis.",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_cipher(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await update.message.reply_text("🔄 Fetching live data from CoinGecko + DeFiLlama...")
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    market, defi, fear, trending = await asyncio.gather(
        data_market_snapshot(),
        data_defi(),
        data_fear_greed(),
        data_trending(),
    )
    prompt = (
        f"{market}\n\n{fear}\n\n{trending}\n\n"
        f"DeFi Summary:\n{defi[:1000]}...\n\n"
        "Produce a full CIPHER intelligence cycle report. "
        "Interpret what ALL this data means for BTC and the broader market right now. "
        "Include: on-chain signal assessment, derivatives context, macro bias, "
        "narrative analysis from trending data, and a trade setup if signals support one. "
        "Be specific. Flag any notable divergences or extremes."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""), max_tokens=2000)
    await send_long(update, result)

async def cmd_btc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    btc_data = await data_btc_full()
    prompt = (
        f"{btc_data}\n\n"
        "Analyze this BTC data. Cover: price structure, volume quality, "
        "distance from ATH and what that implies, community sentiment signal. "
        "Give a concrete bias with reasoning. End with: BULLISH / BEARISH / NEUTRAL + one main reason."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_dominance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    dom_data = await data_dominance()
    prompt = (
        f"{dom_data}\n\n"
        "Analyze BTC dominance and altcoin rotation. "
        "Is capital rotating into alts or consolidating in BTC? "
        "Which sectors or assets show relative strength? "
        "What does current dominance level historically imply for altcoin season probability? "
        "Give a clear rotation thesis."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_trending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    trend_data = await data_trending()
    prompt = (
        f"{trend_data}\n\n"
        "Analyze this trending and momentum data. "
        "What narratives are building? Are these organic moves or late retail? "
        "Which trending coins have strong vs weak fundamentals to back the move? "
        "Flag any shill patterns or pump indicators. "
        "Give a narrative intelligence summary — what story is the market telling right now?"
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_defi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await update.message.reply_text("🔄 Fetching DeFiLlama live data...")
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    defi_data = await data_defi()
    prompt = (
        f"{defi_data}\n\n"
        "Analyze DeFi TVL data. "
        "Which protocols are gaining or losing TVL significantly? "
        "Which chains are dominant and is that shifting? "
        "What does TVL trend imply for the health of DeFi and ETH ecosystem? "
        "Flag any unusual TVL movements that warrant attention."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_fear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    fear_data = await data_fear_greed()
    prompt = (
        f"{fear_data}\n\n"
        "Analyze this sentiment data. "
        "NOTE: Fear & Greed is a lagging/confirming indicator — use it to gauge crowding, not as entry signal. "
        "What does current sentiment imply about positioning? "
        "Is stablecoin supply growing (dry powder building) or shrinking (capital deployed)? "
        "Cross-reference sentiment with dominance data — does it confirm or diverge? "
        "What does this mean for near-term risk/reward?"
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_etf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    etf_data = await data_etf()
    prompt = (
        f"{etf_data}\n\n"
        "Analyze institutional and ETF proxy signals. "
        "What does the Vol/MCap ratio imply about institutional activity? "
        "Is BTC behaving like an institutional asset right now or retail-driven? "
        "What does distance from ATH imply for ETF buyer entry points? "
        "Give context on what institutional flows typically look like at this price level."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_macro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    macro_data = await data_macro()
    market_data = await data_market_snapshot(["bitcoin","ethereum"])
    prompt = (
        f"{macro_data}\n\n"
        f"Current market context:\n{market_data[:500]}\n\n"
        "Give a macro intelligence briefing. "
        "What events are coming up that could move crypto markets? "
        "How should a crypto trader position around these events? "
        "What is the current macro regime (risk-on / risk-off / transitional) and why?"
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def cmd_watchlist_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    watchlist = user_data.get("watchlist", ["bitcoin","ethereum"])

    if context.args:
        # /watchlist add <coin> or /watchlist remove <coin>
        action = context.args[0].lower()
        if action == "add" and len(context.args) > 1:
            coin = context.args[1].lower()
            # Validate coin exists
            check = await fetch_cg(f"/coins/{coin}")
            if not check or "error" in str(check):
                await update.message.reply_text(f"❌ Coin '{coin}' not found. Use CoinGecko ID (e.g. 'chainlink' not 'LINK')")
                return
            if coin not in watchlist:
                watchlist.append(coin)
                user_data["watchlist"] = watchlist
                save_user(update.effective_user.id, user_data)
                await update.message.reply_text(f"✅ Added {coin} to watchlist.\nCurrent: {', '.join(watchlist)}")
            else:
                await update.message.reply_text(f"Already in watchlist: {coin}")
            return
        elif action == "remove" and len(context.args) > 1:
            coin = context.args[1].lower()
            if coin in watchlist:
                watchlist.remove(coin)
                user_data["watchlist"] = watchlist
                save_user(update.effective_user.id, user_data)
                await update.message.reply_text(f"✅ Removed {coin}.\nCurrent: {', '.join(watchlist)}")
            else:
                await update.message.reply_text(f"'{coin}' not in watchlist.")
            return

    # Default: show watchlist data
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    wl_data = await data_watchlist(watchlist)
    prompt = (
        f"{wl_data}\n\n"
        "Analyze each coin in this watchlist. "
        "For each: give a 2-line assessment of current momentum, key level to watch, and bias. "
        "End with: which coin has the strongest setup right now and why."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await update.message.reply_text(
        f"📋 Watchlist: {', '.join(watchlist)}\n"
        f"Edit: /watchlist add <coin-id> | /watchlist remove <coin-id>\n"
        f"(Use CoinGecko IDs: chainlink, solana, avalanche-2, etc.)\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await send_long(update, result)

async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    question = " ".join(context.args) if context.args else ""
    if not question:
        await update.message.reply_text("Usage: /ask [your question]\nExample: /ask Is SOL in a good buy zone right now?")
        return
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    market_data = await data_market_snapshot()
    prompt = (
        f"LIVE MARKET CONTEXT:\n{market_data}\n\n"
        f"ANALYST QUESTION: {question}\n\n"
        "Answer this question using the live data above + your analysis framework. "
        "Be specific and data-driven. If the question requires data not in the snapshot, say so and give best interpretation from available data."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

# ── /setup conversation ────────────────────────────────────────────────────────
async def cmd_setup_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    current = user_data.get("custom_instructions","").strip()
    msg = (
        f"⚙️ *Custom Instructions*\n\n"
        f"Current: `{current or 'None set'}`\n\n"
        "Send new instructions to replace. Examples:\n"
        "• Focus on SOL, LINK, ARB alongside BTC/ETH\n"
        "• Swing trading, 3–5 day horizon, not scalping\n"
        "• Max 2% risk per trade, capital: $10,000\n"
        "• Skip social sentiment, focus on on-chain only\n"
        "• My current positions: long BTC at $85k, long ETH at $2k\n\n"
        "Or /cancel to keep existing."
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
    return WAITING_SETUP

async def cmd_setup_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    user_data["custom_instructions"] = update.message.text.strip()
    save_user(update.effective_user.id, user_data)
    await update.message.reply_text(
        f"✅ *Saved.* Active in all future reports.\n\n`{user_data['custom_instructions']}`",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

async def cmd_setup_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("↩️ Cancelled. Instructions unchanged.")
    return ConversationHandler.END

# ── Free-text handler ──────────────────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    market_data = await data_market_snapshot()
    prompt = (
        f"LIVE MARKET CONTEXT:\n{market_data}\n\n"
        f"USER MESSAGE: {update.message.text.strip()}\n\n"
        "Respond as CIPHER — use live data above + analysis framework."
    )
    result = await call_claude(prompt, user_data.get("custom_instructions",""))
    await send_long(update, result)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception:", exc_info=context.error)
    if isinstance(update, Update) and update.message:
        await update.message.reply_text("⚠️ Error. Try again or check /help.")

# ── Keep-Alive Server (prevents Render free tier sleep) ───────────────────────
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading, time

RENDER_URL = os.getenv("RENDER_EXTERNAL_URL", "")  # Auto-set by Render

class PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"CIPHER alive")
    def log_message(self, *args):
        pass  # Suppress HTTP logs

def run_ping_server():
    port = int(os.getenv("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), PingHandler)
    logger.info(f"Keep-alive server on port {port}")
    server.serve_forever()

def self_ping_loop():
    """Ping own URL every 10 minutes to prevent Render sleep."""
    if not RENDER_URL:
        return
    time.sleep(60)  # Wait for server to start
    while True:
        try:
            import urllib.request
            urllib.request.urlopen(f"{RENDER_URL}/", timeout=10)
            logger.info("Self-ping OK")
        except Exception as e:
            logger.warning(f"Self-ping failed: {e}")
        time.sleep(600)  # Every 10 minutes

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    # Start keep-alive HTTP server in background thread (for Render)
    threading.Thread(target=run_ping_server, daemon=True).start()
    threading.Thread(target=self_ping_loop,  daemon=True).start()

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    setup_conv = ConversationHandler(
        entry_points=[CommandHandler("setup", cmd_setup_start)],
        states={WAITING_SETUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_setup_receive)]},
        fallbacks=[CommandHandler("cancel", cmd_setup_cancel)],
    )

    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("cipher",    cmd_cipher))
    app.add_handler(CommandHandler("btc",       cmd_btc))
    app.add_handler(CommandHandler("dominance", cmd_dominance))
    app.add_handler(CommandHandler("trending",  cmd_trending))
    app.add_handler(CommandHandler("defi",      cmd_defi))
    app.add_handler(CommandHandler("fear",      cmd_fear))
    app.add_handler(CommandHandler("etf",       cmd_etf))
    app.add_handler(CommandHandler("macro",     cmd_macro))
    app.add_handler(CommandHandler("watchlist", cmd_watchlist_view))
    app.add_handler(CommandHandler("ask",       cmd_ask))
    app.add_handler(setup_conv)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    async def post_init(application):
        await application.bot.set_my_commands([
            BotCommand("cipher",    "Full 30-min intelligence cycle"),
            BotCommand("btc",       "Deep BTC snapshot + analysis"),
            BotCommand("dominance", "BTC dominance + altcoin rotation"),
            BotCommand("trending",  "Trending coins + narrative analysis"),
            BotCommand("defi",      "DeFi TVL by protocol + chain"),
            BotCommand("fear",      "Fear & Greed + sentiment data"),
            BotCommand("etf",       "ETF + institutional proxy data"),
            BotCommand("macro",     "Macro event calendar"),
            BotCommand("watchlist", "Your tracked coins"),
            BotCommand("ask",       "Ask anything with live data"),
            BotCommand("setup",     "Set custom instructions"),
            BotCommand("help",      "All commands"),
        ])
    app.post_init = post_init

    logger.info("CIPHER Bot v3 starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
