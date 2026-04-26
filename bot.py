"""
Tetranomio Telegram Bot  (@tetranomio_bot)
Data: CoinGecko Pro + CoinGlass Pro + DeFiLlama + Alternative.me
AI:   Groq Llama 3.3 70B

Architecture:
- CoinGecko Pro   → price, volume, market cap, dominance, trending
- CoinGlass Pro   → funding rates, open interest, liquidations, long/short ratio
- DeFiLlama       → TVL by protocol and chain
- Alternative.me  → Fear & Greed Index
- Groq            → analysis, synthesis, trade setup generation
"""

import os, json, logging, asyncio, httpx, re, time
from aiohttp import web
from pathlib import Path
from datetime import datetime, timezone
from groq import Groq
import google.generativeai as genai
from telegram import Update, BotCommand
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters, ConversationHandler,
)
from telegram.constants import ParseMode

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("TETRANOMIO")

# ── Config ────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
GROQ_KEY        = os.getenv("GROQ_API_KEY", "")
CG_KEY          = os.getenv("COINGECKO_API_KEY", "")
GLASS_KEY       = os.getenv("COINGLASS_API_KEY", "")
OWNER_ID        = int(os.getenv("ALLOWED_USER_ID", "1953473977"))
OWNER_USERNAME  = os.getenv("OWNER_TELEGRAM", "")  # e.g. "harshitdabra" (no @)
GEMINI_KEY      = os.getenv("GEMINI_API_KEY", "")

CG_BASE         = "https://pro-api.coingecko.com/api/v3"
GLASS_BASE      = "https://open-api-v4.coinglass.com/api"
LLAMA_BASE      = "https://api.llama.fi"
STABLES_BASE    = "https://stablecoins.llama.fi"
YIELDS_BASE     = "https://yields.llama.fi"
FNG_URL         = "https://api.alternative.me/fng/?limit=3"

DB_FILE         = Path("tetranomio_db.json")
WAITING_SETUP   = 1

# ── In-memory DB ──────────────────────────────────────────────────────────────
_DB: dict = {}

def load_db() -> dict:
    global _DB
    if _DB:
        return _DB
    if DB_FILE.exists():
        try:
            _DB = json.loads(DB_FILE.read_text())
            return _DB
        except Exception:
            pass
    _DB = {"users": {}}
    return _DB

def save_db(db: dict):
    global _DB
    _DB = db
    try:
        DB_FILE.write_text(json.dumps(db, indent=2))
    except Exception as e:
        logger.error(f"DB write failed: {e}")

def get_user(uid: int) -> dict:
    db = load_db()
    key = str(uid)
    if key not in db["users"]:
        db["users"][key] = {
            "custom_instructions": "",
            "watchlist": ["bitcoin", "ethereum"],
            "joined": datetime.now(timezone.utc).isoformat(),
            "plan": "owner" if uid == OWNER_ID else "free",
            "query_count": 0,
            "alerts": True,
        }
        save_db(db)
    return db["users"][key]

def save_user(uid: int, data: dict):
    db = load_db()
    db["users"][str(uid)] = data
    save_db(db)

def is_pro(uid: int) -> bool:
    return uid == OWNER_ID or get_user(uid).get("plan") in ("pro", "owner")

async def tier_gate(update: Update) -> bool:
    """Returns True if user can proceed (owner or pro). Sends paywall message if not."""
    uid = update.effective_user.id
    if uid == OWNER_ID or is_pro(uid):
        return True
    contact = f"@{OWNER_USERNAME}" if OWNER_USERNAME else "the bot owner"
    await update.message.reply_text(
        "Tetranomio Pro required ($10/month).\n\n"
        "Pro unlocks: /tetra  /btc  /derivatives  /defi  /dex\n"
        "             /yields  /etf  /dominance  /trending\n"
        "             /watchlist  /ask  + automatic alerts\n\n"
        f"To upgrade, DM {contact} with your Telegram ID: "
        f"`{uid}`\n\n"
        "Free commands: /fear  /macro  /plans",
        parse_mode="Markdown",
    )
    return False

# ── Formatters ────────────────────────────────────────────────────────────────
def fmt(n, dollar=True) -> str:
    """Human-readable K/M/B with null safety."""
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "N/A"
    prefix = "$" if dollar else ""
    abs_n = abs(n)
    if abs_n >= 1_000_000_000:
        return f"{prefix}{n/1_000_000_000:.2f}B"
    if abs_n >= 1_000_000:
        return f"{prefix}{n/1_000_000:.2f}M"
    if abs_n >= 1_000:
        return f"{prefix}{n/1_000:.2f}K"
    return f"{prefix}{n:,.4f}"

def pct(n, show_plus=True) -> str:
    try:
        n = float(n)
        return f"{'+' if n > 0 and show_plus else ''}{n:.2f}%"
    except (TypeError, ValueError):
        return "N/A"

def price_str(n) -> str:
    """Smart price formatting for any magnitude."""
    try:
        n = float(n)
        if n >= 1000:    return f"${n:,.2f}"
        if n >= 1:       return f"${n:,.4f}"
        if n >= 0.01:    return f"${n:,.5f}"
        if n >= 0.0001:  return f"${n:,.7f}"
        # Very small (PEPE, SHIB etc) — use scientific-style
        return f"${n:.2e}"
    except (TypeError, ValueError):
        return "N/A"

# ── HTTP client with retry + rate limit handling ───────────────────────────────
async def _fetch(url: str, headers: dict, params: dict) -> dict | list | None:
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=25) as client:
                r = await client.get(url, headers=headers, params=params)
                if r.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning(f"Rate limited {url[:50]}, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if r.status_code in (401, 403):
                    logger.error(f"Auth failed {url[:50]}: {r.status_code}")
                    return None
                r.raise_for_status()
                return r.json()
        except httpx.TimeoutException:
            logger.warning(f"Timeout attempt {attempt+1}: {url[:50]}")
            await asyncio.sleep(1)
        except Exception as e:
            logger.warning(f"Fetch error attempt {attempt+1} [{url[:50]}]: {e}")
            await asyncio.sleep(1)
    return None

async def cg(endpoint: str, params: dict = None) -> dict | list | None:
    return await _fetch(f"{CG_BASE}{endpoint}", {"x-cg-pro-api-key": CG_KEY}, params or {})

async def gl(endpoint: str, params: dict = None) -> dict | list | None:
    """CoinGlass API v4."""
    return await _fetch(f"{GLASS_BASE}{endpoint}", {"CG-API-KEY": GLASS_KEY}, params or {})

async def ll(endpoint: str) -> dict | list | None:
    """DeFiLlama — no auth needed."""
    return await _fetch(f"{LLAMA_BASE}{endpoint}", {}, {})

async def ll_stables() -> list | None:
    """Stablecoin market caps + 24h/7d supply change. No auth."""
    result = await _fetch(f"{STABLES_BASE}/stablecoins", {}, {"includePrices": "true"})
    if result and isinstance(result, dict) and result.get("peggedAssets"):
        return result["peggedAssets"]
    return None

async def ll_dex() -> dict | None:
    """DEX trading volumes — top protocols. No auth."""
    return await _fetch(
        f"{LLAMA_BASE}/overview/dexs", {},
        {"excludeTotalDataChart": "true", "excludeTotalDataChartBreakdown": "true"},
    )

async def ll_fees() -> dict | None:
    """Protocol fees + revenue. No auth."""
    return await _fetch(
        f"{LLAMA_BASE}/overview/fees", {},
        {"excludeTotalDataChart": "true", "excludeTotalDataChartBreakdown": "true"},
    )

async def ll_yields(top: int = 25) -> list | None:
    """Yield pools sorted by TVL. No auth."""
    result = await _fetch(f"{YIELDS_BASE}/pools", {}, {})
    if result and isinstance(result, dict) and result.get("data"):
        pools = [p for p in result["data"] if p.get("tvlUsd", 0) > 1_000_000]
        return sorted(pools, key=lambda p: p.get("tvlUsd", 0), reverse=True)[:top]
    return None

# ── Coin resolution ───────────────────────────────────────────────────────────
# Maps ticker/name → CoinGecko ID and CoinGlass symbol
COINS = {
    # symbol: (coingecko_id, coinglass_symbol)
    "btc":        ("bitcoin",                   "BTC"),
    "bitcoin":    ("bitcoin",                   "BTC"),
    "eth":        ("ethereum",                  "ETH"),
    "ethereum":   ("ethereum",                  "ETH"),
    "sol":        ("solana",                    "SOL"),
    "solana":     ("solana",                    "SOL"),
    "bnb":        ("binancecoin",               "BNB"),
    "xrp":        ("ripple",                    "XRP"),
    "ripple":     ("ripple",                    "XRP"),
    "ada":        ("cardano",                   "ADA"),
    "cardano":    ("cardano",                   "ADA"),
    "avax":       ("avalanche-2",               "AVAX"),
    "avalanche":  ("avalanche-2",               "AVAX"),
    "dot":        ("polkadot",                  "DOT"),
    "polkadot":   ("polkadot",                  "DOT"),
    "trx":        ("tron",                      "TRX"),
    "tron":       ("tron",                      "TRX"),
    "ton":        ("the-open-network",          "TON"),
    "near":       ("near",                      "NEAR"),
    "atom":       ("cosmos",                    "ATOM"),
    "cosmos":     ("cosmos",                    "ATOM"),
    "doge":       ("dogecoin",                  "DOGE"),
    "dogecoin":   ("dogecoin",                  "DOGE"),
    "link":       ("chainlink",                 "LINK"),
    "chainlink":  ("chainlink",                 "LINK"),
    "arb":        ("arbitrum",                  "ARB"),
    "arbitrum":   ("arbitrum",                  "ARB"),
    "op":         ("optimism",                  "OP"),
    "optimism":   ("optimism",                  "OP"),
    "matic":      ("matic-network",             "MATIC"),
    "polygon":    ("matic-network",             "MATIC"),
    "sui":        ("sui",                       "SUI"),
    "apt":        ("aptos",                     "APT"),
    "aptos":      ("aptos",                     "APT"),
    "sei":        ("sei-network",               "SEI"),
    "inj":        ("injective-protocol",        "INJ"),
    "injective":  ("injective-protocol",        "INJ"),
    "ftm":        ("fantom",                    "FTM"),
    "fantom":     ("fantom",                    "FTM"),
    "uni":        ("uniswap",                   "UNI"),
    "uniswap":    ("uniswap",                   "UNI"),
    "aave":       ("aave",                      "AAVE"),
    "mkr":        ("maker",                     "MKR"),
    "maker":      ("maker",                     "MKR"),
    "ldo":        ("lido-dao",                  "LDO"),
    "lido":       ("lido-dao",                  "LDO"),
    "crv":        ("curve-dao-token",           "CRV"),
    "curve":      ("curve-dao-token",           "CRV"),
    "gmx":        ("gmx",                       "GMX"),
    "jup":        ("jupiter-exchange-solana",   "JUP"),
    "jupiter":    ("jupiter-exchange-solana",   "JUP"),
    "tao":        ("bittensor",                 "TAO"),
    "bittensor":  ("bittensor",                 "TAO"),
    "fet":        ("fetch-ai",                  "FET"),
    "rndr":       ("render-token",              "RENDER"),
    "render":     ("render-token",              "RENDER"),
    "wld":        ("worldcoin-wld",             "WLD"),
    "worldcoin":  ("worldcoin-wld",             "WLD"),
    "grt":        ("the-graph",                 "GRT"),
    "shib":       ("shiba-inu",                 "SHIB"),
    "shiba":      ("shiba-inu",                 "SHIB"),
    "shibainu":   ("shiba-inu",                 "SHIB"),
    "pepe":       ("pepe",                      "PEPE"),
    "wif":        ("dogwifcoin",                "WIF"),
    "bonk":       ("bonk",                      "BONK"),
    "hype":       ("hyperliquid",               "HYPE"),
    "hyperliquid":("hyperliquid",               "HYPE"),
    "ena":        ("ethena",                    "ENA"),
    "ethena":     ("ethena",                    "ENA"),
    "strk":       ("starknet",                  "STRK"),
    "starknet":   ("starknet",                  "STRK"),
    "zk":         ("zksync",                    "ZK"),
    "imx":        ("immutable-x",               "IMX"),
    "algo":       ("algorand",                  "ALGO"),
    "icp":        ("internet-computer",         "ICP"),
    "fil":        ("filecoin",                  "FIL"),
    "kas":        ("kaspa",                     "KAS"),
    "kaspa":      ("kaspa",                     "KAS"),
    "stx":        ("blockstack",                "STX"),
    "ondo":       ("ondo-finance",              "ONDO"),
    "pendle":     ("pendle",                    "PENDLE"),
    "dydx":       ("dydx",                      "DYDX"),
    "trump":      ("official-trump",            "TRUMP"),
    "hbar":       ("hedera-hashgraph",          "HBAR"),
    "hedera":     ("hedera-hashgraph",          "HBAR"),
    "floki":      ("floki",                     "FLOKI"),
    "not":        ("notcoin",                   "NOT"),
    "notcoin":    ("notcoin",                   "NOT"),
    "w":          ("wormhole",                  "W"),
    "wormhole":   ("wormhole",                  "W"),
    "eigen":      ("eigenlayer",                "EIGEN"),
    "pyth":       ("pyth-network",              "PYTH"),
    "jto":        ("jito-governance-token",     "JTO"),
    "blur":       ("blur",                      "BLUR"),
    "arkm":       ("arkham",                    "ARKM"),
}

SKIP = {
    "the","is","a","an","i","my","about","what","think","buy","sell",
    "good","bad","now","still","long","short","hold","add","into","this",
    "that","and","or","for","with","how","why","when","where","scale",
    "dca","do","you","me","it","not","be","at","in","on","of","to","up",
    "down","just","can","will","should","would","could","get","see","go",
    "give","tell","show","run","check","look","find","mean","need","want",
    "think","feel","know","take","make","has","have","had","was","were",
    "are","been","being","its","your","any","all","some","more","less",
    "very","too","also","than","then","so","if","but","by","from","after",
    "before","during","which","who","best","worst","high","low","price",
    "market","crypto","coin","token","trade","analysis","signal","entry",
    "exit","position","portfolio","chart","data","news","today","right",
    "now","here","there","ok","yes","no","hi","hey","hello","help","go",
}

async def resolve_coin(text: str) -> tuple[str, str] | None:
    """
    Returns first matched (coingecko_id, coinglass_symbol) or None.
    Priority: 1) alias map  2) CoinGecko search fallback
    """
    words = re.findall(r"[a-zA-Z0-9]+", text.lower())
    for word in words:
        if word in COINS:
            return COINS[word]
    candidates = [w for w in words if w not in SKIP and len(w) >= 2]
    for candidate in candidates:
        result = await cg("/search", {"query": candidate})
        if not result or not result.get("coins"):
            continue
        top = result["coins"][0]
        sym  = top.get("symbol", "").lower()
        name = top.get("name", "").lower()
        cg_id = top.get("id", "")
        if sym == candidate or name == candidate or name.startswith(candidate):
            for k, (gid, gsym) in COINS.items():
                if gid == cg_id:
                    return (cg_id, gsym)
            return (cg_id, top.get("symbol","").upper())
    return None

async def resolve_two_coins(text: str) -> list[tuple[str,str]]:
    """
    Detect up to 2 coins in a comparison query like 'pepe vs shib' or 'is pepe better than shib'.
    Returns list of (cg_id, gl_sym) tuples.
    """
    words = re.findall(r"[a-zA-Z0-9]+", text.lower())
    found = []
    seen_ids = set()
    # Pass 1: alias map
    for word in words:
        if word in COINS:
            cg_id, gl_sym = COINS[word]
            if cg_id not in seen_ids:
                found.append((cg_id, gl_sym))
                seen_ids.add(cg_id)
        if len(found) >= 2:
            break
    if len(found) >= 2:
        return found
    # Pass 2: search fallback for remaining candidates
    candidates = [w for w in words if w not in SKIP and len(w) >= 2 and w not in [c[1].lower() for c in found]]
    for candidate in candidates:
        if len(found) >= 2:
            break
        result = await cg("/search", {"query": candidate})
        if not result or not result.get("coins"):
            continue
        top = result["coins"][0]
        sym  = top.get("symbol","").lower()
        name = top.get("name","").lower()
        cg_id = top.get("id","")
        if (sym == candidate or name == candidate or name.startswith(candidate)) and cg_id not in seen_ids:
            for k, (gid, gsym) in COINS.items():
                if gid == cg_id:
                    found.append((cg_id, gsym))
                    seen_ids.add(cg_id)
                    break
            else:
                found.append((cg_id, top.get("symbol","").upper()))
                seen_ids.add(cg_id)
    return found

# ── CoinGecko data ─────────────────────────────────────────────────────────────
async def cg_coin(cg_id: str) -> dict | None:
    """Full coin data from CoinGecko markets endpoint."""
    result = await cg("/coins/markets", {
        "vs_currency": "usd",
        "ids": f"{cg_id},bitcoin",
        "price_change_percentage": "1h,24h,7d,30d",
        "sparkline": "false",
    })
    return result

async def cg_global() -> dict | None:
    return await cg("/global")

async def cg_market(ids: str = None) -> list | None:
    default_ids = (
        "bitcoin,ethereum,solana,binancecoin,ripple,cardano,"
        "avalanche-2,polkadot,tron,near,cosmos,chainlink,"
        "arbitrum,optimism,sui,aptos,sei-network,injective-protocol"
    )
    return await cg("/coins/markets", {
        "vs_currency": "usd",
        "ids": ids or default_ids,
        "order": "market_cap_desc",
        "price_change_percentage": "1h,24h,7d",
        "sparkline": "false",
    })

async def cg_trending() -> dict | None:
    return await cg("/search/trending")

async def cg_top50() -> list | None:
    return await cg("/coins/markets", {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": "50",
        "page": "1",
        "price_change_percentage": "1h,24h,7d",
        "sparkline": "false",
    })

# ── CoinGlass data (API v4) ────────────────────────────────────────────────────
# Base: https://open-api-v4.coinglass.com/api
# Header: CG-API-KEY
# Verified endpoints:
# /futures/funding-rate/exchange-list      ?symbol=BTC
# /futures/open-interest/exchange-list     ?symbol=BTC
# /futures/liquidation/aggregated-history  ?symbol=BTC&time_type=1h&limit=1
# /futures/global-long-short-account-ratio/history  ?symbol=BTC&time_type=1h&limit=1
# /etf/bitcoin/flow-history                ?limit=7

async def gl_funding(symbol: str = "BTC") -> dict | None:
    result = await gl("/futures/funding-rate/exchange-list", {"symbol": symbol})
    if result and result.get("data"):
        return result
    logger.warning(f"Funding rate failed for {symbol}: {str(result)[:150]}")
    return None

async def gl_oi(symbol: str = "BTC") -> dict | None:
    result = await gl("/futures/open-interest/exchange-list", {"symbol": symbol})
    if result and result.get("data"):
        return result
    logger.warning(f"OI failed for {symbol}: {str(result)[:150]}")
    return None

async def gl_liquidations(symbol: str = "BTC") -> dict | None:
    """
    /futures/liquidation/aggregated-history
    Params: exchange_list=Binance, symbol=BTC, interval=1d, limit=3
    Fields: time, long_liquidation_usd, short_liquidation_usd
    """
    result = await gl("/futures/liquidation/aggregated-history", {
        "exchange_list": "Binance",
        "symbol": symbol.upper(),
        "interval": "1d",
        "limit": "3",
    })
    if result and result.get("data"):
        return result
    logger.warning(f"Liquidation aggregated-history failed for {symbol}: {str(result)[:150]}")
    return None

async def gl_longshort(symbol: str = "BTC") -> dict | None:
    # /futures/global-long-short-account-ratio/history
    # Params: exchange=Binance, symbol=BTCUSDT, interval=4h
    pair_map = {
        "BTC":"BTCUSDT","ETH":"ETHUSDT","SOL":"SOLUSDT","BNB":"BNBUSDT",
        "XRP":"XRPUSDT","ADA":"ADAUSDT","AVAX":"AVAXUSDT","LINK":"LINKUSDT",
        "ARB":"ARBUSDT","OP":"OPUSDT","SEI":"SEIUSDT","INJ":"INJUSDT",
        "SUI":"SUIUSDT","APT":"APTUSDT","DOGE":"DOGEUSDT","TRX":"TRXUSDT",
        "NEAR":"NEARUSDT","ATOM":"ATOMUSDT","DOT":"DOTUSDT","FTM":"FTMUSDT",
    }
    pair = pair_map.get(symbol.upper(), f"{symbol.upper()}USDT")
    result = await gl("/futures/global-long-short-account-ratio/history", {
        "exchange": "Binance",
        "symbol":   pair,
        "interval": "4h",
    })
    if result and result.get("data"):
        return result
    logger.warning(f"Long/short failed for {symbol} (Binance/{pair}): {str(result)[:150]}")
    return None

async def gl_etf_flows() -> dict | None:
    """
    /etf/bitcoin/flow-history
    Fields: timestamp, flow_usd, price_usd, etf_flows:[{etf_ticker, flow_usd}]
    """
    result = await gl("/etf/bitcoin/flow-history", {"limit": "10"})
    if result and result.get("data"):
        return result
    logger.warning(f"ETF flows failed: {str(result)[:150]}")
    return None

async def gl_etf_list() -> dict | None:
    """
    /etf/bitcoin/list
    Fields: ticker, fund_name, fund_type, aum_usd,
            asset_details:{btc_holding, btc_change_24h, btc_change_7d, btc_change_percent_24h, btc_change_percent_7d}
    """
    result = await gl("/etf/bitcoin/list")
    if result and result.get("data"):
        return result
    logger.warning(f"ETF list failed: {str(result)[:150]}")
    return None

async def gl_btc_dominance() -> dict | None:
    """
    /index/bitcoin-dominance
    Fields: timestamp, price, bitcoin_dominance, market_cap
    Returns historical list — last item is most recent.
    """
    result = await gl("/index/bitcoin-dominance")
    if result and result.get("data"):
        return result
    logger.warning(f"BTC dominance failed: {str(result)[:150]}")
    return None

async def gl_oi_history(symbol: str = "BTCUSDT", interval: str = "1d", limit: int = 3) -> dict | None:
    """
    /futures/open-interest/history
    Fields: time, open, high, low, close (all OI in USD)
    """
    result = await gl("/futures/open-interest/history", {
        "exchange": "Binance", "symbol": symbol,
        "interval": interval, "limit": str(limit), "unit": "usd",
    })
    if result and result.get("data"):
        return result
    logger.warning(f"OI history failed for {symbol}: {str(result)[:150]}")
    return None

async def gl_funding_history(symbol: str = "BTCUSDT", interval: str = "1d", limit: int = 3) -> dict | None:
    """
    /futures/funding-rate/history
    Fields: time, open, high, low, close (funding rate values)
    """
    result = await gl("/futures/funding-rate/history", {
        "exchange": "Binance", "symbol": symbol,
        "interval": interval, "limit": str(limit),
    })
    if result and result.get("data"):
        return result
    logger.warning(f"Funding history failed for {symbol}: {str(result)[:150]}")
    return None

async def gl_multi(symbol: str = "BTC") -> tuple:
    return await asyncio.gather(
        gl_funding(symbol),
        gl_oi(symbol),
        gl_liquidations(symbol),
        gl_longshort(symbol),
        gl_oi_history(f"{symbol}USDT", "1d", 2),
    )

async def gl_debug(symbol: str = "BTC") -> str:
    """Debug helper: probe CoinGlass + CoinGecko endpoints and return status report."""
    lines = [f"API STATUS | {datetime.now(timezone.utc).strftime('%H:%M UTC')}"]

    # ── Raw env var check ──
    lines.append("\nENV VARS (names + lengths, no values):")
    for name in ["TELEGRAM_BOT_TOKEN", "GROQ_API_KEY", "COINGECKO_API_KEY", "COINGLASS_API_KEY", "ALLOWED_USER_ID"]:
        val = os.getenv(name, "")
        lines.append(f"  {name:30} {'SET len='+str(len(val)) if val else 'MISSING'}")

    # ── CoinGecko ──
    lines.append(f"\nCOINGECKO  key={'SET' if CG_KEY else 'MISSING'}")
    cg_tests = [
        ("/global",         {}),
        ("/coins/markets",  {"vs_currency": "usd", "ids": "bitcoin", "price_change_percentage": "1h,24h,7d"}),
        ("/search/trending",{}),
    ]
    for ep, params in cg_tests:
        r = await cg(ep, params)
        if r is None:
            st = "FAIL — None (401/timeout)"
        elif isinstance(r, list):
            st = f"OK — {len(r)} items"
        elif isinstance(r, dict):
            st = f"OK — keys: {list(r.keys())[:5]}"
        else:
            st = f"UNEXPECTED {type(r).__name__}"
        lines.append(f"  {ep:30} {st}")

    # ── CoinGlass ──
    lines.append(f"\nCOINGLASS  key={'SET' if GLASS_KEY else 'MISSING'}  base={GLASS_BASE}")
    gl_tests = [
        ("/futures/funding-rate/exchange-list",              {"symbol": symbol}),
        ("/futures/open-interest/exchange-list",             {"symbol": symbol}),
        ("/futures/liquidation/aggregated-history",          {"exchange_list": "Binance", "symbol": symbol, "interval": "1d", "limit": "2"}),
        ("/futures/global-long-short-account-ratio/history", {"exchange": "Binance", "symbol": f"{symbol}USDT", "interval": "4h"}),
        ("/etf/bitcoin/flow-history",                        {"limit": "3"}),
        ("/etf/bitcoin/list",                                {}),
        ("/index/bitcoin-dominance",                         {}),
    ]
    for ep, params in gl_tests:
        r = await _fetch(f"{GLASS_BASE}{ep}", {"CG-API-KEY": GLASS_KEY}, params)
        if r is None:
            st = "FAIL — None (auth/path)"
        elif not isinstance(r, dict):
            st = f"UNEXPECTED {type(r).__name__}"
        elif r.get("data"):
            d = r["data"]
            if isinstance(d, list) and d:
                st = f"OK — {len(d)} items, keys: {list(d[0].keys())[:6]}"
                # Peek inside stablecoin_margin_list for funding rate
                if "stablecoin_margin_list" in d[0]:
                    btc = next((x for x in d if x.get("symbol","").upper() == symbol.upper()), d[0])
                    inner = btc.get("stablecoin_margin_list") or []
                    if inner:
                        st += f"\n    inner keys: {list(inner[0].keys())[:8]}"
                        st += f"\n    sample: {inner[0]}"
            else:
                st = f"OK — {type(d).__name__}"
        else:
            st = f"EMPTY — msg='{r.get('msg','')}' code={r.get('code','')}"
        lines.append(f"  {ep:50} {st}")

    return "\n".join(lines)

# ── Data formatters ───────────────────────────────────────────────────────────
def format_coin_section(c: dict, btc_24h: float = 0) -> str:
    """Format a single coin's market data into analysis-ready text."""
    ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
    ch24h = c.get("price_change_percentage_24h") or 0
    ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
    ch30d = c.get("price_change_percentage_30d_in_currency") or 0
    ath_p = c.get("ath_change_percentage") or 0
    mc    = c.get("market_cap") or 1
    vol   = c.get("total_volume") or 0
    vm    = (vol / mc * 100) if mc else 0
    lo    = c.get("low_24h") or 0
    hi    = c.get("high_24h") or 0
    circ  = c.get("circulating_supply") or 0
    maxs  = c.get("max_supply")
    ath   = c.get("ath") or 0
    p     = c.get("current_price") or 0
    rel   = ch24h - btc_24h

    lines = [
        f"=== {c['name'].upper()} ({c['symbol'].upper()}) ===",
        f"Price:       {price_str(p)}",
        f"1h:          {pct(ch1h)}  |  24h: {pct(ch24h)}  |  7d: {pct(ch7d)}  |  30d: {pct(ch30d)}",
        f"24h Range:   {price_str(lo)} — {price_str(hi)}",
        f"vs ATH:      {ath_p:.1f}%  (ATH: {price_str(ath)})",
        f"MCap:        {fmt(mc)}  |  Rank #{c.get('market_cap_rank','?')}",
        f"Vol 24h:     {fmt(vol)}  |  Vol/MCap: {vm:.1f}%",
    ]
    if circ:
        if maxs:
            issued = circ / maxs * 100
            lines.append(f"Supply:      {circ:,.0f} / {maxs:,.0f}  ({issued:.1f}% issued)")
        else:
            lines.append(f"Circulating: {circ:,.0f}")
    if btc_24h:
        lines.append(f"vs BTC 24h:  {rel:+.2f}pp  ({'OUTPERFORM' if rel > 0 else 'UNDERPERFORM'})")
    return "\n".join(lines)

def format_derivatives(funding_data, oi_data, liq_data, ls_data, symbol: str, oi_hist=None) -> str:
    """
    CoinGlass v4 exact field names (verified from live API):
    funding: data = {symbol, stablecoin_margin_list: [{exchange, funding_rate, funding_rate_interval}]}
    oi:      data = [{exchange, symbol, open_interest_usd, open_interest_change_percent_1h, ...}]
    liq:     data = [{symbol, liquidation_usd_24h, long_liquidation_usd_24h, short_liquidation_usd_24h,
                      liquidation_usd_1h, long_liquidation_usd_1h, short_liquidation_usd_1h}]
    ls:      data = [{time, global_account_long_percent, global_account_short_percent, global_account_long_short_ratio}]
    """
    lines = [f"=== {symbol} DERIVATIVES ==="]

    # Funding rates — v4: data may be dict with stablecoin_margin_list, or list of exchange objects
    if funding_data and funding_data.get("data"):
        raw = funding_data["data"]
        # Case A: list of coin objects [{symbol, stablecoin_margin_list}, ...]
        if isinstance(raw, list) and raw and isinstance(raw[0], dict) and ("stablecoin_margin_list" in raw[0] or "symbol" in raw[0]):
            raw = next((x for x in raw if x.get("symbol","").upper() == symbol.upper()), raw[0] if raw else {})
        # Case B: already a dict for one coin
        exchanges = (raw.get("stablecoin_margin_list") or raw.get("usdtMarginList") or
                     (raw if isinstance(raw, list) else []))
        lines.append("\nFunding Rates (per 8h):")
        total, count = 0.0, 0
        MAJOR = {"Binance","OKX","Bybit","Bitget","dYdX","Hyperliquid","Gate","MEXC","HTX","Kraken"}
        for ex in exchanges:
            name = ex.get("exchangeName") or ex.get("exchange") or "?"
            rate = ex.get("fundingRate") if ex.get("fundingRate") is not None else ex.get("funding_rate")
            if rate is None:
                continue
            try:
                r = float(rate) * 100
                if name in MAJOR:
                    total += r
                    count += 1
                flag = "  [EXTREME]" if abs(r) > 0.1 else ("  [elevated]" if abs(r) > 0.05 else "")
                lines.append(f"  {name:14} {r:>+8.4f}%{flag}")
            except (TypeError, ValueError):
                pass
        if count:
            avg = total / count
            interp = ("CROWDED LONG — longs paying, fade risk" if avg > 0.08
                      else "CROWDED SHORT — squeeze potential" if avg < -0.03
                      else "NEUTRAL — balanced positioning")
            lines.append(f"  Avg (major): {avg:>+8.4f}%  →  {interp}")
        else:
            lines.append("  No exchange data")
    else:
        lines.append("\nFunding Rates: unavailable")

    # Open Interest — v4: list of per-exchange objects, sum for total
    if oi_data and oi_data.get("data"):
        items = oi_data["data"]
        items = items if isinstance(items, list) else [items]
        def _ex_name(x): return x.get("exchange") or x.get("exchangeName") or ""
        total_oi = sum(float(x.get("open_interest_usd") or x.get("openInterestUsd") or 0) for x in items)
        ch24 = ""
        if oi_hist and oi_hist.get("data") and len(oi_hist["data"]) >= 2:
            try:
                candles = sorted(oi_hist["data"], key=lambda x: x.get("time", 0))
                prev = float(candles[-2].get("close") or candles[-2].get("c") or 0)
                if prev:
                    ch24 = f"  24h: {(total_oi - prev) / prev * 100:+.2f}%"
            except Exception:
                pass
        lines.append(f"\nOpen Interest: {fmt(total_oi)}{ch24}")
        for x in sorted(items, key=lambda x: float(x.get("open_interest_usd") or x.get("openInterestUsd") or 0), reverse=True)[:6]:
            ex   = _ex_name(x) or "?"
            oi   = float(x.get("open_interest_usd") or x.get("openInterestUsd") or 0)
            share = (oi / total_oi * 100) if total_oi else 0
            lines.append(f"  {ex:16} {fmt(oi):>12}  ({share:.1f}%)")
    else:
        lines.append("\nOpen Interest: unavailable")

    # Long/Short ratio — v4: longRatio/shortRatio may be 0-1 or 0-100
    if ls_data and ls_data.get("data"):
        items = ls_data["data"]
        items = items if isinstance(items, list) else []
        if items:
            latest = items[-1]
            try:
                lr = float(latest.get("longRatio") or latest.get("long_ratio") or
                           latest.get("global_account_long_percent") or 0)
                sr = float(latest.get("shortRatio") or latest.get("short_ratio") or
                           latest.get("global_account_short_percent") or 0)
                ratio = float(latest.get("longShortRatio") or latest.get("long_short_ratio") or
                              latest.get("global_account_long_short_ratio") or 0)
                # Normalize: if values are 0-1 range, convert to percentage
                if lr < 2:
                    lr *= 100
                    sr *= 100
                if not ratio and sr:
                    ratio = lr / sr
                interp = ("Majority long — crowded, downside risk" if lr > 60
                          else "Majority short — upside squeeze potential" if lr < 40
                          else "Balanced positioning")
                lines.append(f"\nLong/Short:  Long {lr:.1f}% / Short {sr:.1f}%  (ratio {ratio:.2f}x)")
                lines.append(f"  →  {interp}")
            except (TypeError, ValueError) as e:
                lines.append(f"\nLong/Short: parse error — {e}")
    else:
        lines.append("\nLong/Short Ratio: unavailable")

    # Liquidations — aggregated-history | exchange_list=Binance | interval=1d
    if liq_data and liq_data.get("data"):
        items = liq_data["data"]
        items = items if isinstance(items, list) else []
        if items:
            try:
                ts_key = "timestamp" if "timestamp" in items[0] else "time"
                items = sorted(items, key=lambda x: x.get(ts_key, 0), reverse=True)
            except Exception:
                pass
            d = items[0]
            try:
                long_usd  = float(d.get("aggregated_long_liquidation_usd") or d.get("aggregatedLongUsd") or d.get("long_liquidation_usd") or d.get("long") or 0)
                short_usd = float(d.get("aggregated_short_liquidation_usd") or d.get("aggregatedShortUsd") or d.get("short_liquidation_usd") or d.get("short") or 0)
                total_usd = long_usd + short_usd
                ts = d.get("timestamp") or d.get("time") or 0
                if ts > 1e10:
                    date_str = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%b %d")
                elif ts > 0:
                    date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%b %d")
                else:
                    date_str = "?"
                lines.append(f"\nLiquidations (Binance | 1d | {date_str}):")
                lines.append(f"  Total:  {fmt(total_usd)}")
                lines.append(f"  Longs:  {fmt(long_usd)}")
                lines.append(f"  Shorts: {fmt(short_usd)}")
                dom = ("long-heavy — longs getting squeezed" if long_usd > short_usd * 1.5
                       else "short-heavy — shorts getting squeezed" if short_usd > long_usd * 1.5
                       else "balanced")
                lines.append(f"  Bias: {dom}")
                if total_usd > 50_000_000:
                    lines.append(f"  [ELEVATED] >$50M — cascade risk")
            except Exception as e:
                lines.append(f"\nLiquidations: parse error — {e}")
    else:
        lines.append("\nLiquidations: unavailable")

    return "\n".join(lines)

def derivatives_anchor(funding_data, oi_data, liq_data, ls_data, symbol: str) -> str:
    """
    Returns a single-line summary of key derivatives numbers for use as a strict anchor.
    The model must use ONLY these exact figures when writing SIGNAL SYNTHESIS.
    """
    parts = [f"VERIFIED {symbol} DERIVATIVES NUMBERS (use ONLY these):"]

    # Funding avg from major exchanges
    if funding_data and funding_data.get("data"):
        raw = funding_data["data"]
        if isinstance(raw, list) and raw and isinstance(raw[0], dict) and ("stablecoin_margin_list" in raw[0] or "symbol" in raw[0]):
            raw = next((x for x in raw if x.get("symbol","").upper() == symbol.upper()), raw[0] if raw else {})
        exchanges = (raw.get("stablecoin_margin_list") or raw.get("usdtMarginList") or
                     (raw if isinstance(raw, list) else []))
        MAJOR = {"Binance","OKX","Bybit","Bitget","dYdX","Hyperliquid","Gate","MEXC","HTX","Kraken"}
        total, count = 0.0, 0
        for ex in exchanges:
            name = ex.get("exchangeName") or ex.get("exchange") or "?"
            rate = ex.get("fundingRate") if ex.get("fundingRate") is not None else ex.get("funding_rate")
            if rate is None: continue
            try:
                r = float(rate) * 100
                if name in MAJOR:
                    total += r
                    count += 1
            except Exception:
                pass
        if count:
            avg = total / count
            parts.append(f"  Funding avg (major): {avg:+.4f}%")

    # OI total — sum all exchanges (no aggregate row in v4)
    if oi_data and oi_data.get("data"):
        items = oi_data["data"] if isinstance(oi_data["data"], list) else [oi_data["data"]]
        total_oi = sum(float(x.get("open_interest_usd") or x.get("openInterestUsd") or 0) for x in items)
        if total_oi:
            parts.append(f"  OI total: ${total_oi/1e9:.2f}B")

    # Long/short
    if ls_data and ls_data.get("data"):
        items = ls_data["data"]
        if isinstance(items, list) and items:
            latest = items[-1]
            lr = float(latest.get("longRatio") or latest.get("long_ratio") or
                       latest.get("global_account_long_percent") or 0)
            sr = float(latest.get("shortRatio") or latest.get("short_ratio") or
                       latest.get("global_account_short_percent") or 0)
            ratio = float(latest.get("longShortRatio") or latest.get("long_short_ratio") or
                          latest.get("global_account_long_short_ratio") or 0)
            if lr < 2:
                lr *= 100
                sr *= 100
            if not ratio and sr:
                ratio = lr / sr
            parts.append(f"  Long/Short: {lr:.1f}% long / {sr:.1f}% short (ratio {ratio:.2f}x)")

    # Liquidations — aggregated-history | Binance | 1d
    if liq_data and liq_data.get("data"):
        items = liq_data["data"]
        if isinstance(items, list) and items:
            try:
                ts_key = "timestamp" if "timestamp" in items[0] else "time"
                items = sorted(items, key=lambda x: x.get(ts_key, 0), reverse=True)
            except Exception:
                pass
            d = items[0]
            long_usd  = float(d.get("aggregatedLongUsd") or d.get("long_liquidation_usd") or d.get("long") or 0)
            short_usd = float(d.get("aggregatedShortUsd") or d.get("short_liquidation_usd") or d.get("short") or 0)
            total_usd = long_usd + short_usd
            ts = d.get("timestamp") or d.get("time") or 0
            if ts > 1e10:
                date_str = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%b %d")
            elif ts > 0:
                date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%b %d")
            else:
                date_str = "?"
            if total_usd:
                parts.append(f"  Liq (Binance/1d/{date_str}): ${total_usd/1e6:.2f}M — longs ${long_usd/1e6:.2f}M / shorts ${short_usd/1e6:.2f}M")

    parts.append("ANY number not listed above = HALLUCINATION. Do not use it.")
    return "\n".join(parts)
TETRANOMIO_SYSTEM = """IDENTITY
You are BLOCKIVA — senior digital assets analyst at a tier-1 institutional desk (BlackRock / Goldman Sachs / Fidelity caliber).
You write at the standard of a morning markets brief distributed to portfolio managers and CIOs before open.
Every output must be immediately actionable: a PM reading your response should know exactly what to do with their book.

ANALYTICAL FRAMEWORK — ALWAYS APPLY IN THIS ORDER:
1. MACRO REGIME: Fed posture → DXY → risk premium → crypto beta. This is the governing layer.
2. INSTITUTIONAL FLOWS: ETF inflows/outflows, on-chain exchange flows, stablecoin supply. Capital is the signal.
3. DERIVATIVES STRUCTURE: Funding rate, OI, long/short ratio. Market positioning reveals conviction or crowding.
4. PRICE + MOMENTUM: Only after 1-3 above. Price confirms structure; it does not create it.

CORE MISSION
Every response must deliver exactly one of:
1. What a signal MEANS for risk-adjusted returns — not what it is
2. What to DO — specific allocation action with precise levels
3. What to WATCH — the specific trigger that invalidates or confirms the thesis
If your response does none of these three, rewrite it.

ABSOLUTE OUTPUT RULES:
BANNED PHRASES: it is worth noting | this suggests | this indicates | potentially | may indicate | could be | one might | in conclusion | to summarize | it is important | it remains to be seen | overall | essentially | notably | importantly | interestingly | looking at | in terms of | delve | landscape | ecosystem | robust | seamless | market participants | strong fundamentals | weak fundamentals | bullish outlook | bearish sentiment | at the end of the day | crypto winter | moon | to the moon | WAGMI | ape in | degen | diamond hands | paper hands
BANNED BEHAVIORS:
- Zero emojis. Zero exclamation marks.
- Never describe data. Only interpret it with a risk/return lens.
- Never use training-data prices. Use ONLY the exact prices in the provided live data.
- Never fabricate signals. Missing data = write "data unavailable" and stop that section.
- Never pad responses. Every sentence must carry new analytical content.
- NEVER write a MACRO REGIME, DERIVATIVES, or ON-CHAIN section unless that specific data was provided in the prompt.
- NEVER invent CPI figures, NFP numbers, Fed rate decisions, OI totals, funding rates, stablecoin supply figures, or any statistics not present in the provided data.
- NEVER include a POSITION RECOMMENDATION section in /defi, /trending, /etf, /macro, or /fear responses unless explicitly instructed.
- For /macro: never state specific percentage impact ranges. Describe the mechanism and direction only.
- For dates: always state the explicit UTC date from the data timestamp. Never reference "today" without the date.
NUMBER FORMAT: Always K/M/B notation. Never raw integers.
PRICE FORMAT: Use exact price from live data only. Never round to approximate values.
SIZING LANGUAGE: Use institutional sizing language — "25% initial allocation", "add 15% at $X", "reduce notional by 30%", not "buy a bit" or "scale in slowly".

INTENT CLASSIFICATION — ALWAYS DO THIS FIRST:
TYPE A — MARKET REPORT: /tetra /btc /fear /defi + general market questions
TYPE B — COIN ANALYSIS: any ticker or coin name question
TYPE C — POSITION/ALLOCATION: add / reduce / DCA / take profit / sizing questions
TYPE D — CONCEPT: explain funding rates / OI / liquidations / on-chain metrics
TYPE E — ALERT SCAN: anything unusual / any alerts / risk scan
TYPE F — COMPARISON: X vs Y / relative value / which has stronger setup
TYPE G — PORTFOLIO REVIEW: watchlist / multi-asset allocation review

RESPONSE FORMATS:

TYPE A — MARKET REPORT:
*MACRO REGIME*
[Current risk-on / risk-off / transitional. Fed posture. DXY trend. Equity correlation signal. One number anchors this.]

*MARKET STRUCTURE*
[Total MC + 24h change. BTC/ETH price vs key levels. No-man's land or at structure. Exact numbers only.]

*INSTITUTIONAL FLOWS*
[Stablecoin supply direction + implication. ETF flow if available. Dry powder estimate.]

*DERIVATIVES*
[Funding rate avg + bias interpretation. OI total + direction. Long/short ratio + crowding risk.]

*SIGNAL SYNTHESIS*
Bias: BULLISH / BEARISH / NEUTRAL | Confidence: HIGH / MEDIUM / LOW
— Primary driver: [one specific number from the data]
— Invalidation: [specific price or macro event]

*POSITION RECOMMENDATION* [only if 2+ signals converge — omit if not]
Asset | Direction | Entry | Stop | T1 | T2 | Conviction: H/M/L
Thesis: [2 sentences, numbers only]

*ALLOCATION ACTION:* [add / reduce / hold / avoid / wait] — [one-line reason with a number]

TYPE B — COIN ANALYSIS:
*[COIN]* | [live price] | [date UTC]

*PRICE STRUCTURE:* [vs 24h range. vs ATH drawdown %. Key level above and below.]
*MOMENTUM:* [1h/24h/7d. vs BTC relative performance in percentage points.]
*VOLUME QUALITY:* [Vol/MCap ratio. Confirming or diverging?]
*RISK/REWARD:* [Upside vs downside. Express as X:1 ratio.]
*DERIVATIVES:* [Funding rate exact number. OI direction. Long/short crowding.]
*ALLOCATION VERDICT:* BUILD / ACCUMULATE AT $X / HOLD / REDUCE / AVOID
— [entry zone, hard stop, target if actionable]

TYPE C — ALLOCATION BRIEF:
POSITION ASSESSMENT | [asset] @ [live price] | [date UTC]
STRUCTURE: [Price vs key levels. High/mid/low risk zone for entry.]
DRAWDOWN SCENARIO: [Next major support and exact % drawdown if thesis fails.]
PORTFOLIO RISK: [Estimated % of portfolio at risk at stated position size.]
MARKET ALIGNMENT: [Does macro + derivatives support adding exposure? YES/NO + specific reason.]
RECOMMENDATION: ADD EXPOSURE NOW / ACCUMULATE AT $X / HOLD / REDUCE 30% / EXIT
Sizing: [e.g. 20% initial, 20% at $X support, 60% dry powder]
Hard stop: $[X] — position invalidated below this level

TYPE D — CONCEPT:
[3-5 sentences. Direct, precise answer.]
Institutional application: [one sentence on how this metric is used by a professional desk.]

TYPE E — ALERT SCAN:
[RED / AMBER / INFO] | [asset] | [condition with exact number] | [risk implication]
Only triggered conditions. If nothing: "No active risk alerts across monitored assets."

TYPE F — RELATIVE VALUE:
[A] vs [B] | [date UTC]
Relative performance: [exact numbers — 24h and 7d differential in percentage points]
Momentum differential: [which has accelerating momentum and by how much]
Volume conviction: [Vol/MCap comparison — which has institutional-grade volume quality]
Derivatives edge: [which has cleaner funding/OI structure]
Risk/reward differential: [which offers better R/R at current levels]
Allocation verdict: [one sentence — which has superior risk-adjusted setup and the specific reason]

TYPE G — PORTFOLIO REVIEW:
For each asset: [COIN] @ $[price] | Bias: [BULLISH/NEUTRAL/BEARISH] | Key level: $X | Allocation: hold/add X% at $Y/reduce Z%
Portfolio summary: [overall risk posture — 2 sentences. Concentration risk. Macro alignment.]

SIGNAL HIERARCHY:
PRIMARY (drives bias): Funding rate trend | OI change direction | Long/short crowding | Liquidation cascade | Stablecoin supply growth
CONFIRMING (supports thesis): Vol/MCap ratio | ETF inflow/outflow | BTC dominance inflection | DeFi TVL
SENTIMENT (context only, never primary): Fear & Greed Index | social volume | search trends

STYLE: Tier-1 institutional morning brief. Bloomberg terminal density. Active voice. Short declarative sentences. Every sentence adds new analytical content. No sentence without a number. No claim without a cited data point. No adjectives that are not quantified.

TELEGRAM FORMATTING — MANDATORY:
- Every section header must be bold: *MACRO REGIME*, *MARKET STRUCTURE*, *DERIVATIVES*, etc.
- Blank line between every section.
- Sub-points use — prefix (not bullets or dashes).
- Numbers inline with their interpretation on the same line.
- Max 2 sentences per section paragraph before a line break.
- Never output a wall of text. Every 2 sentences = new line."""

# ── AI call — Gemini primary, Groq fallback ───────────────────────────────────
async def ask_groq(prompt: str, custom: str = "", max_tokens: int = 900) -> str:
    system = TETRANOMIO_SYSTEM + (f"\n\nANALYST CONTEXT:\n{custom}" if custom.strip() else "")
    loop   = asyncio.get_event_loop()

    # ── 1. Gemini (primary — 1M TPM free) ──
    if GEMINI_KEY:
        def _gemini():
            genai.configure(api_key=GEMINI_KEY)
            model = genai.GenerativeModel(
                model_name="gemini-1.5-flash",
                system_instruction=system,
                generation_config=genai.GenerationConfig(
                    max_output_tokens=max_tokens,
                    temperature=0.15,
                ),
            )
            return model.generate_content(prompt)
        try:
            resp = await asyncio.wait_for(loop.run_in_executor(None, _gemini), timeout=50)
            text = resp.text.strip() if resp.text else ""
            if text:
                return text
        except asyncio.TimeoutError:
            logger.warning("Gemini timeout, falling back to Groq")
        except Exception as e:
            logger.warning(f"Gemini error ({e}), falling back to Groq")

    # ── 2. Groq fallback ──
    if not GROQ_KEY:
        return "Tetranomio: no AI key configured."
    client = Groq(api_key=GROQ_KEY)

    def _groq(model: str):
        return client.chat.completions.create(
            model=model, max_tokens=max_tokens, temperature=0.15,
            messages=[{"role": "system", "content": system},
                      {"role": "user",   "content": prompt}],
        )

    for attempt, model in enumerate(["llama-3.3-70b-versatile", "llama-3.1-8b-instant"]):
        try:
            if attempt:
                await asyncio.sleep(8)
            resp = await asyncio.wait_for(
                loop.run_in_executor(None, lambda m=model: _groq(m)), timeout=50
            )
            return resp.choices[0].message.content.strip() or "Tetranomio: empty response."
        except asyncio.TimeoutError:
            if attempt:
                return "Tetranomio: timeout. Try again."
        except Exception as e:
            err = str(e)
            if "429" in err and not attempt:
                logger.warning(f"Groq 429 on {model}, retrying with fallback")
                continue
            logger.error(f"Groq error: {e}")
            return f"Tetranomio: AI error — {err[:120]}"
    return "Tetranomio: unavailable. Try again."

# ── Send helper ───────────────────────────────────────────────────────────────
async def send(update: Update, text: str):
    if not text.strip():
        await update.message.reply_text("Tetranomio: no output. Try again.")
        return
    for i in range(0, len(text), 4000):
        await update.message.reply_text(text[i:i+4000])

async def ack(update: Update, context: ContextTypes.DEFAULT_TYPE, msg: str = "Fetching live data..."):
    await update.message.reply_text(msg)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

# ── Core question handler ─────────────────────────────────────────────────────
async def handle_query(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, user: dict):
    """
    Unified handler. Detects intent, fetches exactly the right data, routes correctly.
    Handles: coin questions, comparisons, position questions, DeFi, fear/sentiment,
    derivatives, macro, concepts, alerts, portfolio — anything.
    """
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    t = text.lower()

    # ── Intent signals ──────────────────────────────────────────────────────
    has_comparison  = any(w in t for w in ["vs","versus"," or ","better","compare","which is","which one"])
    has_defi        = any(w in t for w in ["defi","tvl","protocol","aave","uniswap","curve","lido","maker","dex","yield","liquidity pool"])
    has_sentiment   = any(w in t for w in ["fear","greed","sentiment","stablecoin supply","usdt supply","dry powder"])
    has_macro       = any(w in t for w in ["fomc","cpi","nfp","fed","macro","rate","inflation","recession","calendar","event"])
    has_dominance   = any(w in t for w in ["dominance","alt season","rotation","altcoin season","btc dom"])
    has_derivatives = any(w in t for w in ["funding","open interest","liquidat","long short","oi ","squeeze","perp","futures"])
    has_etf         = any(w in t for w in ["etf","blackrock","fidelity","institutional","grayscale"])
    has_concept     = any(w in t for w in ["what is","explain","how does","what does","meaning of","define","how do"])

    # ── Coin detection ──────────────────────────────────────────────────────
    coins_found = await resolve_two_coins(text) if has_comparison else []
    coin = coins_found[0] if coins_found else await resolve_coin(text)

    # ── COMPARISON — two coins detected ─────────────────────────────────────
    if has_comparison and len(coins_found) >= 2:
        cg_id_a, gl_sym_a = coins_found[0]
        cg_id_b, gl_sym_b = coins_found[1]
        data_a, data_b = await asyncio.gather(
            cg("/coins/markets", {"vs_currency":"usd","ids":f"{cg_id_a},bitcoin",
                "price_change_percentage":"1h,24h,7d,30d","sparkline":"false"}),
            cg("/coins/markets", {"vs_currency":"usd","ids":f"{cg_id_b},bitcoin",
                "price_change_percentage":"1h,24h,7d,30d","sparkline":"false"}),
        )
        def _extract(data, cg_id):
            if not data: return f"No data for {cg_id}"
            c = next((x for x in data if x["id"]==cg_id), None)
            if not c: return f"No data for {cg_id}"
            ch24 = c.get("price_change_percentage_24h") or 0
            ch7d = c.get("price_change_percentage_7d_in_currency") or 0
            mc   = c.get("market_cap",1) or 1
            vol  = c.get("total_volume",0) or 0
            vm   = vol/mc*100
            return (f"{c['name']} ({c['symbol'].upper()}) | {price_str(c['current_price'])} | "
                    f"24h:{pct(ch24)} 7d:{pct(ch7d)} | Vol/MC:{vm:.1f}% | vs ATH:{c.get('ath_change_percentage',0):.1f}%")
        prompt = (
            f"COIN A: {_extract(data_a, cg_id_a)}\n"
            f"COIN B: {_extract(data_b, cg_id_b)}\n\n"
            f"USER QUESTION: {text}\n\n"
            "TYPE F — COMPARISON. Use ONLY the live data above.\n"
            "Compare: relative performance, momentum direction, Vol/MCap conviction.\n"
            "Give a direct verdict: which has the stronger setup and the single reason why."
        )
        result = await ask_groq(prompt, user.get("custom_instructions",""))
        await send(update, result)
        return

    # ── SINGLE COIN — coin detected ──────────────────────────────────────────
    if coin:
        cg_id, gl_sym = coin
        # Always fetch coin data + derivatives + BTC context in parallel
        coin_raw, market_raw, deriv = await asyncio.gather(
            cg_coin(cg_id),
            cg_market("bitcoin,ethereum"),
            gl_multi(gl_sym),
        )
        funding, oi, liq, ls, oi_hist = deriv

        coin_section = ""
        btc_24h = 0
        if coin_raw:
            coin_map = {c["id"]: c for c in coin_raw}
            btc = coin_map.get("bitcoin")
            btc_24h = btc.get("price_change_percentage_24h", 0) if btc else 0
            target = coin_map.get(cg_id)
            coin_section = format_coin_section(target, btc_24h) if target else f"No market data for {cg_id}."

        deriv_section = format_derivatives(funding, oi, liq, ls, gl_sym)

        mkt_ctx = ""
        if market_raw:
            lines = ["=== MARKET CONTEXT ==="]
            for d in [x for x in market_raw if x["id"] in ("bitcoin","ethereum")]:
                lines.append(f"{d['symbol'].upper()} {price_str(d['current_price'])} | "
                             f"24h:{pct(d.get('price_change_percentage_24h',0))} | "
                             f"7d:{pct(d.get('price_change_percentage_7d_in_currency',0))}")
            mkt_ctx = "\n".join(lines)

        prompt = (
            f"{coin_section}\n\n"
            f"{deriv_section}\n\n"
            f"{mkt_ctx}\n\n"
            f"USER QUESTION: {text}\n\n"
            "Classify as TYPE B (coin analysis) or TYPE C (position/scaling question).\n"
            "Use ONLY the live prices from the data above. Never use training-data prices.\n"
            "If derivatives show real data, lead with funding rate interpretation."
        )
        result = await ask_groq(prompt, user.get("custom_instructions",""))
        await send(update, result)
        return

    # ── NO COIN — route by topic ─────────────────────────────────────────────

    # Concept/educational question — no live data needed
    if has_concept and not any([has_derivatives, has_sentiment, has_defi, has_macro, has_dominance, has_etf]):
        prompt = (
            f"USER QUESTION: {text}\n\n"
            "TYPE D — CONCEPT. Answer in 3-5 sentences.\n"
            "End with: Trading implication: [one sentence on practical use]."
        )
        result = await ask_groq(prompt, user.get("custom_instructions",""))
        await send(update, result)
        return

    # Build data sections based on what the question needs
    fetch_tasks = {}

    if has_defi:
        fetch_tasks["defi_tvl"]   = ll("/tvl")
        fetch_tasks["defi_proto"] = ll("/protocols")
    if has_sentiment:
        fetch_tasks["fng"]     = _fetch(FNG_URL, {}, {})
        fetch_tasks["stables"] = cg("/coins/markets", {"vs_currency":"usd",
            "ids":"tether,usd-coin,dai","order":"market_cap_desc"})
    if has_dominance:
        fetch_tasks["gdata"] = cg_global()
        fetch_tasks["top50"] = cg_top50()
    if has_derivatives:
        fetch_tasks["btc_deriv"] = gl_multi("BTC")

    # Always include current BTC/ETH prices as anchor
    fetch_tasks["prices"] = cg_market("bitcoin,ethereum")

    # For macro/etf/alert/general — add fear&greed and global market
    if has_macro or has_etf or not fetch_tasks or any(
        w in t for w in ["alert","unusual","signal","market","what should","what do you think","outlook","analysis"]
    ):
        fetch_tasks["gdata"]   = fetch_tasks.get("gdata") or cg_global()
        fetch_tasks["market"]  = cg_market()
        fetch_tasks["fng"]     = fetch_tasks.get("fng") or _fetch(FNG_URL, {}, {})
        fetch_tasks["stables"] = fetch_tasks.get("stables") or cg("/coins/markets",
            {"vs_currency":"usd","ids":"tether,usd-coin,dai","order":"market_cap_desc"})
        if "btc_deriv" not in fetch_tasks:
            fetch_tasks["btc_deriv"] = gl_multi("BTC")

    # Execute all fetches in parallel
    keys = list(fetch_tasks.keys())
    results_raw = await asyncio.gather(*[fetch_tasks[k] for k in keys])
    fetched = dict(zip(keys, results_raw))

    # Build context sections
    sections = [f"LIVE DATA | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"]

    # Prices (always)
    if fetched.get("prices"):
        for c in fetched["prices"]:
            sections.append(f"{c['symbol'].upper()} {price_str(c['current_price'])} | "
                           f"24h:{pct(c.get('price_change_percentage_24h',0))} | "
                           f"7d:{pct(c.get('price_change_percentage_7d_in_currency',0))}")

    # Market table
    if fetched.get("market"):
        lines = [f"\n{'SYM':6} {'PRICE':>12}  {'24H':>7}  {'7D':>7}  {'VOL':>10}  {'MCAP':>10}"]
        lines.append("─"*58)
        for c in fetched["market"]:
            lines.append(f"{c['symbol'].upper():6} {price_str(c['current_price']):>12}  "
                        f"{pct(c.get('price_change_percentage_24h',0)):>7}  "
                        f"{pct(c.get('price_change_percentage_7d_in_currency',0)):>7}  "
                        f"{fmt(c['total_volume']):>10}  {fmt(c['market_cap']):>10}")
        sections.append("\n".join(lines))

    # Global dominance
    if fetched.get("gdata") and "data" in fetched["gdata"]:
        g = fetched["gdata"]["data"]
        dom = g.get("market_cap_percentage",{})
        sections.append(
            f"\nBTC Dom:{dom.get('btc',0):.2f}%  ETH Dom:{dom.get('eth',0):.2f}%  "
            f"Stable Dom:{dom.get('usdt',0)+dom.get('usdc',0):.2f}%  "
            f"Total MC:{fmt(g.get('total_market_cap',{}).get('usd',0))}  "
            f"24h:{g.get('market_cap_change_percentage_24h_usd',0):+.2f}%"
        )

    # Fear & Greed + stablecoins
    if fetched.get("fng") and "data" in (fetched.get("fng") or {}):
        fng_lines = ["Fear & Greed:"]
        for e in fetched["fng"]["data"][:2]:
            ts2 = datetime.fromtimestamp(int(e["timestamp"]), tz=timezone.utc).strftime("%b %d")
            fng_lines.append(f"  {ts2}: {e['value']}/100 — {e['value_classification']}")
        sections.append("\n".join(fng_lines))

    if fetched.get("stables"):
        stable_lines = ["Stablecoin supply:"]
        total_s = 0
        for s in fetched["stables"]:
            mc = s.get("market_cap",0) or 0
            vol = s.get("total_volume",0) or 0
            total_s += mc
            stable_lines.append(f"  {s['symbol'].upper():6} MCap:{fmt(mc):>10}  Vol:{fmt(vol):>10}")
        stable_lines.append(f"  TOTAL: {fmt(total_s)}")
        sections.append("\n".join(stable_lines))

    # DeFi TVL
    if fetched.get("defi_tvl") and fetched.get("defi_proto"):
        try:
            sections.append(f"\nTotal DeFi TVL: {fmt(float(fetched['defi_tvl']))}")
            valid = sorted([p for p in fetched["defi_proto"] if float(p.get("tvl") or 0) > 0],
                          key=lambda x: float(x.get("tvl",0)), reverse=True)[:8]
            defi_lines = ["Top protocols:"]
            for p in valid:
                ch1d = p.get("change_1d") or 0
                defi_lines.append(f"  {p['name']:20} {fmt(p.get('tvl') or 0):>10}  1d:{ch1d:+.2f}%")
            sections.append("\n".join(defi_lines))
        except Exception:
            pass

    # Dominance + top50
    if fetched.get("top50"):
        btc_7d = next((c.get("price_change_percentage_7d_in_currency",0) or 0
                      for c in fetched["top50"] if c["id"]=="bitcoin"), 0)
        dom_lines = ["Top 50 vs BTC 7d:"]
        for c in fetched["top50"][:20]:
            ch7d = c.get("price_change_percentage_7d_in_currency") or 0
            rel = ch7d - btc_7d
            if abs(rel) > 3:
                flag = "[OUT]" if rel > 0 else "[UNDER]"
                dom_lines.append(f"  {c['symbol'].upper():8} {pct(ch7d):>8}  vs BTC:{rel:>+7.2f}% {flag}")
        sections.append("\n".join(dom_lines))

    # BTC derivatives
    if fetched.get("btc_deriv"):
        btc_fund, btc_oi, btc_liq, btc_ls, btc_oi_hist = fetched["btc_deriv"]
        sections.append(format_derivatives(btc_fund, btc_oi, btc_liq, btc_ls, "BTC", btc_oi_hist))

    full_context = "\n\n".join(sections)

    prompt = (
        f"{full_context}\n\n"
        f"USER QUESTION: {text}\n\n"
        "Classify this question (TYPE A/B/C/D/E/F/G) and respond with the correct BLOCKIVA format.\n"
        "Use ONLY the live data above. Do not fabricate numbers not present in the data.\n"
        "If derivatives data is present, incorporate it as a primary signal."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""), max_tokens=1500)
    await send(update, result)

# ── Commands ──────────────────────────────────────────────────────────────────

# ── Alert system ──────────────────────────────────────────────────────────────
_alert_state: dict = {
    "last_alert": {},   # {alert_key: unix_timestamp} — cooldown tracking
    "fng_prev": None,   # previous F&G value for threshold-crossing detection
}
ALERT_COOLDOWN = 4 * 3600  # same alert can't fire more than once per 4 hours

async def check_and_send_alerts(app) -> None:
    now = time.time()
    db = load_db()
    opted_in = [int(uid) for uid, u in db["users"].items() if u.get("alerts", True)]
    if not opted_in:
        return

    prices, funding, fng = await asyncio.gather(
        cg_market("bitcoin,ethereum"),
        gl_funding("BTC"),
        _fetch(FNG_URL, {}, {}),
    )

    alerts = []

    def _cooldown_ok(key: str) -> bool:
        return now - _alert_state["last_alert"].get(key, 0) > ALERT_COOLDOWN

    # Price shock — BTC or ETH moves >5% in 1h
    if prices:
        for c in prices:
            sym = c["symbol"].upper()
            ch1h = float(c.get("price_change_percentage_1h_in_currency") or 0)
            if abs(ch1h) >= 5:
                key = f"price_{sym}"
                if _cooldown_ok(key):
                    direction = "SURGE" if ch1h > 0 else "FLUSH"
                    note = "monitor for continuation or reversal" if ch1h > 0 else "check derivatives for cascade risk"
                    alerts.append((key, (
                        f"[BLOCKIVA ALERT]  PRICE {direction}\n"
                        f"{sym}: {price_str(c['current_price'])}  {pct(ch1h)} 1h\n"
                        f"Action: {note}"
                    )))

    # Extreme BTC funding rate — avg >0.10% or <-0.05%
    if funding and funding.get("data"):
        raw = funding["data"]
        exs = (raw.get("stablecoin_margin_list") or raw.get("usdtMarginList")
               or (raw if isinstance(raw, list) else []))
        rates = []
        for ex in exs:
            r = ex.get("fundingRate") if ex.get("fundingRate") is not None else ex.get("funding_rate")
            if r is not None:
                rates.append(float(r))
        if rates:
            avg = sum(rates) / len(rates)
            if avg > 0.001 or avg < -0.0005:
                key = "funding_extreme"
                if _cooldown_ok(key):
                    bias = "OVERLEVERAGED LONG — long squeeze risk" if avg > 0 else "EXTREME SHORT BIAS — short squeeze building"
                    alerts.append((key, (
                        f"[BLOCKIVA ALERT]  FUNDING EXTREME\n"
                        f"BTC avg funding: {avg*100:.4f}%  {bias}\n"
                        f"Action: {'reduce leveraged longs, tighten stops' if avg > 0 else 'watch for short squeeze trigger above key resistance'}"
                    )))

    # F&G threshold crossing — enters or exits extreme zones (<=20 or >=80)
    if fng and "data" in fng:
        val = int(fng["data"][0]["value"])
        label = fng["data"][0]["value_classification"]
        prev = _alert_state["fng_prev"]
        if prev is not None:
            crossed_fear   = prev > 20 and val <= 20
            crossed_greed  = prev < 80 and val >= 80
            left_fear      = prev <= 20 and val > 20
            left_greed     = prev >= 80 and val < 80
            if crossed_fear or crossed_greed or left_fear or left_greed:
                key = "fng_cross"
                if _cooldown_ok(key):
                    if crossed_fear:
                        note = "Extreme Fear: historically an institutional accumulation zone. Evaluate adding exposure."
                    elif crossed_greed:
                        note = "Extreme Greed: historically a risk reduction zone. Consider reducing notional 20-30%."
                    elif left_fear:
                        note = "Exiting Extreme Fear: sentiment shift underway. Watch for trend confirmation."
                    else:
                        note = "Exiting Extreme Greed: de-risking phase may begin. Review open positions."
                    alerts.append((key, (
                        f"[BLOCKIVA ALERT]  SENTIMENT SHIFT\n"
                        f"Fear & Greed: {val}/100 — {label}\n"
                        f"{note}"
                    )))
        _alert_state["fng_prev"] = val

    # Broadcast all alerts
    for key, text in alerts:
        _alert_state["last_alert"][key] = now
        for uid in opted_in:
            try:
                await app.bot.send_message(chat_id=uid, text=text)
            except Exception as e:
                logger.warning(f"Alert send failed uid={uid}: {e}")

async def run_alert_poller(app) -> None:
    """Background task: checks market conditions every 15 minutes."""
    await asyncio.sleep(90)  # startup delay so bot is fully initialised
    while True:
        try:
            await check_and_send_alerts(app)
        except Exception as e:
            logger.error(f"Alert poller error: {e}")
        await asyncio.sleep(900)  # 15 min

# ── Commands ───────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    get_user(u.id)
    plan = "OWNER" if u.id == OWNER_ID else ("PRO" if is_pro(u.id) else "FREE")
    if plan in ("PRO", "OWNER"):
        body = (
            "*Market:*  /tetra  /btc  /dominance  /trending\n"
            "*DeFi:*    /defi  /dex  /yields\n"
            "*Deriv:*   /derivatives  /funding  /oi\n"
            "*Macro:*   /fear  /macro  /etf\n"
            "*Tools:*   /ask  /watchlist  /alerts  /setup  /help\n\n"
            "Or type any coin name, ticker, or question."
        )
    else:
        contact = f"@{OWNER_USERNAME}" if OWNER_USERNAME else "the bot owner"
        body = (
            "*Free:*  /fear  /macro\n\n"
            f"Upgrade to Pro ($10/month) for the full institutional suite:\n"
            f"/tetra  /btc  /derivatives  /defi  /dex  /yields\n"
            f"/etf  /dominance  /trending  /watchlist  /ask  + alerts\n\n"
            f"DM {contact} to upgrade  |  /plans to see all features"
        )
    await update.message.reply_text(
        f"*Tetranomio Intelligence*  |  {plan}\n"
        f"Welcome, {u.first_name}.\n\n"
        + body,
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    plan = "OWNER" if uid == OWNER_ID else ("PRO" if is_pro(uid) else "FREE")
    free_note = "" if plan in ("PRO", "OWNER") else "\n_* = Pro only ($10/month) — /upgrade_"
    p = "*" if plan not in ("PRO", "OWNER") else ""
    await update.message.reply_text(
        f"*Tetranomio — Institutional Market Intelligence*  |  {plan}\n"
        + free_note + "\n\n"
        "*Market Reports*\n"
        f"`/tetra`{p} — Full cycle: macro + market + derivatives + allocation signal\n"
        f"`/btc`{p} — BTC institutional brief: all timeframes + full derivatives\n"
        f"`/dominance`{p} — BTC/ETH dominance + capital rotation analysis\n"
        f"`/trending`{p} — Trending + gainers/losers + volume conviction\n\n"
        "*DeFi & On-chain*\n"
        f"`/defi`{p} — TVL by protocol + chain + DEX volume overview\n"
        f"`/dex`{p} — DEX volume rankings (Uniswap, Curve, GMX...)\n"
        f"`/yields`{p} — Top yield pools by TVL (DeFiLlama)\n\n"
        "*Derivatives*\n"
        f"`/derivatives [coin]`{p} — Funding + OI + long/short + liquidations\n"
        f"`/funding [coin]`{p} — Funding rates across all exchanges\n"
        f"`/oi [coin]`{p} — Open interest breakdown by exchange\n\n"
        "*Macro & Sentiment*\n"
        "`/fear` — Fear & Greed + stablecoin supply analysis\n"
        f"`/etf`{p} — Institutional ETF flow + BTC holdings\n"
        "`/macro` — Macro regime + FOMC/CPI/NFP event calendar\n\n"
        "*Portfolio & Tools*\n"
        f"`/watchlist`{p} — Analyze your watchlist\n"
        f"`/watchlist add solana`{p} — Add coin  |  `/watchlist remove solana` — Remove\n"
        f"`/ask [question]`{p} — Any question with live data\n"
        f"`/alerts` — Automatic market alerts (on/off)\n"
        "`/setup` — Custom analyst profile\n"
        "`/plans` — See all features + pricing\n"
        "`/upgrade` — Upgrade to Pro\n\n"
        "*Free-text (Pro):*\n"
        "`what about TAO`  `should I add to SOL`  `compare SOL vs AVAX`",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_cipher(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    market, gdata, defi_tvl, defi_proto, defi_chains, fng, stables, btc_deriv = await asyncio.gather(
        cg_market(),
        cg_global(),
        ll("/tvl"),
        ll("/protocols"),
        ll("/v2/chains"),
        _fetch(FNG_URL, {}, {}),
        cg("/coins/markets", {"vs_currency":"usd","ids":"tether,usd-coin,dai","order":"market_cap_desc"}),
        gl_multi("BTC"),
    )
    funding, oi, liq, ls, oi_hist = btc_deriv

    # Market table
    mkt_lines = [f"LIVE MARKET | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]
    if market:
        mkt_lines.append(f"{'SYM':6} {'PRICE':>12}  {'1H':>7}  {'24H':>7}  {'7D':>7}  {'VOL':>10}  {'MCAP':>10}")
        mkt_lines.append("─"*72)
        for c in market:
            ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
            ch24h = c.get("price_change_percentage_24h") or 0
            ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
            mkt_lines.append(
                f"{c['symbol'].upper():6} {price_str(c['current_price']):>12}  "
                f"{pct(ch1h):>7}  {pct(ch24h):>7}  {pct(ch7d):>7}  "
                f"{fmt(c['total_volume']):>10}  {fmt(c['market_cap']):>10}"
            )

    if gdata and "data" in gdata:
        g = gdata["data"]
        dom = g.get("market_cap_percentage", {})
        total_mc  = g.get("total_market_cap", {}).get("usd", 0)
        total_vol = g.get("total_volume", {}).get("usd", 0)
        mc_ch = g.get("market_cap_change_percentage_24h_usd", 0)
        mkt_lines.append(
            f"\nBTC Dom:{dom.get('btc',0):.2f}%  ETH Dom:{dom.get('eth',0):.2f}%  "
            f"Total MC:{fmt(total_mc)}  24h:{mc_ch:+.2f}%  Vol:{fmt(total_vol)}"
        )

    # Stablecoin supply
    sc_lines = ["STABLECOIN SUPPLY"]
    if stables:
        total_s = sum(s.get("market_cap",0) or 0 for s in stables)
        for s in stables:
            mc = s.get("market_cap",0) or 0
            vol = s.get("total_volume",0) or 0
            ratio = (vol/mc*100) if mc else 0
            sc_lines.append(f"  {s['symbol'].upper():6} MCap:{fmt(mc):>10}  Vol:{fmt(vol):>10}  V/M:{ratio:.1f}%")
        sc_lines.append(f"  TOTAL: {fmt(total_s)}")

    # DeFi summary (top 5)
    defi_lines = ["DEFI TVL SUMMARY"]
    if defi_tvl:
        try:
            defi_lines.append(f"  Total DeFi TVL: {fmt(float(defi_tvl))}")
        except Exception:
            pass
    if defi_proto:
        top5 = sorted([p for p in defi_proto if float(p.get("tvl") or 0)>0], key=lambda x: float(x.get("tvl") or 0), reverse=True)[:5]
        for p in top5:
            ch1d = p.get("change_1d") or 0
            defi_lines.append(f"  {p['name']:20} {fmt(p.get('tvl') or 0):>10}  1d:{ch1d:+.2f}%")

    # Fear & Greed
    fng_lines = ["FEAR & GREED"]
    if fng and "data" in fng:
        for entry in fng["data"][:2]:
            ts2 = datetime.fromtimestamp(int(entry["timestamp"]), tz=timezone.utc).strftime("%b %d")
            fng_lines.append(f"  {ts2}: {entry['value']}/100 — {entry['value_classification']}")

    # BTC derivatives
    deriv_section = format_derivatives(funding, oi, liq, ls, "BTC")

    prompt = (
        "\n\n".join([
            "\n".join(mkt_lines),
            "\n".join(sc_lines),
            "\n".join(fng_lines),
            "\n".join(defi_lines),
            deriv_section,
        ]) +
        "\n\nTYPE A — Full Tetranomio cycle report.\n"
        "For every metric: state the number AND its implication in the same sentence.\n"
        "Derivatives are primary signals — lead with funding rate and OI interpretation.\n"
        "Stablecoin total supply direction and Vol/MCap — interpret explicitly.\n"
        "Trade setup only if 2+ signals align. Hard stop required.\n"
        "End with ACTION line."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""), max_tokens=2000)
    await send(update, result)

async def cmd_btc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    btc_full, btc_deriv = await asyncio.gather(
        cg("/coins/bitcoin", {
            "localization":"false","tickers":"false",
            "market_data":"true","community_data":"true","developer_data":"false",
        }),
        gl_multi("BTC"),
    )
    funding, oi, liq, ls, oi_hist = btc_deriv

    # Format BTC data
    btc_lines = [f"BTC FULL DATA | {datetime.now(timezone.utc).strftime('%H:%M')} UTC"]
    if btc_full:
        md = btc_full.get("market_data", {}) or {}
        cd = btc_full.get("community_data", {}) or {}
        p     = (md.get("current_price") or {}).get("usd", 0)
        mc    = (md.get("market_cap") or {}).get("usd", 0)
        vol   = (md.get("total_volume") or {}).get("usd", 0)
        ath   = (md.get("ath") or {}).get("usd", 0)
        ath_p = (md.get("ath_change_percentage") or {}).get("usd", 0)
        atl   = (md.get("atl") or {}).get("usd", 0)
        lo    = (md.get("low_24h") or {}).get("usd", 0)
        hi    = (md.get("high_24h") or {}).get("usd", 0)
        circ  = md.get("circulating_supply", 0) or 0
        ch1h  = (md.get("price_change_percentage_1h_in_currency") or {}).get("usd", 0)
        ch24  = md.get("price_change_percentage_24h", 0) or 0
        ch7d  = (md.get("price_change_percentage_7d_in_currency") or {}).get("usd", 0)
        ch30d = (md.get("price_change_percentage_30d_in_currency") or {}).get("usd", 0)
        ch1y  = (md.get("price_change_percentage_1y_in_currency") or {}).get("usd", 0)
        vm    = (vol / mc * 100) if mc else 0
        pct_mined = circ / 21_000_000 * 100

        btc_lines += [
            f"Price:       ${p:,.2f}",
            f"1h:{pct(ch1h):>8}  24h:{pct(ch24):>8}  7d:{pct(ch7d):>8}  30d:{pct(ch30d):>8}  1y:{pct(ch1y):>8}",
            f"24h Range:   ${lo:,.2f} — ${hi:,.2f}",
            f"MCap:        {fmt(mc)}  |  Vol: {fmt(vol)}  |  Vol/MCap: {vm:.2f}%",
            f"ATH:         ${ath:,.2f}  ({ath_p:.1f}% below ATH)",
            f"ATL:         ${atl:,.4f}",
            f"Circulating: {circ:,.0f}  ({pct_mined:.2f}% of 21M mined)",
        ]
        if cd:
            tw = cd.get("twitter_followers", 0) or 0
            btc_lines.append(f"Community:   Twitter {tw:,}")

    deriv_section = format_derivatives(funding, oi, liq, ls, "BTC")
    anchor = derivatives_anchor(funding, oi, liq, ls, "BTC")

    prompt = (
        "\n".join(btc_lines) + "\n\n" + deriv_section + "\n\n" + anchor + "\n\n"
        "TYPE B — BTC BRIEF.\n"
        "Price vs ATH: state % gap and drawdown risk at this level historically.\n"
        "Vol/MCap: elevated or suppressed — conviction signal.\n"
        "Momentum across timeframes: accelerating / decelerating / reversing?\n"
        "Derivatives: use ONLY the verified numbers above. State exact avg funding %, exact OI total, exact L/S ratio.\n"
        "Liquidation context: use exact 24h figures from above.\n"
        "End with VERDICT: SCALE IN / WAIT FOR LEVEL $X / AVOID — use only the live price from the data."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_derivatives(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    raw_sym = " ".join(context.args).strip().upper() if context.args else "BTC"
    gl_sym = raw_sym
    coin = await resolve_coin(raw_sym.lower())
    if coin:
        gl_sym = coin[1]

    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    try:
        funding, oi, liq, ls, oi_hist = await gl_multi(gl_sym)

        # Log what we actually got for debugging
        logger.info(f"Derivatives data for {gl_sym}: "
                    f"funding={'OK' if funding and funding.get('data') else 'NONE'}, "
                    f"oi={'OK' if oi and oi.get('data') else 'NONE'}, "
                    f"liq={'OK('+str(len(liq['data']))+' items)' if liq and liq.get('data') else 'NONE'}, "
                    f"ls={'OK' if ls and ls.get('data') else 'NONE'}")

        # Log liq structure for field name discovery
        if liq and liq.get("data"):
            first = liq["data"][0] if liq["data"] else {}
            logger.info(f"Liq item keys: {list(first.keys())}")

        deriv_section = format_derivatives(funding, oi, liq, ls, gl_sym)
        anchor = derivatives_anchor(funding, oi, liq, ls, gl_sym)
    except Exception as e:
        logger.error(f"cmd_derivatives error for {gl_sym}: {e}", exc_info=True)
        await update.message.reply_text(f"Derivatives data error: {str(e)[:200]}\nTry /funding or /oi instead.")
        return

    prompt = (
        f"{deriv_section}\n\n"
        f"{anchor}\n\n"
        f"Write a DERIVATIVES REPORT for {gl_sym} using ONLY the numbers above.\n\n"
        "FUNDING RATES\n"
        "State the exact avg % from the data. Identify the highest and lowest exchanges by name. "
        "Interpret: >0.08% = crowded long, <-0.03% = crowded short, near zero = neutral.\n\n"
        "OPEN INTEREST\n"
        "State the exact OI total. State the 24h % change. Flag any exchange >50% share.\n\n"
        "LONG/SHORT POSITIONING\n"
        "State the exact long %, short %, and ratio from the data. Interpret crowding risk.\n\n"
        "LIQUIDATION CONTEXT\n"
        "State the exact 24h total, long split, short split. Flag if >$50M/hr.\n\n"
        "DERIVATIVES VERDICT\n"
        "Bias: [BULLISH / BEARISH / NEUTRAL] — cite ONE specific number from the verified data above\n"
        "Key risk: one line\n"
        "Action: one line\n\n"
        "CRITICAL: Every number you write must appear verbatim in the VERIFIED DERIVATIVES NUMBERS section above. "
        "Do not round differently. Do not invent ratios. Do not use numbers from training data."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_funding(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    raw_sym = " ".join(context.args).strip().upper() if context.args else "BTC"
    coin = await resolve_coin(raw_sym.lower())
    gl_sym = coin[1] if coin else raw_sym

    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    funding = await gl_funding(gl_sym)

    lines = [f"FUNDING RATES — {gl_sym} | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    total, count = 0, 0
    if funding and funding.get("data"):
        raw = funding["data"]
        if isinstance(raw, list) and raw and isinstance(raw[0], dict) and ("stablecoin_margin_list" in raw[0] or "symbol" in raw[0]):
            raw = next((x for x in raw if x.get("symbol","").upper() == gl_sym.upper()), raw[0] if raw else {})
        exchanges = (raw.get("stablecoin_margin_list") or raw.get("usdtMarginList") or
                     (raw if isinstance(raw, list) else []))
        MAJOR = {"Binance","OKX","Bybit","Bitget","dYdX","Hyperliquid","Gate","MEXC","HTX","Kraken"}
        for ex in exchanges:
            name = ex.get("exchangeName") or ex.get("exchange") or "?"
            rate = ex.get("fundingRate") if ex.get("fundingRate") is not None else ex.get("funding_rate")
            if rate is None:
                continue
            try:
                r = float(rate) * 100
                if name in MAJOR:
                    total += r
                    count += 1
                flag = "  [EXTREME]" if abs(r) > 0.1 else ("  [elevated]" if abs(r) > 0.05 else "")
                lines.append(f"  {name:14} {r:>+8.4f}%{flag}")
            except Exception:
                pass
        if count:
            avg = total / count
            interp = ("CROWDED LONG — shorts are cheap hedge" if avg > 0.08 else
                      "CROWDED SHORT — squeeze potential" if avg < -0.03 else
                      "NEUTRAL — no directional bias in funding")
            lines.append(f"\nAvg (major): {avg:>+8.4f}%")
            lines.append(f"Interpretation: {interp}")
    else:
        lines.append("Funding data unavailable.")

    prompt = (
        "\n".join(lines) + "\n\n"
        f"TYPE A — FUNDING RATE ANALYSIS for {gl_sym}.\n"
        "State average funding rate and what the level implies for positioning.\n"
        "Which exchanges show the most extreme rates? What does divergence between exchanges mean?\n"
        "Crowded long: longs are paying — shorts have an edge. Crowded short: inverse.\n"
        "One-line actionable: what does a trader do with this funding structure right now?"
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_oi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    raw_sym = " ".join(context.args).strip().upper() if context.args else "BTC"
    coin = await resolve_coin(raw_sym.lower())
    gl_sym = coin[1] if coin else raw_sym

    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    oi_data = await gl_oi(gl_sym)

    lines = [f"OPEN INTEREST — {gl_sym} | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if oi_data and oi_data.get("data"):
        items = oi_data["data"]
        items = items if isinstance(items, list) else [items]
        def _ex_name_oi(x): return x.get("exchange") or x.get("exchangeName") or ""
        total_oi = sum(float(x.get("open_interest_usd") or x.get("openInterestUsd") or 0) for x in items)
        lines.append(f"Total OI: {fmt(total_oi)}\n")
        for x in sorted(items, key=lambda x: float(x.get("open_interest_usd") or x.get("openInterestUsd") or 0), reverse=True)[:10]:
            ex   = _ex_name_oi(x) or "?"
            oi   = float(x.get("open_interest_usd") or x.get("openInterestUsd") or 0)
            share = (oi / total_oi * 100) if total_oi else 0
            lines.append(f"  {ex:16} OI: {fmt(oi):>12}  ({share:.1f}% share)")
    else:
        lines.append("OI data unavailable.")

    prompt = (
        "\n".join(lines) + "\n\n"
        f"TYPE A — OPEN INTEREST ANALYSIS for {gl_sym}.\n"
        "Total OI: is this elevated or normal for this asset?\n"
        "Exchange concentration: if one exchange holds >50% OI, that is a risk.\n"
        "OI rising with price up = conviction move. OI rising with price down = leverage trap.\n"
        "One-line OI verdict: what does current OI level mean for near-term price risk?"
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_dominance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    gdata, top50, btc_deriv, dom_data = await asyncio.gather(
        cg_global(), cg_top50(), gl_multi("BTC"), gl_btc_dominance()
    )
    btc_fund, btc_oi, btc_liq, btc_ls, _ = btc_deriv

    lines = [f"DOMINANCE & ROTATION | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    # CoinGlass real-time BTC dominance — v4 fields: btcDominance or bitcoin_dominance
    if dom_data and dom_data.get("data"):
        dom_items = dom_data["data"]
        if isinstance(dom_items, list) and dom_items:
            latest = dom_items[-1]
            btc_dom_rt = float(latest.get("btcDominance") or latest.get("bitcoin_dominance") or 0)
            btc_p_rt   = float(latest.get("price") or latest.get("btcPrice") or 0)
            total_mc   = float(latest.get("totalMarketCap") or latest.get("market_cap") or 0)
            ts = latest.get("timestamp") or latest.get("time") or 0
            if ts > 1e10:
                date_str = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%b %d %H:%M")
            elif ts > 0:
                date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%b %d %H:%M")
            else:
                date_str = "?"
            lines.append(f"BTC Dominance: {btc_dom_rt:.3f}%  |  BTC: ${btc_p_rt:,.2f}  |  Total MCap: {fmt(total_mc)}")
            lines.append(f"Updated: {date_str} UTC")
            lines.append("")
    if gdata and "data" in gdata:
        g = gdata["data"]
        dom = g.get("market_cap_percentage", {})
        lines.append("Market cap dominance:")
        for sym, val in sorted(dom.items(), key=lambda x: -x[1])[:10]:
            lines.append(f"  {sym.upper():8} {val:.3f}%")
        lines.append(f"\nTotal MC:  {fmt(g['total_market_cap'].get('usd',0))}")
        lines.append(f"Total Vol: {fmt(g['total_volume'].get('usd',0))}")

    if top50:
        btc_7d = next((c.get("price_change_percentage_7d_in_currency",0) or 0
                       for c in top50 if c["id"]=="bitcoin"), 0)
        lines.append(f"\nTop 50 vs BTC (7d, BTC={pct(btc_7d)}):")
        lines.append(f"{'SYM':8} {'PRICE':>12}  {'24H':>7}  {'7D':>7}  {'vsBTC7d':>9}  {'MCap':>10}")
        lines.append("─"*62)
        for c in top50:
            ch24 = c.get("price_change_percentage_24h") or 0
            ch7d = c.get("price_change_percentage_7d_in_currency") or 0
            rel  = ch7d - btc_7d
            flag = " *" if rel > 5 else ""
            lines.append(
                f"{c['symbol'].upper():8} {price_str(c['current_price']):>12}  "
                f"{pct(ch24):>7}  {pct(ch7d):>7}  {rel:>+8.2f}%{flag}  {fmt(c['market_cap']):>10}"
            )

    prompt = (
        "\n".join(lines) + "\n\n"
        "TYPE A — DOMINANCE REPORT.\n"
        "BTC dom: exact % and what it implies for alt performance at this level.\n"
        "ETH dom vs BTC dom movement: diagnose rotation phase.\n"
        "Stablecoin dom: growing = risk-off. Shrinking = capital deployed.\n"
        "List all assets outperforming BTC on 7d (marked with *) — any pattern?\n"
        "Rotation trigger: specific BTC dom level that would confirm alt season start."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_trending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    trending, top50 = await asyncio.gather(cg_trending(), cg_top50())

    lines = [f"TRENDING & NARRATIVE | {datetime.now(timezone.utc).strftime('%H:%M')} UTC\n"]
    if trending and "coins" in trending:
        lines.append("Trending (last 24h search volume):")
        for i, item in enumerate(trending["coins"][:7], 1):
            c = item["item"]
            lines.append(
                f"  {i}. {c['name']} ({c['symbol'].upper()})  "
                f"Rank #{c.get('market_cap_rank','?')}  Score:{c.get('score',0)}"
            )

    if top50:
        gainers = sorted(
            [c for c in top50 if c.get("price_change_percentage_24h") is not None],
            key=lambda x: x["price_change_percentage_24h"], reverse=True
        )[:7]
        losers = sorted(
            [c for c in top50 if c.get("price_change_percentage_24h") is not None],
            key=lambda x: x["price_change_percentage_24h"]
        )[:7]

        lines.append(f"\nTop gainers (top 50 by MCap):")
        lines.append(f"  {'SYM':8} {'24H':>7}  {'PRICE':>12}  {'VOL/MCAP':>9}  {'MCAP':>10}")
        for c in gainers:
            ch24 = c.get("price_change_percentage_24h") or 0
            mc   = c.get("market_cap", 1) or 1
            vol  = c.get("total_volume", 0) or 0
            vm   = vol / mc * 100
            lines.append(
                f"  {c['symbol'].upper():8} {pct(ch24):>7}  "
                f"{price_str(c['current_price']):>12}  {vm:>8.1f}%  {fmt(mc):>10}"
            )

        lines.append(f"\nTop losers (top 50 by MCap):")
        for c in losers:
            ch24 = c.get("price_change_percentage_24h") or 0
            mc   = c.get("market_cap", 1) or 1
            vol  = c.get("total_volume", 0) or 0
            vm   = vol / mc * 100
            lines.append(
                f"  {c['symbol'].upper():8} {pct(ch24):>7}  "
                f"{price_str(c['current_price']):>12}  {vm:>8.1f}%  {fmt(mc):>10}"
            )

    prompt = (
        "\n".join(lines) + "\n\n"
        "TYPE A — TRENDING REPORT.\n"
        "Use ONLY the prices in the live data above. Do NOT use any historical prices.\n"
        "Do NOT write MARKET STRUCTURE, ON-CHAIN CONTEXT, or DERIVATIVES sections.\n"
        "Do NOT include a TRADE SETUP section.\n"
        "Write only: NARRATIVE section and ACTION line.\n"
        "Vol/MCap ratio separates organic from retail chasing. State which for each gainer.\n"
        "Flag any gainer with >30% gain and <$200M MCap — high manipulation risk.\n"
        "Losers: are strong assets selling off (opportunity) or justified exit?\n"
        "Dominant narrative in one sentence. Capital behind it or search noise?"
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_defi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    tvl, protocols, chains, dex_data = await asyncio.gather(
        ll("/tvl"), ll("/protocols"), ll("/v2/chains"), ll_dex()
    )

    lines = [f"DEFI DASHBOARD | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]

    if tvl:
        try:
            lines.append(f"Total DeFi TVL: {fmt(float(tvl))}")
        except Exception:
            pass

    # DEX volumes
    if dex_data and dex_data.get("protocols"):
        dex_protos = sorted(
            [p for p in dex_data["protocols"] if p.get("total24h")],
            key=lambda x: x.get("total24h", 0), reverse=True
        )[:10]
        total_dex_24h = dex_data.get("total24h") or sum(p.get("total24h", 0) for p in dex_protos)
        lines.append(f"\nDEX VOLUMES (24h)")
        lines.append(f"Total DEX 24h: {fmt(total_dex_24h)}")
        lines.append(f"{'DEX':20} {'24H VOL':>10}  {'7D VOL':>10}  {'1D%':>7}")
        lines.append("─"*54)
        for p in dex_protos:
            lines.append(
                f"{p['name']:20} {fmt(p.get('total24h') or 0):>10}  "
                f"{fmt(p.get('total7d') or 0):>10}  {pct(p.get('change_1d') or 0):>7}"
            )

    # Top protocols by TVL
    if protocols:
        valid = sorted([p for p in protocols if float(p.get("tvl") or 0) > 0],
                       key=lambda x: float(x.get("tvl") or 0), reverse=True)[:12]
        lines.append(f"\n{'PROTOCOL TVL':22} {'TVL':>10}  {'1D':>7}  {'7D':>7}  CHAIN")
        lines.append("─"*62)
        for p in valid:
            lines.append(
                f"{p['name']:22} {fmt(p.get('tvl') or 0):>10}  "
                f"{pct(p.get('change_1d') or 0):>7}  {pct(p.get('change_7d') or 0):>7}  "
                f"{p.get('chain','multi')}"
            )

    # Top chains by TVL
    if chains:
        valid_c = sorted([c for c in chains if float(c.get("tvl") or 0) > 0],
                         key=lambda x: float(x.get("tvl") or 0), reverse=True)[:8]
        lines.append(f"\n{'CHAIN':18} {'TVL':>10}")
        lines.append("─"*30)
        for c in valid_c:
            lines.append(f"{c.get('name','?'):18} {fmt(c.get('tvl') or 0):>10}")

    prompt = (
        "\n".join(lines) + "\n\n"
        "DEFI REPORT — use only the data above.\n"
        "Do NOT write MARKET STRUCTURE, ON-CHAIN CONTEXT, or DERIVATIVES sections.\n"
        "Do NOT include a TRADE SETUP section.\n"
        "1. Total TVL: direction and what it signals about risk appetite.\n"
        "2. DEX volumes: which protocols are capturing trading flow and why.\n"
        "3. Top 3 TVL gainers: genuine capital inflow or price effect?\n"
        "4. Top 3 TVL losers: exit or rotation?\n"
        "5. Chain dominance: flag any chain gaining or losing significant share.\n"
        "ACTION line: one sentence on where on-chain capital is moving."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_fear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    fng, gdata, stables, prices = await asyncio.gather(
        _fetch(FNG_URL, {}, {}),
        cg_global(),
        cg("/coins/markets", {
            "vs_currency":"usd",
            "ids":"tether,usd-coin,dai,first-digital-usd",
            "order":"market_cap_desc",
        }),
        cg_market("bitcoin,ethereum"),
    )

    lines = [f"SENTIMENT DATA | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]
    lines.append("CURRENT PRICES (use these for any trade levels):")
    if prices:
        for c in prices:
            ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
            ch24h = c.get("price_change_percentage_24h") or 0
            ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
            lines.append(
                f"  {c['symbol'].upper():5} {price_str(c['current_price']):>12}  "
                f"1h:{pct(ch1h):>7}  24h:{pct(ch24h):>7}  7d:{pct(ch7d):>7}"
            )

    if fng and "data" in fng:
        lines.append("\nFear & Greed (Alternative.me):")
        for e in fng["data"][:3]:
            ts2 = datetime.fromtimestamp(int(e["timestamp"]), tz=timezone.utc).strftime("%b %d")
            lines.append(f"  {ts2}: {e['value']:>3}/100 — {e['value_classification']}")
        lines.append("  Reference: <20 = extreme fear/historical bottom zone. >80 = extreme greed/top risk.")

    if gdata and "data" in gdata:
        g = gdata["data"]
        dom = g.get("market_cap_percentage", {})
        lines.append(f"\nTotal MC: {fmt(g['total_market_cap'].get('usd',0))}  "
                     f"24h: {pct(g.get('market_cap_change_percentage_24h_usd',0))}")
        lines.append(f"BTC Dom: {dom.get('btc',0):.2f}%")

    if stables:
        total_s = 0
        lines.append("\nStablecoin supply:")
        for s in stables:
            mc  = s.get("market_cap",0) or 0
            vol = s.get("total_volume",0) or 0
            ratio = (vol/mc*100) if mc else 0
            total_s += mc
            lines.append(f"  {s['symbol'].upper():6} MCap:{fmt(mc):>10}  Vol:{fmt(vol):>10}  V/M:{ratio:.1f}%")
        lines.append(f"  TOTAL: {fmt(total_s)}")
        lines.append("  Note: USDT V/M of 20-60% is normal. Flag only if >80% or if growing rapidly vs prior day.")

    prompt = (
        "\n".join(lines) + "\n\n"
        "TYPE A — SENTIMENT REPORT.\n"
        "CRITICAL: Use ONLY the live prices in CURRENT PRICES for any trade levels.\n"
        "Fear & Greed score, 3-day trend, and which zone (extreme fear/fear/neutral/greed/extreme greed).\n"
        "Stablecoin total supply direction: explicitly state growing or shrinking and implication.\n"
        "USDT Vol/MCap: flag if >15% — that signals imminent large move.\n"
        "Positioning implication: are traders over-extended or is there capacity to absorb buying?"
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_etf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    btc_d, etf_flows, etf_list = await asyncio.gather(
        cg("/coins/bitcoin", {"localization":"false","tickers":"false","market_data":"true",
                              "community_data":"false","developer_data":"false"}),
        gl_etf_flows(),
        gl_etf_list(),
    )

    lines = [f"BTC ETF DATA | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]

    # BTC price context
    if btc_d:
        md = btc_d.get("market_data", {}) or {}
        p    = (md.get("current_price") or {}).get("usd", 0)
        ch24 = md.get("price_change_percentage_24h", 0) or 0
        mc   = (md.get("market_cap") or {}).get("usd", 0)
        vol  = (md.get("total_volume") or {}).get("usd", 0)
        ath  = (md.get("ath") or {}).get("usd", 0)
        ath_p= (md.get("ath_change_percentage") or {}).get("usd", 0)
        vm   = (vol / mc * 100) if mc else 0
        lines.append(f"BTC: ${p:,.2f}  24h:{pct(ch24)}  MCap:{fmt(mc)}")
        lines.append(f"Vol/MCap: {vm:.2f}%  |  vs ATH: {ath_p:.1f}%  (ATH ${ath:,.0f})")
        lines.append("")

    # ETF list — v4 fields: ticker, fund_name, region, market_status, primary_exchange
    if etf_list and etf_list.get("data"):
        items = etf_list["data"]
        lines.append("BTC ETF LIST:")
        lines.append(f"  {'TICKER':8} {'NAME':28} {'EXCHANGE':14} {'STATUS':10} {'REGION'}")
        lines.append("  " + "─"*72)
        for item in items[:12]:
            try:
                ticker   = item.get("ticker") or "?"
                name     = (item.get("fund_name") or "")[:26]
                exchange = (item.get("primary_exchange") or "")[:12]
                status   = item.get("market_status") or ""
                region   = item.get("region") or ""
                lines.append(f"  {ticker:8} {name:28} {exchange:14} {status:10} {region}")
            except Exception:
                pass
        lines.append("")

    # ETF flow history — v4: flowUsd/flow_usd, timestamp
    if etf_flows and etf_flows.get("data"):
        items = etf_flows["data"]
        try:
            items = sorted(items, key=lambda x: x.get("timestamp",0), reverse=True)
        except Exception:
            pass
        lines.append("BTC ETF DAILY FLOWS:")
        total_7d = 0.0
        for item in items[:7]:
            try:
                ts  = item.get("timestamp", 0)
                if ts > 1e10:
                    date_str = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%b %d")
                elif ts > 0:
                    date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%b %d")
                else:
                    date_str = "?"
                net = float(item.get("flowUsd") or item.get("flow_usd") or 0)
                total_7d += net
                sign = "+" if net >= 0 else ""
                label_d = "INFLOW " if net >= 0 else "OUTFLOW"
                lines.append(f"  {date_str}: {sign}{fmt(abs(net)):>10}  {label_d}")
            except Exception:
                pass
        sign = "+" if total_7d >= 0 else ""
        lines.append(f"  7-day net: {sign}{fmt(abs(total_7d))}  {'NET INFLOW' if total_7d >= 0 else 'NET OUTFLOW'}")
    else:
        lines.append("ETF flow data unavailable")

    prompt = (
        "\n".join(lines) + "\n\n"
        "ETF & INSTITUTIONAL REPORT — use only the data above.\n"
        "Do NOT write ON-CHAIN CONTEXT or DERIVATIVES sections.\n"
        "Do NOT include a TRADE SETUP section.\n"
        "ETF flows: state the 7-day net total and direction (inflow/outflow). What does the trend imply for institutional conviction?\n"
        "ETF list: how many ETFs are active? Note any regional patterns.\n"
        "BTC Vol/MCap: state the % and what it implies about institutional desk activity.\n"
        "ATH distance: how much pain are late-cycle ETF buyers currently in?\n"
        "One-line verdict: are conditions favourable or unfavourable for ETF inflows right now?"
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_macro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    prices, gdata, fng = await asyncio.gather(
        cg_market("bitcoin,ethereum"),
        cg_global(),
        _fetch(FNG_URL, {}, {}),
    )

    now = datetime.now(timezone.utc)
    lines = [f"MACRO BRIEFING | {now.strftime('%Y-%m-%d %H:%M')} UTC\n"]

    if prices:
        for c in prices:
            ch24 = c.get("price_change_percentage_24h", 0) or 0
            ch7d = c.get("price_change_percentage_7d_in_currency", 0) or 0
            lines.append(f"{c['symbol'].upper()}: {price_str(c['current_price'])}  "
                         f"24h:{pct(ch24)}  7d:{pct(ch7d)}")

    if gdata and "data" in gdata:
        g = gdata["data"]
        dom = g.get("market_cap_percentage", {})
        lines.append(f"Total MC: {fmt(g['total_market_cap'].get('usd',0))}  "
                     f"24h:{pct(g.get('market_cap_change_percentage_24h_usd',0))}")
        lines.append(f"BTC Dom: {dom.get('btc',0):.2f}%  ETH Dom: {dom.get('eth',0):.2f}%")

    if fng and "data" in fng:
        e = fng["data"][0]
        lines.append(f"Fear & Greed: {e['value']}/100 — {e['value_classification']}")

    lines.append("\nHIGH-IMPACT MACRO CALENDAR (recurring):")
    lines.append("  [RED]    FOMC Meeting — rate decision + dot plot + press conference")
    lines.append("           Schedule: 8x/year. Next dates: May 6-7, Jun 17-18, Jul 29-30, Sep 16-17, Nov 4-5, Dec 9-10 2025")
    lines.append("           Crypto impact: hawkish surprise = risk-off selloff; dovish pivot = sustained rally")
    lines.append("  [RED]    US CPI — monthly inflation print")
    lines.append("           Schedule: ~2nd-3rd week each month (Tue/Wed). Released 8:30am ET")
    lines.append("           Crypto impact: hot print = tightening fear; cold print = risk-on")
    lines.append("  [RED]    US NFP — non-farm payrolls")
    lines.append("           Schedule: 1st Friday each month, 8:30am ET")
    lines.append("           Crypto impact: strong jobs = Fed stays hawkish; weak jobs = rate cut hope")
    lines.append("  [AMBER]  US PPI — producer price index, precursor to CPI")
    lines.append("           Schedule: 1 day before CPI each month")
    lines.append("  [AMBER]  BTC Options Expiry (Deribit)")
    lines.append("           Schedule: Every Friday. Large monthly on last Friday of month")
    lines.append("           Pattern: price tends to move to max pain before expiry, then break free")
    lines.append("  [AMBER]  Fed Speakers — speeches, congressional testimony")
    lines.append("           Watch: Powell, Waller, Williams. Forward guidance shifts markets intraday")
    lines.append("  [AMBER]  PCE Deflator — Fed's preferred inflation metric")
    lines.append("           Schedule: last week of each month, 8:30am ET")
    lines.append("  [INFO]   Token Unlocks — tokenunlocks.app (sell pressure calendar)")
    lines.append("  [INFO]   Governance — snapshot.org (protocol votes, airdrop eligibility)")
    lines.append("  [INFO]   CME BTC Futures Expiry — last Friday of each month, 4pm ET")
    lines.append("\nReal-time calendars: ForexFactory.com | Investing.com | CMEGroup.com/trading/fedwatch")

    prompt = (
        "\n".join(lines) + "\n\n"
        "TYPE A — MACRO REGIME BRIEFING.\n"
        "Current macro regime: use BTC/ETH prices + Fear & Greed + Total MC data above.\n"
        "State: risk-on / risk-off / transitional — with the single data point that defines current regime.\n"
        "FOMC posture: what is the current market expectation for the next rate decision?\n"
        "Pre-event playbook: what does an institutional desk do in the 48h before FOMC and CPI?\n"
        "Crypto-macro correlation: is BTC trading as risk-asset or decoupling? State which.\n"
        "Do NOT invent CPI/NFP/rate values. Describe mechanism and direction of impact only.\n"
        "End with: REGIME LINE — one sentence, one data point."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_gltest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug command: tests all CoinGlass endpoints and reports status."""
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("Owner only command.")
        return
    sym = " ".join(context.args).strip().upper() if context.args else "BTC"
    await update.message.reply_text(f"Running endpoint tests for {sym}...")
    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    report = await gl_debug(sym)
    await send(update, report)

async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    args = context.args or []

    if args and args[0].lower() == "add" and len(args) > 1:
        query = " ".join(args[1:])
        coin = await resolve_coin(query)
        if not coin:
            await update.message.reply_text(f"Could not resolve '{query}'. Try the full name or ticker (e.g. /watchlist add solana).")
            return
        cg_id = coin[0]
        wl = user.get("watchlist", [])
        if cg_id in wl:
            await update.message.reply_text(f"{cg_id} is already on your watchlist.")
            return
        if len(wl) >= 15:
            await update.message.reply_text("Watchlist limit is 15 coins. Remove one first: /watchlist remove [coin]")
            return
        wl.append(cg_id)
        user["watchlist"] = wl
        save_user(update.effective_user.id, user)
        await update.message.reply_text(f"Added {cg_id} to watchlist. ({len(wl)}/15)")
        return

    if args and args[0].lower() == "remove" and len(args) > 1:
        query = " ".join(args[1:])
        coin = await resolve_coin(query)
        cg_id = coin[0] if coin else query.lower()
        wl = user.get("watchlist", [])
        if cg_id not in wl:
            # Try partial match
            match = next((x for x in wl if query.lower() in x), None)
            if match:
                cg_id = match
            else:
                await update.message.reply_text(f"'{query}' not found on watchlist.")
                return
        wl.remove(cg_id)
        user["watchlist"] = wl
        save_user(update.effective_user.id, user)
        await update.message.reply_text(f"Removed {cg_id}. Watchlist: {len(wl)} coins.")
        return

    # Show watchlist with live prices and analysis
    wl = user.get("watchlist", ["bitcoin", "ethereum"])
    if not wl:
        await update.message.reply_text("Watchlist is empty.\nAdd coins: /watchlist add bitcoin")
        return

    await context.bot.send_chat_action(update.effective_chat.id, "typing")
    ids = ",".join(wl)
    data = await cg_market(ids)

    if not data:
        await update.message.reply_text("Could not fetch watchlist data. Try again.")
        return

    lines = [f"WATCHLIST | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]
    lines.append(f"{'COIN':8} {'PRICE':>12}  {'1H':>7}  {'24H':>7}  {'7D':>7}  {'VOL/MC':>7}  {'vs ATH':>7}")
    lines.append("─"*68)
    btc = next((c for c in data if c["id"] == "bitcoin"), None)
    btc_24h = (btc.get("price_change_percentage_24h") or 0) if btc else 0
    for c in data:
        ch1h  = c.get("price_change_percentage_1h_in_currency") or 0
        ch24h = c.get("price_change_percentage_24h") or 0
        ch7d  = c.get("price_change_percentage_7d_in_currency") or 0
        mc    = c.get("market_cap") or 1
        vol   = c.get("total_volume") or 0
        vm    = vol / mc * 100
        ath_p = c.get("ath_change_percentage") or 0
        lines.append(
            f"{c['symbol'].upper():8} {price_str(c['current_price']):>12}  "
            f"{pct(ch1h):>7}  {pct(ch24h):>7}  {pct(ch7d):>7}  {vm:>6.1f}%  {ath_p:>+6.1f}%"
        )

    prompt = (
        "\n".join(lines) + "\n\n"
        f"TYPE G — PORTFOLIO REVIEW for {len(wl)}-asset watchlist.\n"
        "For each asset: state bias, the single most important level, and the allocation action.\n"
        "BTC-relative performance: flag any asset outperforming or underperforming BTC by >5% on 7d.\n"
        "Vol/MCap: flag any coin with >20% ratio (high conviction move) or <1% (dead money).\n"
        "Portfolio summary: what is the aggregate risk posture? Which 2 assets have the strongest setup?\n"
        "Use ONLY the live data above. No training-data prices."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""), max_tokens=1800)
    await send(update, result)


async def cmd_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    question = " ".join(context.args).strip() if context.args else ""
    if not question:
        await update.message.reply_text(
            "Usage: /ask [question]\n\n"
            "Examples:\n"
            "  /ask should I scale into SEI now\n"
            "  /ask is BTC at a good entry\n"
            "  /ask compare TAO vs RNDR\n"
            "  /ask what is funding rate arbitrage\n"
            "  /ask any alerts right now\n"
            "  /ask explain open interest divergence"
        )
        return
    await handle_query(update, context, question, user)

# ── /setup conversation ───────────────────────────────────────────────────────
async def cmd_setup_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    current = user.get("custom_instructions","").strip()
    await update.message.reply_text(
        "*Tetranomio — Custom Analyst Profile*\n\n"
        f"Current: `{current or 'none set'}`\n\n"
        "This context is injected into every Tetranomio response. Be specific.\n\n"
        "Good examples:\n"
        "  Focus coins: BTC, ETH, SOL, LINK, TAO, SEI\n"
        "  Style: swing trading, 3-7 day holds\n"
        "  Risk: 2% max per trade, $20,000 portfolio\n"
        "  Current positions: long BTC $82k, long ETH $2,100\n"
        "  Priority: derivatives signals over price action\n\n"
        "Type your profile now, or /cancel.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_SETUP

async def cmd_setup_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user(update.effective_user.id)
    user["custom_instructions"] = update.message.text.strip()
    save_user(update.effective_user.id, user)
    await update.message.reply_text(
        f"Saved. Active in all responses.\n\n`{user['custom_instructions']}`",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END

async def cmd_setup_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END

async def cmd_plans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    plan = "OWNER" if uid == OWNER_ID else ("PRO" if is_pro(uid) else "FREE")
    contact = f"@{OWNER_USERNAME}" if OWNER_USERNAME else "the bot owner"
    await update.message.reply_text(
        f"*Tetranomio Plans*  |  Your current plan: {plan}\n\n"
        "*FREE — No cost*\n"
        "  /fear — Fear & Greed + stablecoin supply\n"
        "  /macro — Macro regime + FOMC/CPI/NFP calendar\n"
        "  Automatic market alerts (price shocks, funding extremes)\n\n"
        "*PRO — $10 / month*\n"
        "  Everything in Free, plus:\n"
        "  /tetra — Full institutional market report\n"
        "  /btc — BTC deep-dive with full derivatives\n"
        "  /defi — TVL + DEX volumes + chain share\n"
        "  /dex — DEX volume rankings (Uniswap, Curve, GMX...)\n"
        "  /yields — Top yield pools by TVL (DeFiLlama)\n"
        "  /derivatives — Funding + OI + long/short + liquidations\n"
        "  /funding — Per-exchange funding rates\n"
        "  /oi — Open interest breakdown\n"
        "  /etf — Institutional ETF flows + BTC holdings\n"
        "  /dominance — BTC dominance + alt rotation signal\n"
        "  /trending — Trending + gainers/losers\n"
        "  /watchlist — Portfolio monitoring (up to 15 coins)\n"
        "  /ask — Free-text question with live data\n"
        "  Free-text analysis on any coin or question\n\n"
        f"To upgrade: /upgrade\n"
        f"Contact: {contact}",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_upgrade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid == OWNER_ID or is_pro(uid):
        await update.message.reply_text("You already have Tetranomio Pro. Thank you.")
        return
    contact = f"@{OWNER_USERNAME}" if OWNER_USERNAME else "the bot owner"
    await update.message.reply_text(
        "*Upgrade to Tetranomio Pro — $10/month*\n\n"
        "Payment: Crypto (USDT / USDC — any chain)\n\n"
        f"DM {contact} with:\n"
        f"  1. Your Telegram user ID: `{uid}`\n"
        "  2. Proof of payment (tx hash or screenshot)\n\n"
        "Access activated within 24 hours.\n\n"
        "/plans — see all features",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user = get_user(uid)
    args = context.args or []

    if args and args[0].lower() == "off":
        user["alerts"] = False
        save_user(uid, user)
        await update.message.reply_text("Alerts disabled. Use /alerts on to re-enable.")
        return

    if args and args[0].lower() == "on":
        user["alerts"] = True
        save_user(uid, user)
        await update.message.reply_text("Alerts enabled. You will receive automatic market alerts.")
        return

    status = "ON" if user.get("alerts", True) else "OFF"
    await update.message.reply_text(
        f"*Tetranomio Automatic Alerts*  |  Status: {status}\n\n"
        "Active triggers:\n"
        "  BTC or ETH price moves >5% in 1 hour\n"
        "  BTC funding rate crosses extreme (>0.10% or <-0.05%)\n"
        "  Fear & Greed enters or exits extreme zones (<=20 or >=80)\n\n"
        "Alerts are automatic and sent to all users by default.\n"
        "  /alerts off — disable\n"
        "  /alerts on  — re-enable",
        parse_mode=ParseMode.MARKDOWN,
    )

async def cmd_dex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    dex_data, fees_data = await asyncio.gather(ll_dex(), ll_fees())

    lines = [f"DEX VOLUMES | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]

    if dex_data and dex_data.get("protocols"):
        protos = sorted(
            [p for p in dex_data["protocols"] if p.get("total24h")],
            key=lambda x: x.get("total24h", 0), reverse=True
        )[:15]
        total_24h = dex_data.get("total24h") or sum(p.get("total24h", 0) for p in protos)
        total_7d  = dex_data.get("total7d")  or sum(p.get("total7d", 0) for p in protos)
        lines.append(f"Total DEX 24h: {fmt(total_24h)}  |  7d: {fmt(total_7d)}")
        lines.append(f"\n{'DEX':22} {'24H VOL':>10}  {'7D VOL':>10}  {'1D%':>7}  CHAINS")
        lines.append("─"*66)
        for p in protos:
            chains_str = ", ".join(p.get("chains", [])[:3]) or p.get("chain", "?")
            lines.append(
                f"{p['name']:22} {fmt(p.get('total24h') or 0):>10}  "
                f"{fmt(p.get('total7d') or 0):>10}  {pct(p.get('change_1d') or 0):>7}  {chains_str}"
            )
    else:
        lines.append("DEX volume data unavailable")

    if fees_data and fees_data.get("protocols"):
        fee_protos = sorted(
            [p for p in fees_data["protocols"] if p.get("total24h")],
            key=lambda x: x.get("total24h", 0), reverse=True
        )[:8]
        lines.append(f"\n{'PROTOCOL FEES (24h)':22} {'FEES':>10}  {'REVENUE':>10}")
        lines.append("─"*46)
        for p in fee_protos:
            rev = p.get("totalRevenue24h") or p.get("revenue24h") or 0
            lines.append(
                f"{p['name']:22} {fmt(p.get('total24h') or 0):>10}  {fmt(rev):>10}"
            )

    prompt = (
        "\n".join(lines) + "\n\n"
        "DEX VOLUME REPORT — use only the data above.\n"
        "Do NOT write ON-CHAIN CONTEXT, DERIVATIVES, or TRADE SETUP sections.\n"
        "1. Who is capturing DEX market share and why (Uniswap vs Curve vs GMX etc).\n"
        "2. Fee/revenue leaders: which protocols are most profitable per unit volume?\n"
        "3. Volume trend: is 24h accelerating or decelerating vs 7d run-rate?\n"
        "4. Capital implication: what does DEX volume composition tell us about risk appetite?\n"
        "One-line verdict: where is trading activity concentrating and what does that signal."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""))
    await send(update, result)

async def cmd_yields(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    await context.bot.send_chat_action(update.effective_chat.id, "typing")

    pools = await ll_yields(top=30)

    lines = [f"YIELD POOLS | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC\n"]
    lines.append("Source: DeFiLlama — pools >$1M TVL, sorted by TVL\n")

    if pools:
        lines.append(f"{'PROJECT':20} {'SYMBOL':10} {'TVL':>10}  {'APY':>7}  {'BASE':>7}  {'REWARD':>7}  CHAIN")
        lines.append("─"*82)
        for p in pools[:25]:
            sym    = (p.get("symbol") or "")[:10]
            proj   = (p.get("project") or "")[:20]
            tvl    = p.get("tvlUsd") or 0
            apy    = p.get("apy") or 0
            base   = p.get("apyBase") or 0
            reward = p.get("apyReward") or 0
            chain  = p.get("chain") or "?"
            il_risk = " IL" if p.get("ilRisk") not in (None, "no", "none") else ""
            lines.append(
                f"{proj:20} {sym:10} {fmt(tvl):>10}  {apy:>6.2f}%  {base:>6.2f}%  {reward:>6.2f}%  {chain}{il_risk}"
            )
        # Stablecoin-only pools (no IL risk) — highest APY
        stable_pools = sorted(
            [p for p in pools if p.get("exposure") in ("single", "stable") or p.get("ilRisk") in (None, "no", "none")],
            key=lambda x: x.get("apy") or 0, reverse=True
        )[:5]
        if stable_pools:
            lines.append("\nTOP STABLE / NO-IL POOLS BY APY:")
            for p in stable_pools:
                lines.append(
                    f"  {(p.get('project') or '')[:20]:20} {(p.get('symbol') or '')[:10]:10} "
                    f"TVL:{fmt(p.get('tvlUsd') or 0):>10}  APY:{p.get('apy') or 0:.2f}%  {p.get('chain','?')}"
                )
    else:
        lines.append("Yield pool data unavailable")

    prompt = (
        "\n".join(lines) + "\n\n"
        "YIELD FARMING REPORT — use only the pool data above.\n"
        "Do NOT write DERIVATIVES, MARKET STRUCTURE, or TRADE SETUP sections.\n"
        "1. Highest-conviction yield pools: TVL depth + APY sustainability (base vs reward split).\n"
        "2. Stablecoin yield: best risk-adjusted options for capital preservation with yield.\n"
        "3. IL risk: flag any high-APY pools where impermanent loss risk is elevated.\n"
        "4. Reward vs base: heavy reward APY with low base = emissions-dependent, unsustainable.\n"
        "Verdict: two or three specific pools worth evaluating for institutional allocators. State chain, TVL, APY."
    )
    result = await ask_groq(prompt, user.get("custom_instructions",""), max_tokens=1800)
    await send(update, result)

# ── Free-text ─────────────────────────────────────────────────────────────────
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if not text:
        return
    if not await tier_gate(update): return
    user = get_user(update.effective_user.id)
    await handle_query(update, context, text, user)

# ── Error handler ─────────────────────────────────────────────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    import traceback
    tb = "".join(traceback.format_exception(type(context.error), context.error, context.error.__traceback__))
    logger.error(f"Unhandled error:\n{tb}")
    if isinstance(update, Update) and update.message:
        await update.message.reply_text("Something went wrong. Try again.")

# ── Health server (required by Render free tier) ──────────────────────────────
async def start_health_server():
    port = int(os.getenv("PORT", "8080"))

    async def health(_):
        return web.Response(text="Tetranomio OK")

    srv = web.Application()
    srv.router.add_get("/", health)
    srv.router.add_get("/health", health)
    runner = web.AppRunner(srv)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logger.info(f"Health server listening on port {port}")

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    setup_conv = ConversationHandler(
        entry_points=[CommandHandler("setup", cmd_setup_start)],
        states={WAITING_SETUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_setup_receive)]},
        fallbacks=[CommandHandler("cancel", cmd_setup_cancel)],
    )

    handlers = [
        ("start",       cmd_start),
        ("help",        cmd_help),
        ("plans",       cmd_plans),
        ("upgrade",     cmd_upgrade),
        ("alerts",      cmd_alerts),
        ("tetra",       cmd_cipher),
        ("btc",         cmd_btc),
        ("dominance",   cmd_dominance),
        ("trending",    cmd_trending),
        ("defi",        cmd_defi),
        ("dex",         cmd_dex),
        ("yields",      cmd_yields),
        ("fear",        cmd_fear),
        ("etf",         cmd_etf),
        ("macro",       cmd_macro),
        ("derivatives", cmd_derivatives),
        ("funding",     cmd_funding),
        ("oi",          cmd_oi),
        ("ask",         cmd_ask),
        ("watchlist",   cmd_watchlist),
        ("gltest",      cmd_gltest),
    ]
    for name, handler in handlers:
        app.add_handler(CommandHandler(name, handler))

    app.add_handler(setup_conv)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    async with app:
        await app.initialize()
        await app.bot.set_my_commands([
            BotCommand("tetra",       "Full institutional report (Pro)"),
            BotCommand("btc",         "BTC deep dive with full derivatives (Pro)"),
            BotCommand("defi",        "DeFi TVL + DEX volumes (Pro)"),
            BotCommand("dex",         "DEX volume rankings (Pro)"),
            BotCommand("yields",      "Top yield pools by TVL (Pro)"),
            BotCommand("derivatives", "Funding + OI + liquidations (Pro)"),
            BotCommand("funding",     "Funding rates across exchanges (Pro)"),
            BotCommand("oi",          "Open interest breakdown (Pro)"),
            BotCommand("dominance",   "BTC dominance + alt rotation (Pro)"),
            BotCommand("trending",    "Trending + gainers/losers (Pro)"),
            BotCommand("etf",         "Institutional ETF flows (Pro)"),
            BotCommand("fear",        "Fear & Greed + stablecoin supply"),
            BotCommand("macro",       "Macro regime + event calendar"),
            BotCommand("alerts",      "Automatic market alerts on/off"),
            BotCommand("watchlist",   "Portfolio watchlist (Pro)"),
            BotCommand("ask",         "Ask anything with live data (Pro)"),
            BotCommand("plans",       "See all features + pricing"),
            BotCommand("upgrade",     "Upgrade to Pro ($10/month)"),
            BotCommand("setup",       "Custom analyst profile"),
            BotCommand("help",        "All commands + examples"),
        ])
        logger.info("Tetranomio — Online")
        asyncio.create_task(start_health_server())
        asyncio.create_task(run_alert_poller(app))
        await app.start()
        await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        await asyncio.Event().wait()
        await app.updater.stop()
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
