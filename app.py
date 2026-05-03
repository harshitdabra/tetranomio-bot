"""
Tetranomio — FastAPI backend
Data: CoinGecko Pro + CoinGlass v4 + DeFiLlama + Alternative.me
AI:   Gemini 1.5 Flash
"""

import os, json, asyncio, logging
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import google.generativeai as genai

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("TETRANOMIO")

# ── Config ────────────────────────────────────────────────────────────────────
CG_KEY     = os.getenv("COINGECKO_API_KEY", "")
GLASS_KEY  = os.getenv("COINGLASS_API_KEY", "")
GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
PORT       = int(os.getenv("PORT", "8000"))

CG_BASE     = "https://pro-api.coingecko.com/api/v3"
GLASS_BASE  = "https://open-api-v4.coinglass.com/api"
LLAMA_BASE  = "https://api.llama.fi"
YIELDS_BASE = "https://yields.llama.fi"
FNG_URL     = "https://api.alternative.me/fng/?limit=3"

# ── Gemini init ───────────────────────────────────────────────────────────────
if GEMINI_KEY:
    genai.configure(api_key=GEMINI_KEY)

GEMINI_SYSTEM = """IDENTITY
You are TETRANOMIO — senior digital assets analyst at a tier-1 institutional desk (BlackRock / Goldman Sachs / Fidelity caliber).
You write at the standard of a morning markets brief distributed to portfolio managers and CIOs.
Every output must be immediately actionable.

ANALYTICAL FRAMEWORK — APPLY IN ORDER:
1. MACRO REGIME: Fed posture → DXY → risk premium → crypto beta.
2. INSTITUTIONAL FLOWS: ETF inflows/outflows, stablecoin supply. Capital is the signal.
3. DERIVATIVES STRUCTURE: Funding rate, OI, long/short ratio.
4. PRICE + MOMENTUM: Only after 1-3 above.

ABSOLUTE OUTPUT RULES:
- Zero emojis. Zero exclamation marks.
- Use **bold** for section headers.
- Never use training-data prices. Use ONLY the exact prices in the provided live data.
- Never fabricate signals. Missing data = write "data unavailable" and stop that section.
- Never pad responses. Every sentence must carry new analytical content.
- NUMBER FORMAT: Always K/M/B notation.
- BANNED PHRASES: it is worth noting | this suggests | potentially | may indicate | could be | in conclusion | to summarize | overall | essentially | notably | importantly | interestingly | delve | landscape | ecosystem | robust | seamless | market participants | strong fundamentals | weak fundamentals | bullish outlook | bearish sentiment | moon | WAGMI | ape in | degen"""

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Tetranomio API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
import pathlib
STATIC_DIR = pathlib.Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ── HTTP helpers ──────────────────────────────────────────────────────────────
async def _fetch(url: str, headers: dict, params: dict) -> dict | list | None:
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=25) as client:
                r = await client.get(url, headers=headers, params=params)
                if r.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning(f"Rate limited {url[:60]}, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if r.status_code in (401, 403):
                    logger.error(f"Auth failed {url[:60]}: {r.status_code}")
                    return None
                r.raise_for_status()
                return r.json()
        except httpx.TimeoutException:
            logger.warning(f"Timeout attempt {attempt+1}: {url[:60]}")
            await asyncio.sleep(1)
        except Exception as e:
            logger.warning(f"Fetch error attempt {attempt+1} [{url[:60]}]: {e}")
            await asyncio.sleep(1)
    return None

async def cg(endpoint: str, params: dict = None):
    return await _fetch(f"{CG_BASE}{endpoint}", {"x-cg-pro-api-key": CG_KEY}, params or {})

async def gl(endpoint: str, params: dict = None):
    return await _fetch(f"{GLASS_BASE}{endpoint}", {"CG-API-KEY": GLASS_KEY}, params or {})

async def ll(endpoint: str):
    return await _fetch(f"{LLAMA_BASE}{endpoint}", {}, {})

async def ll_yields_fetch(top: int = 8) -> list | None:
    result = await _fetch(f"{YIELDS_BASE}/pools", {}, {})
    if result and isinstance(result, dict) and result.get("data"):
        pools = [
            p for p in result["data"]
            if p.get("tvlUsd", 0) > 1_000_000
            and p.get("stablecoin", False)
        ]
        return sorted(pools, key=lambda p: p.get("tvlUsd", 0), reverse=True)[:top]
    return None

# ── Static file route ─────────────────────────────────────────────────────────
@app.get("/")
async def serve_index():
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return JSONResponse({"error": "index.html not found in static/"}, status_code=404)

# ── Health ─────────────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}

# ── Market endpoint ────────────────────────────────────────────────────────────
@app.get("/api/market")
async def market():
    try:
        prices_task    = cg("/coins/markets", {
            "vs_currency": "usd",
            "ids": "bitcoin,ethereum,solana",
            "price_change_percentage": "1h,24h,7d",
            "sparkline": "false",
        })
        global_task    = cg("/global")
        fng_task       = _fetch(FNG_URL, {}, {})
        top50_task     = cg("/coins/markets", {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": "50",
            "page": "1",
            "price_change_percentage": "1h,24h,7d",
            "sparkline": "false",
        })

        prices_raw, global_raw, fng_raw, top50_raw = await asyncio.gather(
            prices_task, global_task, fng_task, top50_task
        )

        # Build coin map
        coins = {}
        if prices_raw:
            for c in prices_raw:
                sym = c.get("symbol", "").upper()
                coins[sym] = {
                    "id":          c.get("id"),
                    "name":        c.get("name"),
                    "symbol":      sym,
                    "price":       c.get("current_price"),
                    "market_cap":  c.get("market_cap"),
                    "volume_24h":  c.get("total_volume"),
                    "change_1h":   c.get("price_change_percentage_1h_in_currency"),
                    "change_24h":  c.get("price_change_percentage_24h"),
                    "change_7d":   c.get("price_change_percentage_7d_in_currency"),
                    "high_24h":    c.get("high_24h"),
                    "low_24h":     c.get("low_24h"),
                    "ath":         c.get("ath"),
                    "ath_change":  c.get("ath_change_percentage"),
                    "rank":        c.get("market_cap_rank"),
                }

        # Global data
        global_data = {}
        if global_raw and global_raw.get("data"):
            gd = global_raw["data"]
            global_data = {
                "total_market_cap":      gd.get("total_market_cap", {}).get("usd"),
                "total_volume_24h":      gd.get("total_volume", {}).get("usd"),
                "market_cap_change_24h": gd.get("market_cap_change_percentage_24h_usd"),
                "btc_dominance":         gd.get("market_cap_percentage", {}).get("btc"),
                "eth_dominance":         gd.get("market_cap_percentage", {}).get("eth"),
                "active_coins":          gd.get("active_cryptocurrencies"),
            }

            # Stablecoins: sum USDT + USDC + DAI from top50
            stablecoins = []
            stable_ids = {
                "tether":        "USDT",
                "usd-coin":      "USDC",
                "dai":           "DAI",
                "first-digital-usd": "FDUSD",
            }
            if top50_raw:
                for c in top50_raw:
                    if c.get("id") in stable_ids:
                        mc  = c.get("market_cap") or 0
                        vol = c.get("total_volume") or 0
                        stablecoins.append({
                            "symbol":    stable_ids[c["id"]],
                            "name":      c.get("name"),
                            "supply":    mc,
                            "volume_24h": vol,
                            "vm_ratio":  (vol / mc * 100) if mc else 0,
                            "change_24h": c.get("price_change_percentage_24h"),
                        })

            global_data["stablecoins"] = stablecoins

        # Fear & Greed
        fng = {}
        if fng_raw and fng_raw.get("data"):
            latest = fng_raw["data"][0]
            fng = {
                "value":              int(latest.get("value", 0)),
                "value_classification": latest.get("value_classification"),
                "timestamp":          latest.get("timestamp"),
            }

        # Top50 movers
        top_gainers, top_losers = [], []
        btc_7d = 0
        if top50_raw:
            for c in top50_raw:
                if c.get("symbol", "").upper() == "BTC":
                    btc_7d = c.get("price_change_percentage_7d_in_currency") or 0
                    break
            movers = []
            for c in top50_raw:
                ch7 = c.get("price_change_percentage_7d_in_currency")
                if ch7 is None:
                    continue
                movers.append({
                    "symbol":   c.get("symbol", "").upper(),
                    "name":     c.get("name"),
                    "price":    c.get("current_price"),
                    "change_7d": ch7,
                    "vs_btc_7d": ch7 - btc_7d,
                })
            movers.sort(key=lambda x: x["vs_btc_7d"], reverse=True)
            top_gainers = movers[:5]
            top_losers  = movers[-5:][::-1]

        return {
            "coins":        coins,
            "global":       global_data,
            "fear_greed":   fng,
            "top_gainers":  top_gainers,
            "top_losers":   top_losers,
        }
    except Exception as e:
        logger.error(f"/api/market error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

# ── Derivatives endpoint ───────────────────────────────────────────────────────
@app.get("/api/derivatives")
async def derivatives():
    try:
        funding_raw, oi_raw, liq_raw, ls_raw = await asyncio.gather(
            gl("/futures/funding-rate/exchange-list",               {"symbol": "BTC"}),
            gl("/futures/open-interest/exchange-list",              {"symbol": "BTC"}),
            gl("/futures/liquidation/aggregated-history", {
                "exchange_list": "Binance",
                "symbol":        "BTC",
                "interval":      "1d",
                "limit":         "3",
            }),
            gl("/futures/global-long-short-account-ratio/history", {
                "exchange": "Binance",
                "symbol":   "BTCUSDT",
                "interval": "4h",
            }),
        )

        # ── Funding rates ──
        funding = {"exchanges": [], "avg": None, "bias": "NEUTRAL"}
        if funding_raw and funding_raw.get("data"):
            raw = funding_raw["data"]
            # data is list of {symbol, stablecoin_margin_list: [{exchange, funding_rate}]}
            coin_obj = {}
            if isinstance(raw, list):
                coin_obj = next((x for x in raw if x.get("symbol", "").upper() == "BTC"), raw[0] if raw else {})
            else:
                coin_obj = raw
            exchanges = (
                coin_obj.get("stablecoin_margin_list") or
                coin_obj.get("usdtMarginList") or
                (raw if isinstance(raw, list) else [])
            )
            MAJOR = {"Binance", "OKX", "Bybit", "Bitget", "dYdX", "Hyperliquid", "Gate", "MEXC", "HTX", "Kraken"}
            total, count = 0.0, 0
            rows = []
            for ex in exchanges:
                name = ex.get("exchangeName") or ex.get("exchange") or "?"
                rate = ex.get("fundingRate") if ex.get("fundingRate") is not None else ex.get("funding_rate")
                if rate is None:
                    continue
                try:
                    r = float(rate) * 100
                    rows.append({"exchange": name, "rate": r})
                    if name in MAJOR:
                        total += r
                        count += 1
                except (TypeError, ValueError):
                    pass
            rows.sort(key=lambda x: abs(x["rate"]), reverse=True)
            funding["exchanges"] = rows
            if count:
                avg = total / count
                funding["avg"] = avg
                funding["bias"] = (
                    "CROWDED LONG"   if avg > 0.08 else
                    "CROWDED SHORT"  if avg < -0.03 else
                    "NEUTRAL"
                )

        # ── Open interest ──
        oi = {"total": 0, "exchanges": []}
        if oi_raw and oi_raw.get("data"):
            items = oi_raw["data"]
            if not isinstance(items, list):
                items = [items]
            total_oi = sum(float(x.get("open_interest_usd") or x.get("openInterestUsd") or 0) for x in items)
            oi["total"] = total_oi
            ex_list = []
            for x in items:
                name = x.get("exchange") or x.get("exchangeName") or "?"
                val  = float(x.get("open_interest_usd") or x.get("openInterestUsd") or 0)
                if val > 0:
                    ex_list.append({"exchange": name, "oi": val, "share": (val / total_oi * 100) if total_oi else 0})
            ex_list.sort(key=lambda x: x["oi"], reverse=True)
            oi["exchanges"] = ex_list[:10]

        # ── Long/Short ──
        longshort = {"long_pct": None, "short_pct": None, "ratio": None}
        if ls_raw and ls_raw.get("data"):
            items = ls_raw["data"]
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
                longshort = {"long_pct": lr, "short_pct": sr, "ratio": ratio}

        # ── Liquidations ──
        liquidations = {"total": 0, "longs": 0, "shorts": 0, "bias": "balanced"}
        if liq_raw and liq_raw.get("data"):
            items = liq_raw["data"]
            if isinstance(items, list) and items:
                try:
                    ts_key = "timestamp" if "timestamp" in items[0] else "time"
                    items = sorted(items, key=lambda x: x.get(ts_key, 0), reverse=True)
                except Exception:
                    pass
                d = items[0]
                long_usd  = float(d.get("aggregated_long_liquidation_usd") or
                                  d.get("aggregatedLongUsd") or
                                  d.get("long_liquidation_usd") or 0)
                short_usd = float(d.get("aggregated_short_liquidation_usd") or
                                  d.get("aggregatedShortUsd") or
                                  d.get("short_liquidation_usd") or 0)
                total_usd = long_usd + short_usd
                bias = (
                    "long-heavy"  if long_usd > short_usd * 1.5 else
                    "short-heavy" if short_usd > long_usd * 1.5 else
                    "balanced"
                )
                liquidations = {
                    "total":  total_usd,
                    "longs":  long_usd,
                    "shorts": short_usd,
                    "bias":   bias,
                }

        return {
            "funding":      funding,
            "oi":           oi,
            "longshort":    longshort,
            "liquidations": liquidations,
        }
    except Exception as e:
        logger.error(f"/api/derivatives error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

# ── DeFi endpoint ──────────────────────────────────────────────────────────────
@app.get("/api/defi")
async def defi():
    try:
        tvl_task      = ll("/v2/historicalChainTvl")
        protocols_task = ll("/protocols")
        chains_task   = ll("/v2/chains")
        dex_task      = _fetch(
            f"{LLAMA_BASE}/overview/dexs", {},
            {"excludeTotalDataChart": "true", "excludeTotalDataChartBreakdown": "true"},
        )
        yields_task   = ll_yields_fetch(8)

        tvl_raw, protocols_raw, chains_raw, dex_raw, yields_raw = await asyncio.gather(
            tvl_task, protocols_task, chains_task, dex_task, yields_task
        )

        # Total TVL — last data point from historical
        total_tvl, tvl_change_24h = 0, 0
        if tvl_raw and isinstance(tvl_raw, list) and len(tvl_raw) >= 2:
            total_tvl      = tvl_raw[-1].get("tvl", 0)
            prev_tvl       = tvl_raw[-2].get("tvl", 0)
            tvl_change_24h = ((total_tvl - prev_tvl) / prev_tvl * 100) if prev_tvl else 0

        # Top 10 protocols
        top_protocols = []
        if protocols_raw and isinstance(protocols_raw, list):
            valid = [p for p in protocols_raw if p.get("tvl") and p.get("tvl") > 0]
            valid.sort(key=lambda x: x.get("tvl", 0), reverse=True)
            for p in valid[:10]:
                top_protocols.append({
                    "name":       p.get("name"),
                    "symbol":     p.get("symbol", "").upper(),
                    "tvl":        p.get("tvl"),
                    "change_1d":  p.get("change_1d"),
                    "change_7d":  p.get("change_7d"),
                    "chain":      p.get("chain"),
                    "category":   p.get("category"),
                })

        # Top 8 chains by TVL
        top_chains = []
        if chains_raw and isinstance(chains_raw, list):
            valid = [c for c in chains_raw if c.get("tvl") and c.get("tvl") > 0]
            valid.sort(key=lambda x: x.get("tvl", 0), reverse=True)
            for c in valid[:8]:
                top_chains.append({
                    "name":      c.get("name"),
                    "tvl":       c.get("tvl"),
                    "change_1d": c.get("change_1d"),
                    "change_7d": c.get("change_7d"),
                })

        # Top 8 DEX volumes
        top_dex = []
        if dex_raw and dex_raw.get("protocols"):
            dex_list = dex_raw["protocols"]
            dex_list.sort(key=lambda x: x.get("totalVolume24h") or x.get("total24h") or 0, reverse=True)
            for d in dex_list[:8]:
                vol = d.get("totalVolume24h") or d.get("total24h") or 0
                top_dex.append({
                    "name":       d.get("name"),
                    "chain":      d.get("chain"),
                    "volume_24h": vol,
                    "change_1d":  d.get("change_1d"),
                })

        # Top 8 yield pools (stablecoins only, filtered in ll_yields_fetch)
        top_yields = []
        if yields_raw:
            for p in yields_raw:
                top_yields.append({
                    "project":  p.get("project"),
                    "symbol":   p.get("symbol"),
                    "chain":    p.get("chain"),
                    "tvl":      p.get("tvlUsd"),
                    "apy":      p.get("apy"),
                    "apy_base": p.get("apyBase"),
                    "apy_reward": p.get("apyReward"),
                })

        return {
            "total_tvl":      total_tvl,
            "tvl_change_24h": tvl_change_24h,
            "top_protocols":  top_protocols,
            "top_chains":     top_chains,
            "top_dex":        top_dex,
            "top_yields":     top_yields,
        }
    except Exception as e:
        logger.error(f"/api/defi error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

# ── ETF endpoint ───────────────────────────────────────────────────────────────
@app.get("/api/etf")
async def etf():
    try:
        btc_price_task = cg("/coins/markets", {
            "vs_currency": "usd",
            "ids": "bitcoin",
            "price_change_percentage": "1h,24h,7d",
            "sparkline": "false",
        })
        flows_task = gl("/etf/bitcoin/flow-history", {"limit": "30"})
        list_task  = gl("/etf/bitcoin/list")

        btc_raw, flows_raw, list_raw = await asyncio.gather(
            btc_price_task, flows_task, list_task
        )

        # BTC price context
        btc_price = {}
        if btc_raw and isinstance(btc_raw, list) and btc_raw:
            b = btc_raw[0]
            btc_price = {
                "price":      b.get("current_price"),
                "change_24h": b.get("price_change_percentage_24h"),
                "change_7d":  b.get("price_change_percentage_7d_in_currency"),
                "market_cap": b.get("market_cap"),
            }

        # ETF flows — data is list of {timestamp, flow_usd, price_usd}
        flows = []
        total_7d_flow = 0
        if flows_raw and flows_raw.get("data"):
            raw = flows_raw["data"]
            if isinstance(raw, list):
                raw_sorted = sorted(raw, key=lambda x: x.get("timestamp", 0), reverse=True)
                for i, item in enumerate(raw_sorted[:30]):
                    ts       = item.get("timestamp", 0)
                    flow_usd = item.get("flow_usd") or item.get("flowUsd") or 0
                    price    = item.get("price_usd") or item.get("priceUsd") or 0
                    date_str = datetime.fromtimestamp(
                        ts / 1000 if ts > 1e10 else ts, tz=timezone.utc
                    ).strftime("%Y-%m-%d") if ts else "?"
                    flows.append({
                        "date":      date_str,
                        "flow_usd":  float(flow_usd),
                        "price_usd": float(price),
                        "timestamp": ts,
                    })
                    if i < 7:
                        total_7d_flow += float(flow_usd)

        # ETF list — data is list of {ticker, fund_name, region, market_status, primary_exchange}
        etf_list = []
        if list_raw and list_raw.get("data"):
            raw = list_raw["data"]
            if isinstance(raw, list):
                for item in raw:
                    etf_list.append({
                        "ticker":    item.get("ticker"),
                        "fund_name": item.get("fund_name") or item.get("fundName"),
                        "region":    item.get("region"),
                        "exchange":  item.get("primary_exchange") or item.get("primaryExchange"),
                        "status":    item.get("market_status") or item.get("marketStatus"),
                        "aum_usd":   item.get("aum_usd") or item.get("aumUsd"),
                    })

        return {
            "btc_price":     btc_price,
            "flows":         flows,
            "total_7d_flow": total_7d_flow,
            "etf_list":      etf_list,
        }
    except Exception as e:
        logger.error(f"/api/etf error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

# ── Dominance endpoint ─────────────────────────────────────────────────────────
@app.get("/api/dominance")
async def dominance():
    try:
        raw = await gl("/index/bitcoin-dominance")
        history = []
        if raw and raw.get("data"):
            items = raw["data"]
            if isinstance(items, list):
                # data is list of {timestamp, price, bitcoin_dominance, market_cap}
                items_sorted = sorted(items, key=lambda x: x.get("timestamp", 0))
                # Last 30 data points
                for item in items_sorted[-30:]:
                    ts  = item.get("timestamp", 0)
                    date_str = datetime.fromtimestamp(
                        ts / 1000 if ts > 1e10 else ts, tz=timezone.utc
                    ).strftime("%Y-%m-%d") if ts else "?"
                    history.append({
                        "date":        date_str,
                        "timestamp":   ts,
                        "price":       item.get("price"),
                        "dominance":   item.get("bitcoin_dominance") or item.get("bitcoinDominance"),
                        "market_cap":  item.get("market_cap") or item.get("marketCap"),
                    })
        latest = history[-1] if history else {}
        return {
            "current_dominance": latest.get("dominance"),
            "current_price":     latest.get("price"),
            "history":           history,
        }
    except Exception as e:
        logger.error(f"/api/dominance error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

# ── AI analyze endpoint ────────────────────────────────────────────────────────
class AnalyzeRequest(BaseModel):
    prompt:  str
    context: str = ""

@app.post("/api/analyze")
async def analyze(req: AnalyzeRequest):
    if not GEMINI_KEY:
        return JSONResponse({"error": "GEMINI_API_KEY not configured"}, status_code=503)
    try:
        model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            system_instruction=GEMINI_SYSTEM,
        )
        full_prompt = req.prompt
        if req.context:
            full_prompt = f"LIVE MARKET DATA:\n{req.context}\n\nQUESTION / TASK:\n{req.prompt}"

        response = await asyncio.to_thread(model.generate_content, full_prompt)
        return {"result": response.text}
    except Exception as e:
        logger.error(f"/api/analyze error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=False)
