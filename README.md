# Tetranomio Telegram Bot

A Telegram bot (@tetranomio_bot) that pulls crypto market data from a handful of sources and uses an LLM to turn it into readable analysis and trade setups.

## Data sources

Price, volume, market cap and dominance data comes from CoinGecko Pro. Funding rates, open interest, liquidations and long/short ratios come from CoinGlass Pro. TVL by protocol and chain comes from DeFiLlama. The Fear & Greed Index comes from Alternative.me. Groq's Llama 3.3 70B model handles the analysis, synthesis and trade setup generation, with Gemini available as a secondary model.

## Running it

```
pip install -r requirements.txt
python bot.py
```

You'll need your own API keys for Telegram, Groq, CoinGecko, CoinGlass and Gemini set as environment variables (check `bot.py` for the exact variable names it expects).

Set up to deploy on Railway or Render, config files for both (`railway.toml`, `render.yaml`, `nixpacks.toml`) are included.
