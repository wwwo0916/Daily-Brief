"""
Daily Market Brief — Email Generator
Runs every morning, fetches live data via Anthropic API, sends HTML email.
"""

import anthropic
import requests
import json
import os
import time
from datetime import datetime, timezone, timedelta

# ── Config (loaded from environment variables) ────────────────────────────────
ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
SHEETS_WEBHOOK_URL = os.environ["SHEETS_WEBHOOK_URL"]
WEBHOOK_SECRET     = os.environ["WEBHOOK_SECRET"]

# ── Tracked topics ────────────────────────────────────────────────────────────
TOPICS = [
    "Nvidia", "AMD", "Google", "Tesla (Motors, Energy & Robotics)",
    "TSMC", "Bitcoin", "Ethereum", "Cardano", "RWA (Real World Assets)",
    "DeFi & Stablecoin", "Crypto Regulation"
]

TAIPEI_TZ = timezone(timedelta(hours=8))


# ── 1. Fetch market data via yfinance ─────────────────────────────────────────
def fetch_market_data():
    """Fetch via Yahoo Finance query API directly — no yfinance dependency."""
    SYMBOLS = {
        "S&P 500":          ("^GSPC",  lambda v: f"{v:,.2f}",  ""),
        "Nasdaq 100":       ("^NDX",   lambda v: f"{v:,.2f}",  ""),
        "Nikkei 225":       ("^N225",  lambda v: f"{v:,.2f}",  ""),
        "Hang Seng":        ("^HSI",   lambda v: f"{v:,.2f}",  ""),
        "DAX":              ("^GDAXI", lambda v: f"{v:,.2f}",  ""),
        "WTI Crude Oil":    ("CL=F",   lambda v: f"${v:.2f}",  "per barrel"),
        "Natural Gas":      ("NG=F",   lambda v: f"${v:.3f}",  "Henry Hub"),
        "Gold":             ("GC=F",   lambda v: f"${v:,.0f}", "per oz"),
        "Silver":           ("SI=F",   lambda v: f"${v:.2f}",  "per oz"),
        "VIX":              ("^VIX",   lambda v: f"{v:.2f}",   ">20 = elevated"),
        "DXY Dollar Index": ("DX=F",   lambda v: f"{v:.2f}",   ""),
    }
    results = {}
    headers = {"User-Agent": "Mozilla/5.0"}
    for name, (sym, fmt, note) in SYMBOLS.items():
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
            r = requests.get(url, headers=headers, timeout=15)
            data = r.json()
            result = data.get("chart", {}).get("result")
            if not result:
                raise ValueError("No result in response")
            meta = result[0].get("meta", {})
            price = meta.get("regularMarketPrice") or meta.get("previousClose")
            if price is None:
                raise ValueError("No price found")
            prev = meta.get("chartPreviousClose") or meta.get("previousClose") or price
            change_pct = ((price - prev) / prev * 100) if prev else 0
            sign  = "+" if change_pct >= 0 else ""
            arrow = "▲" if change_pct > 0 else ("▼" if change_pct < 0 else "—")
            results[name] = {
                "price": fmt(price),
                "change_pct": change_pct,
                "change_label": f"{sign}{change_pct:.2f}%",
                "arrow": arrow,
                "note": note,
            }
            print(f"  ✓ {name}: {fmt(price)} ({sign}{change_pct:.2f}%)")
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            results[name] = {"price": "N/A", "change_pct": 0, "change_label": "—", "arrow": "—", "note": note}
    return results


def fetch_fear_greed():
    """CNN Fear & Greed Index"""
    try:
        r = requests.get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata", timeout=10)
        data = r.json()
        score = round(data["fear_and_greed"]["score"])
        label = data["fear_and_greed"]["rating"].replace("_", " ").title()
        return {"score": score, "label": label}
    except:
        return {"score": None, "label": "Unavailable"}


def fetch_crypto_fear_greed():
    """Crypto Fear & Greed Index"""
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        data = r.json()
        score = int(data["data"][0]["value"])
        label = data["data"][0]["value_classification"]
        return {"score": score, "label": label}
    except:
        return {"score": None, "label": "Unavailable"}


def fetch_us_cpi():
    """Latest US CPI from FRED (free, no API key needed for this endpoint)"""
    try:
        r = requests.get(
            "https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL",
            timeout=10
        )
        lines = r.text.strip().split("\n")
        last = lines[-1].split(",")
        date_str, value = last[0], float(last[1])
        # Calculate YoY change
        prev_year = lines[-13].split(",")
        prev_value = float(prev_year[1])
        yoy = ((value - prev_value) / prev_value) * 100
        return {"value": f"{value:.1f}", "yoy": f"{yoy:.1f}%", "date": date_str}
    except:
        return {"value": "N/A", "yoy": "N/A", "date": "N/A"}


# ── 2. Fetch news via Anthropic API with web search ───────────────────────────
def fetch_must_know(client, today_str):
    print("🌍 Fetching must-know headlines...")
    prompt = f"""Today is {today_str}. Search for the top 5 must-know global news stories for investors today.
Focus on: macro-economic events, geopolitical developments, central bank decisions, trade policy.
Exclude: Nvidia, AMD, Google, Tesla, TSMC, Bitcoin, Ethereum, crypto, DeFi (those are covered separately).

Respond ONLY with a JSON array, no markdown:
[
  {{"headline": "short headline", "detail": "one neutral factual sentence", "category": "Macro|Geopolitics|Trade|Energy|Finance|Policy", "url": "source URL or null"}}
]"""
    print("⏳ Waiting 30s before must-know fetch...")
    time.sleep(30)
    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        print(f"❌ Anthropic API error in fetch_must_know: {type(e).__name__}: {e}")
        raise
    text = "".join(b.text for b in message.content if hasattr(b, "text"))
    text = text.replace("```json", "").replace("```", "").strip()
    start, end = text.find("["), text.rfind("]")
    items = json.loads(text[start:end+1])
    print(f"✅ Got {len(items)} must-know items")
    # One-line summary
    summary_msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        messages=[{"role": "user", "content": f"In 2-3 neutral sentences, summarize the macro/geopolitical backdrop for investors today based on: {json.dumps([i['headline'] for i in items])}"}],
    )
    summary = summary_msg.content[0].text.strip()
    return {"summary": summary, "items": items}


def fetch_topic_news(client, today_str):
    """Fetch news in 3 batches of ~4 topics to avoid timeout."""
    BATCHES = [
        ["Nvidia", "AMD", "Google", "Tesla (Motors, Energy & Robotics)", "TSMC"],
        ["Bitcoin", "Ethereum", "Cardano", "RWA (Real World Assets)", "DeFi & Stablecoin", "Crypto Regulation"],
    ]
    all_results = {}
    for i, batch in enumerate(BATCHES):
        if i > 0:
            print(f"⏳ Waiting 15s before batch {i+1}...")
            time.sleep(15)
        topics_str = ", ".join(batch)
        prompt = f"""Today is {today_str}. Search for the latest news (last 24h) for these topics: {topics_str}.

For each topic return 2-3 bullet points tagged [BULLISH], [BEARISH], or [NEUTRAL].
If no news found for a topic, return one NEUTRAL bullet saying so.

Respond ONLY with a JSON object, no markdown, no extra text:
{{
  "{batch[0]}": [{{"sentiment": "bullish", "text": "...", "url": "URL or null"}}],
  ...
}}"""
        try:
            message = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1200,
                messages=[{"role": "user", "content": prompt}],
            )
            text = "".join(b.text for b in message.content if hasattr(b, "text"))
            text = text.replace("```json", "").replace("```", "").strip()
            start, end = text.find("{"), text.rfind("}")
            if start != -1 and end != -1:
                batch_data = json.loads(text[start:end+1])
                all_results.update(batch_data)
                print(f"✅ Batch done: {batch}")
        except Exception as e:
            print(f"⚠️ Batch failed ({batch}): {e}")
            for topic in batch:
                all_results[topic] = [{"sentiment": "neutral", "text": "Could not fetch news for this topic.", "url": None}]
    return all_results


# ── 3. Build JSON payload for Google Sheets ───────────────────────────────────
def build_payload(today_str, now_taipei, must_know, topic_news, market_data, fg, cfg, cpi):
    # Equity
    equity = []
    for name in ["S&P 500", "Nasdaq 100", "Nikkei 225", "Hang Seng", "DAX"]:
        d = market_data.get(name, {})
        equity.append({"name": name, "price": d.get("price","N/A"), "change_pct": d.get("change_pct",0), "change_label": d.get("change_label","—")})

    # Commodities
    commodities = []
    for name in ["WTI Crude Oil", "Natural Gas", "Gold", "Silver"]:
        d = market_data.get(name, {})
        commodities.append({"name": name, "price": d.get("price","N/A"), "change_pct": d.get("change_pct",0), "change_label": d.get("change_label","—"), "note": d.get("note","")})

    # Macro
    macro = []
    for name in ["VIX", "DXY Dollar Index"]:
        d = market_data.get(name, {})
        macro.append({"name": name, "price": d.get("price","N/A"), "change_pct": d.get("change_pct",0), "change_label": d.get("change_label","—"), "note": d.get("note","")})
    macro.append({"name": "CNN Fear & Greed", "price": str(fg.get("score","—")), "change_pct": 0, "change_label": "", "note": fg.get("label","")})
    macro.append({"name": "Crypto Fear & Greed", "price": str(cfg.get("score","—")), "change_pct": 0, "change_label": "", "note": cfg.get("label","")})
    macro.append({"name": "US CPI (YoY)", "price": cpi.get("yoy","N/A"), "change_pct": 0, "change_label": f"as of {cpi.get('date','')}", "note": ""})

    # Topics
    topics_out = []
    for topic in TOPICS:
        items = topic_news.get(topic, [])
        b = sum(1 for i in items if i.get("sentiment") == "bullish")
        r = sum(1 for i in items if i.get("sentiment") == "bearish")
        overall = "bullish" if b > r else ("bearish" if r > b else "neutral")
        topics_out.append({"name": topic, "overall": overall, "items": items})

    return {
        "secret": WEBHOOK_SECRET,
        "date": today_str,
        "generated_at": now_taipei,
        "must_know": must_know,
        "market": {"equity": equity, "commodities": commodities, "macro": macro},
        "topics": topics_out
    }


# ── 4. POST to Google Sheets webhook ─────────────────────────────────────────
def post_to_sheets(payload, webhook_url):
    print("📊 Posting to Google Sheets...")
    resp = requests.post(
        webhook_url,
        json=payload,
        timeout=60
    )
    if resp.status_code == 200:
        result = resp.json()
        if result.get("status") == "ok":
            print(f"✅ Google Sheet updated! Rows written: {result.get('rows')}")
        else:
            print(f"⚠️ Apps Script error: {result.get('message')}")
    else:
        print(f"❌ Webhook failed: {resp.status_code} {resp.text}")
        raise Exception(f"Webhook failed: {resp.status_code}")


# ── 5. Main ───────────────────────────────────────────────────────────────────
def main():
    now = datetime.now(TAIPEI_TZ)
    today_str = now.strftime("%A, %B %-d, %Y")
    now_taipei = now.strftime("%I:%M %p TPE")
    print(f"🚀 Running Daily Brief for {today_str}")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=120.0)

    print("📈 Fetching market data...")
    try:
        market_data = fetch_market_data()
    except Exception as e:
        print(f"⚠️ Market data failed: {e}")
        market_data = {}

    try:
        fg = fetch_fear_greed()
    except Exception as e:
        print(f"⚠️ CNN F&G failed: {e}")
        fg = {"score": None, "label": "Unavailable"}

    try:
        cfg = fetch_crypto_fear_greed()
    except Exception as e:
        print(f"⚠️ Crypto F&G failed: {e}")
        cfg = {"score": None, "label": "Unavailable"}

    try:
        cpi = fetch_us_cpi()
    except Exception as e:
        print(f"⚠️ CPI failed: {e}")
        cpi = {"value": "N/A", "yoy": "N/A", "date": "N/A"}

    print("🌍 Fetching must-know news...")
    try:
        must_know = fetch_must_know(client, today_str)
    except Exception as e:
        print(f"⚠️ Must-know failed: {e}")
        must_know = {"summary": "Could not fetch global news today.", "items": []}

    print("📰 Fetching topic news...")
    try:
        topic_news = fetch_topic_news(client, today_str)
    except Exception as e:
        print(f"⚠️ Topic news failed: {e}")
        topic_news = {}

    print("📊 Building payload and posting to Google Sheets...")
    try:
        payload = build_payload(today_str, now_taipei, must_know, topic_news, market_data, fg, cfg, cpi)
        post_to_sheets(payload, SHEETS_WEBHOOK_URL)
    except Exception as e:
        print(f"❌ Google Sheets post failed: {type(e).__name__}: {e}")
        raise

    print("✅ Done!")


if __name__ == "__main__":
    main()

