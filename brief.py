"""
Daily Market Brief
- Reads topic config from Google Sheet "Config" tab
- Fetches market data + news (Anthropic / OpenAI / Gemini — switchable)
- Writes to Google Sheet + sends email via Resend
"""

import requests
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY     = os.environ.get("OPENAI_API_KEY", "")
GEMINI_API_KEY     = os.environ.get("GEMINI_API_KEY", "")
AI_PROVIDER        = os.environ.get("AI_PROVIDER", "anthropic").lower()

SHEETS_WEBHOOK_URL = os.environ["SHEETS_WEBHOOK_URL"]
WEBHOOK_SECRET     = os.environ["WEBHOOK_SECRET"]
TO_EMAIL           = os.environ.get("TO_EMAIL", "")  # Gmail address to receive the brief

TAIPEI_TZ = timezone(timedelta(hours=8))

# ── AI provider abstraction ───────────────────────────────────────────────────
def ai_call(prompt, use_search=False, max_tokens=800):
    """Unified AI call — switches provider based on AI_PROVIDER env var."""

    if AI_PROVIDER == "openai":
        import openai
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        tools = [{"type": "web_search_preview"}] if use_search else []
        kwargs = dict(model="gpt-4o-mini", max_output_tokens=max_tokens,
                      input=[{"role":"user","content":prompt}])
        if tools: kwargs["tools"] = tools
        resp = client.responses.create(**kwargs)
        return resp.output_text

    elif AI_PROVIDER == "gemini":
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
        tools = [{"google_search": {}}] if use_search else []
        body = {"contents": [{"parts": [{"text": prompt}]}]}
        if tools: body["tools"] = tools
        resp = requests.post(url, json=body, timeout=120)
        resp.raise_for_status()
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"]

    else:  # anthropic (default)
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=120.0)
        kwargs = dict(model="claude-haiku-4-5-20251001", max_tokens=max_tokens,
                      messages=[{"role":"user","content":prompt}])
        if use_search:
            kwargs["tools"] = [{"type": "web_search_20250305", "name": "web_search"}]
        msg = client.messages.create(**kwargs)
        return "".join(b.text for b in msg.content if hasattr(b, "text"))


def clean_text(text):
    """Strip citation tags and stray HTML."""
    if not text: return text
    text = re.sub(r'<cite[^>]*>(.*?)</cite>', r'\1', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# ── 1. Read config from Google Sheet ─────────────────────────────────────────
def fetch_config():
    print(f"📋 Reading config from Google Sheet (AI provider: {AI_PROVIDER})...")
    url = f"{SHEETS_WEBHOOK_URL}?action=get_config&secret={WEBHOOK_SECRET}"
    try:
        r = requests.get(url, timeout=15)
        data = r.json()
        if data.get("status") != "ok":
            raise ValueError(data.get("message", "Unknown error"))
        topics = [t for t in data["topics"] if t.get("active")]
        print(f"✅ Loaded {len(topics)} active topics from Config tab")
        return topics
    except Exception as e:
        print(f"⚠️ Could not read Config tab: {e} — using hardcoded defaults")
        return [
            {"topic": t, "keywords": k} for t, k in {
                "Nvidia": '"Jensen Huang" OR "Blackwell" OR "Rubin"',
                "AMD": '"Lisa Su" OR "Ryzen AI" OR "Zen 6"',
                "Google": '"Gemini" OR "DeepMind" OR "Antitrust"',
                "Tesla (Motors, Energy & Robotics)": '"Cybercab" OR "Robotaxi" OR "FSD" OR "Optimus" OR "Megapack"',
                "TSMC": '"C.C. Wei" OR "2nm" OR "CoWoS" OR "Arizona"',
                "Bitcoin": '"OP_CAT" OR "BitVM" OR "ETF" OR "Bitcoin price"',
                "Ethereum": '"Pectra" OR "PeerDAS" OR "Blob" OR "Ethereum upgrade"',
                "Cardano": '"Hoskinson" OR "Leios" OR "Midnight" OR "Chang"',
                "RWA (Real World Assets)": '"BlackRock" OR "Ondo" OR "Securitize" OR "Tokenized Treasuries"',
                "DeFi & Stablecoin": '"Uniswap" OR "Aave" OR "USDC" OR "Restaking"',
                "Crypto Regulation": '"Paul Atkins" OR "Clarity Act" OR "SAB 121" OR "CFTC"',
            }.items()
        ]


# ── 2. Market data ────────────────────────────────────────────────────────────
def fetch_market_data():
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
            r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d",
                             headers=headers, timeout=15)
            meta = r.json()["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice") or meta.get("previousClose")
            if not price: raise ValueError("No price")
            prev  = meta.get("chartPreviousClose") or meta.get("previousClose") or price
            pct   = ((price - prev) / prev * 100) if prev else 0
            sign  = "+" if pct >= 0 else ""
            results[name] = {"price": fmt(price), "change_pct": pct,
                             "change_label": f"{sign}{pct:.2f}%",
                             "arrow": "▲" if pct>0 else ("▼" if pct<0 else "—"), "note": note}
            print(f"  ✓ {name}: {fmt(price)} ({sign}{pct:.2f}%)")
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            results[name] = {"price":"N/A","change_pct":0,"change_label":"—","arrow":"—","note":note}
    return results


def fetch_fear_greed():
    try:
        d = requests.get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata",timeout=10).json()
        return {"score":round(d["fear_and_greed"]["score"]),"label":d["fear_and_greed"]["rating"].replace("_"," ").title()}
    except: return {"score":None,"label":"Unavailable"}

def fetch_crypto_fear_greed():
    try:
        d = requests.get("https://api.alternative.me/fng/?limit=1",timeout=10).json()
        return {"score":int(d["data"][0]["value"]),"label":d["data"][0]["value_classification"]}
    except: return {"score":None,"label":"Unavailable"}

def fetch_us_cpi():
    try:
        lines = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL",timeout=10).text.strip().split("\n")
        last,prev = lines[-1].split(","), lines[-13].split(",")
        yoy = ((float(last[1])-float(prev[1]))/float(prev[1]))*100
        return {"value":f"{float(last[1]):.1f}","yoy":f"{yoy:.1f}%","date":last[0]}
    except: return {"value":"N/A","yoy":"N/A","date":"N/A"}


# ── 3. News ───────────────────────────────────────────────────────────────────
def fetch_must_know(today_str):
    print("🌍 Fetching must-know headlines...")
    prompt = f"""Today is {today_str}. Search for the top 5 must-know global news stories for investors.
Focus on: macro-economic events, geopolitics, central bank decisions, trade policy.
Exclude: Nvidia, AMD, Google, Tesla, TSMC, Bitcoin, Ethereum, crypto, DeFi.
Plain text only — no <cite> tags, no XML, no markdown.

Respond ONLY with a JSON array:
[{{"headline":"short headline","detail":"one neutral sentence","category":"Macro|Geopolitics|Trade|Energy|Finance|Policy","url":"source URL or null"}}]"""

    print("⏳ Waiting 30s...")
    time.sleep(30)
    text  = clean_text(ai_call(prompt, use_search=True, max_tokens=800).replace("```json","").replace("```","").strip())
    items = json.loads(text[text.find("["):text.rfind("]")+1])
    for item in items:
        item["headline"] = clean_text(item.get("headline",""))
        item["detail"]   = clean_text(item.get("detail",""))
    print(f"✅ Got {len(items)} must-know items")

    summary = clean_text(ai_call(
        f"2-3 neutral sentences summarizing macro/geopolitical backdrop today based on: {json.dumps([i['headline'] for i in items])}. Plain text only.",
        max_tokens=200
    ).strip())
    return {"summary": summary, "items": items}


def fetch_topic_news(today_str, topic_configs):
    BATCH_SIZE = 5
    batches    = [topic_configs[i:i+BATCH_SIZE] for i in range(0, len(topic_configs), BATCH_SIZE)]
    all_results = {}

    for i, batch in enumerate(batches):
        if i > 0:
            print(f"⏳ Waiting 15s before batch {i+1}...")
            time.sleep(15)

        topic_lines = "\n".join([f'- {t["topic"]}: {t["keywords"]}' for t in batch])
        names = [t["topic"] for t in batch]
        prompt = f"""Today is {today_str}. Search for latest news (last 48h) using these queries:

{topic_lines}

For each topic return 2-3 bullets tagged BULLISH, BEARISH, or NEUTRAL.
If you searched but found NO relevant news, use: {{"sentiment":"neutral","text":"No relevant news found in last 48h.","url":null}}
If search FAILED due to an error, use: {{"sentiment":"neutral","text":"Search unavailable for this topic.","url":null}}
Plain text only — no <cite> tags, no XML.

Respond ONLY with JSON:
{{"{names[0]}":[{{"sentiment":"bullish","text":"plain summary","url":"URL or null"}}],...}}"""

        try:
            text = clean_text(ai_call(prompt, use_search=True, max_tokens=1500).replace("```json","").replace("```","").strip())
            batch_data = json.loads(text[text.find("{"):text.rfind("}")+1])
            for topic, items in batch_data.items():
                for item in items:
                    item["text"] = clean_text(item.get("text",""))
            all_results.update(batch_data)
            print(f"✅ Batch done: {names}")
        except Exception as e:
            print(f"⚠️ Batch failed {names}: {e}")
            for t in batch:
                all_results[t["topic"]] = [{"sentiment":"neutral","text":"Search unavailable for this topic.","url":None}]

    return all_results


# ── 4. Build payload ──────────────────────────────────────────────────────────
def build_payload(today_str, now_str, must_know, topic_news, topic_configs, market_data, fg, cfg, cpi):
    def mkt(name):
        d = market_data.get(name,{})
        return {"name":name,"price":d.get("price","N/A"),"change_pct":d.get("change_pct",0),
                "change_label":d.get("change_label","—"),"note":d.get("note","")}

    macro = [mkt("VIX"), mkt("DXY Dollar Index"),
             {"name":"CNN Fear & Greed","price":str(fg.get("score","—")),"change_pct":0,"change_label":"","note":fg.get("label","")},
             {"name":"Crypto Fear & Greed","price":str(cfg.get("score","—")),"change_pct":0,"change_label":"","note":cfg.get("label","")},
             {"name":"US CPI (YoY)","price":cpi.get("yoy","N/A"),"change_pct":0,"change_label":f"as of {cpi.get('date','')}","note":""}]

    topics_out = []
    for tc in topic_configs:
        name  = tc["topic"]
        items = topic_news.get(name,[])
        b = sum(1 for i in items if i.get("sentiment")=="bullish")
        r = sum(1 for i in items if i.get("sentiment")=="bearish")
        topics_out.append({"name":name,
                            "overall":"bullish" if b>r else ("bearish" if r>b else "neutral"),
                            "items":items})

    return {
        "secret": WEBHOOK_SECRET, "date": today_str, "generated_at": now_str,
        "to_email": TO_EMAIL,
        "must_know": must_know,
        "market": {
            "equity":      [mkt(n) for n in ["S&P 500","Nasdaq 100","Nikkei 225","Hang Seng","DAX"]],
            "commodities": [mkt(n) for n in ["WTI Crude Oil","Natural Gas","Gold","Silver"]],
            "macro":       macro,
        },
        "topics": topics_out,
    }


# ── 5. Post to Google Sheets ──────────────────────────────────────────────────
def post_to_sheets(payload):
    print("📊 Posting to Google Sheets...")
    resp = requests.post(SHEETS_WEBHOOK_URL, json=payload, timeout=60)
    if resp.status_code == 200:
        result = resp.json()
        status = result.get("status")
        print(f"✅ Sheet updated! Rows: {result.get('rows')}" if status=="ok" else f"⚠️ Apps Script: {result.get('message')}")
    else:
        raise Exception(f"Webhook failed {resp.status_code}: {resp.text}")


# ── 6. Main ───────────────────────────────────────────────────────────────────
def main():
    now       = datetime.now(TAIPEI_TZ)
    today_str = now.strftime("%A, %B %-d, %Y")
    now_str   = now.strftime("%I:%M %p TPE")
    print(f"🚀 Running Daily Brief for {today_str} via {AI_PROVIDER.upper()}")

    topic_configs = fetch_config()

    print("📈 Fetching market data...")
    try:    market_data = fetch_market_data()
    except Exception as e: print(f"⚠️ {e}"); market_data = {}
    try:    fg  = fetch_fear_greed()
    except: fg  = {"score":None,"label":"Unavailable"}
    try:    cfg = fetch_crypto_fear_greed()
    except: cfg = {"score":None,"label":"Unavailable"}
    try:    cpi = fetch_us_cpi()
    except: cpi = {"value":"N/A","yoy":"N/A","date":"N/A"}

    print("🌍 Fetching must-know news...")
    try:    must_know = fetch_must_know(today_str)
    except Exception as e: print(f"⚠️ {e}"); must_know = {"summary":"Could not fetch news today.","items":[]}

    print("📰 Fetching topic news...")
    try:    topic_news = fetch_topic_news(today_str, topic_configs)
    except Exception as e: print(f"⚠️ {e}"); topic_news = {}

    payload = build_payload(today_str, now_str, must_know, topic_news, topic_configs, market_data, fg, cfg, cpi)

    try:
        post_to_sheets(payload)
        if TO_EMAIL:
            print("📧 Email will be sent by Google Apps Script via Gmail")
        else:
            print("⏭ No TO_EMAIL set — skipping email")
    except Exception as e: print(f"❌ Sheets: {e}")

    print("✅ Done!")

if __name__ == "__main__":
    main()
