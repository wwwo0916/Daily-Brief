"""
Daily Market Brief
- Reads topic config (keywords, active flags) from Google Sheet "Config" tab
- Fetches market data + news via Anthropic web search
- Writes results to a dated Google Sheet tab
- Optionally sends HTML email via Resend
"""

import anthropic
import requests
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
SHEETS_WEBHOOK_URL = os.environ["SHEETS_WEBHOOK_URL"]
WEBHOOK_SECRET     = os.environ["WEBHOOK_SECRET"]
RESEND_API_KEY     = os.environ.get("RESEND_API_KEY", "")
TO_EMAIL           = os.environ.get("TO_EMAIL", "")

TAIPEI_TZ = timezone(timedelta(hours=8))


def clean_text(text):
    """Strip citation tags and stray HTML from API responses."""
    if not text:
        return text
    text = re.sub(r'<cite[^>]*>(.*?)</cite>', r'\1', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# ── 1. Read topic config from Google Sheet ────────────────────────────────────
def fetch_config():
    """GET /exec?action=get_config&secret=... → list of {topic, keywords, active}"""
    print("📋 Reading topic config from Google Sheet...")
    url = f"{SHEETS_WEBHOOK_URL}?action=get_config&secret={WEBHOOK_SECRET}"
    try:
        r = requests.get(url, timeout=15)
        data = r.json()
        if data.get("status") != "ok":
            raise ValueError(data.get("message", "Unknown error"))
        topics = [t for t in data["topics"] if t.get("active")]
        print(f"✅ Loaded {len(topics)} active topics from Config sheet")
        return topics
    except Exception as e:
        print(f"⚠️ Could not read config: {e} — using hardcoded defaults")
        return [
            {"topic": "Nvidia",         "keywords": '"Jensen Huang" OR "Blackwell"'},
            {"topic": "AMD",            "keywords": '"Lisa Su" OR "Ryzen AI"'},
            {"topic": "Google",         "keywords": '"Gemini" OR "DeepMind"'},
            {"topic": "Tesla (Motors, Energy & Robotics)", "keywords": '"Robotaxi" OR "Optimus" OR "Megapack"'},
            {"topic": "TSMC",           "keywords": '"C.C. Wei" OR "2nm" OR "CoWoS"'},
            {"topic": "Bitcoin",        "keywords": '"Bitcoin" OR "BTC ETF"'},
            {"topic": "Ethereum",       "keywords": '"Pectra" OR "Ethereum"'},
            {"topic": "Cardano",        "keywords": '"Hoskinson" OR "Cardano"'},
            {"topic": "RWA (Real World Assets)", "keywords": '"BlackRock" OR "Tokenized"'},
            {"topic": "DeFi & Stablecoin", "keywords": '"Uniswap" OR "Aave" OR "USDC"'},
            {"topic": "Crypto Regulation", "keywords": '"Paul Atkins" OR "CFTC"'},
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
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
            r = requests.get(url, headers=headers, timeout=15)
            data = r.json()
            result = data.get("chart", {}).get("result")
            if not result:
                raise ValueError("No result")
            meta  = result[0].get("meta", {})
            price = meta.get("regularMarketPrice") or meta.get("previousClose")
            if not price:
                raise ValueError("No price")
            prev  = meta.get("chartPreviousClose") or meta.get("previousClose") or price
            pct   = ((price - prev) / prev * 100) if prev else 0
            sign  = "+" if pct >= 0 else ""
            results[name] = {
                "price": fmt(price), "change_pct": pct,
                "change_label": f"{sign}{pct:.2f}%",
                "arrow": "▲" if pct > 0 else ("▼" if pct < 0 else "—"),
                "note": note,
            }
            print(f"  ✓ {name}: {fmt(price)} ({sign}{pct:.2f}%)")
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            results[name] = {"price":"N/A","change_pct":0,"change_label":"—","arrow":"—","note":note}
    return results


def fetch_fear_greed():
    try:
        r = requests.get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata", timeout=10)
        d = r.json()
        return {"score": round(d["fear_and_greed"]["score"]), "label": d["fear_and_greed"]["rating"].replace("_"," ").title()}
    except:
        return {"score": None, "label": "Unavailable"}


def fetch_crypto_fear_greed():
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        d = r.json()
        return {"score": int(d["data"][0]["value"]), "label": d["data"][0]["value_classification"]}
    except:
        return {"score": None, "label": "Unavailable"}


def fetch_us_cpi():
    try:
        r = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL", timeout=10)
        lines = r.text.strip().split("\n")
        last  = lines[-1].split(",")
        prev  = lines[-13].split(",")
        yoy   = ((float(last[1]) - float(prev[1])) / float(prev[1])) * 100
        return {"value": f"{float(last[1]):.1f}", "yoy": f"{yoy:.1f}%", "date": last[0]}
    except:
        return {"value":"N/A","yoy":"N/A","date":"N/A"}


# ── 3. News via Anthropic ─────────────────────────────────────────────────────
def fetch_must_know(client, today_str):
    print("🌍 Fetching must-know headlines...")
    prompt = f"""Today is {today_str}. Search for the top 5 must-know global news stories for investors today.
Focus on: macro-economic events, geopolitical developments, central bank decisions, trade policy.
Exclude: Nvidia, AMD, Google, Tesla, TSMC, Bitcoin, Ethereum, crypto, DeFi.
Write plain text only — no <cite> tags, no XML, no markdown.

Respond ONLY with a JSON array:
[{{"headline":"short headline","detail":"one neutral sentence","category":"Macro|Geopolitics|Trade|Energy|Finance|Policy","url":"source URL or null"}}]"""

    print("⏳ Waiting 30s before API call...")
    time.sleep(30)
    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=800,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": prompt}],
    )
    text  = "".join(b.text for b in message.content if hasattr(b, "text"))
    text  = clean_text(text.replace("```json","").replace("```","").strip())
    items = json.loads(text[text.find("["):text.rfind("]")+1])
    for item in items:
        item["headline"] = clean_text(item.get("headline",""))
        item["detail"]   = clean_text(item.get("detail",""))
    print(f"✅ Got {len(items)} must-know items")

    summary_resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        messages=[{"role":"user","content":f"In 2-3 neutral sentences summarize the macro/geopolitical backdrop today based on: {json.dumps([i['headline'] for i in items])}. Plain text only, no tags."}],
    )
    return {"summary": clean_text(summary_resp.content[0].text.strip()), "items": items}


def fetch_topic_news(client, today_str, topic_configs):
    """Fetch in batches of 5, using keywords from the config sheet."""
    BATCH_SIZE = 5
    batches = [topic_configs[i:i+BATCH_SIZE] for i in range(0, len(topic_configs), BATCH_SIZE)]
    all_results = {}

    for i, batch in enumerate(batches):
        if i > 0:
            print(f"⏳ Waiting 15s before batch {i+1}...")
            time.sleep(15)

        topic_lines = "\n".join([
            f'- {t["topic"]}: search for {t["keywords"]}'
            for t in batch
        ])
        names = [t["topic"] for t in batch]
        prompt = f"""Today is {today_str}. Search for latest news (last 48h) for these topics:

{topic_lines}

For each topic return 2-3 bullet points tagged BULLISH, BEARISH, or NEUTRAL.
Write plain text only — absolutely no <cite> tags, no XML tags, no markdown.
If no news found, return one NEUTRAL bullet saying so.

Respond ONLY with JSON:
{{
  "{names[0]}": [{{"sentiment":"bullish","text":"plain text summary","url":"URL or null"}}],
  ...
}}"""
        try:
            message = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1500,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
                messages=[{"role":"user","content":prompt}],
            )
            text = "".join(b.text for b in message.content if hasattr(b, "text"))
            text = clean_text(text.replace("```json","").replace("```","").strip())
            batch_data = json.loads(text[text.find("{"):text.rfind("}")+1])
            # Clean all text fields
            for topic, items in batch_data.items():
                for item in items:
                    item["text"] = clean_text(item.get("text",""))
            all_results.update(batch_data)
            print(f"✅ Batch done: {names}")
        except Exception as e:
            print(f"⚠️ Batch failed {names}: {e}")
            for t in batch:
                all_results[t["topic"]] = [{"sentiment":"neutral","text":"Could not fetch news.","url":None}]

    return all_results


# ── 4. Build payload ──────────────────────────────────────────────────────────
def build_payload(today_str, now_taipei, must_know, topic_news, topic_configs, market_data, fg, cfg, cpi):
    equity = [{"name":n,"price":market_data.get(n,{}).get("price","N/A"),"change_pct":market_data.get(n,{}).get("change_pct",0),"change_label":market_data.get(n,{}).get("change_label","—")} for n in ["S&P 500","Nasdaq 100","Nikkei 225","Hang Seng","DAX"]]
    commodities = [{"name":n,"price":market_data.get(n,{}).get("price","N/A"),"change_pct":market_data.get(n,{}).get("change_pct",0),"change_label":market_data.get(n,{}).get("change_label","—"),"note":market_data.get(n,{}).get("note","")} for n in ["WTI Crude Oil","Natural Gas","Gold","Silver"]]
    macro = []
    for n in ["VIX","DXY Dollar Index"]:
        d = market_data.get(n,{})
        macro.append({"name":n,"price":d.get("price","N/A"),"change_pct":d.get("change_pct",0),"change_label":d.get("change_label","—"),"note":d.get("note","")})
    macro.append({"name":"CNN Fear & Greed","price":str(fg.get("score","—")),"change_pct":0,"change_label":"","note":fg.get("label","")})
    macro.append({"name":"Crypto Fear & Greed","price":str(cfg.get("score","—")),"change_pct":0,"change_label":"","note":cfg.get("label","")})
    macro.append({"name":"US CPI (YoY)","price":cpi.get("yoy","N/A"),"change_pct":0,"change_label":f"as of {cpi.get('date','')}","note":""})

    topics_out = []
    for tc in topic_configs:
        name  = tc["topic"]
        items = topic_news.get(name, [])
        b = sum(1 for i in items if i.get("sentiment")=="bullish")
        r = sum(1 for i in items if i.get("sentiment")=="bearish")
        overall = "bullish" if b>r else ("bearish" if r>b else "neutral")
        topics_out.append({"name":name,"overall":overall,"items":items})

    return {
        "secret": WEBHOOK_SECRET,
        "date": today_str,
        "generated_at": now_taipei,
        "must_know": must_know,
        "market": {"equity":equity,"commodities":commodities,"macro":macro},
        "topics": topics_out,
    }


# ── 5. Post to Google Sheets ──────────────────────────────────────────────────
def post_to_sheets(payload):
    print("📊 Posting to Google Sheets...")
    resp = requests.post(SHEETS_WEBHOOK_URL, json=payload, timeout=60)
    if resp.status_code == 200:
        result = resp.json()
        if result.get("status") == "ok":
            print(f"✅ Sheet updated! Rows written: {result.get('rows')}")
        else:
            print(f"⚠️ Apps Script error: {result.get('message')}")
    else:
        print(f"❌ Webhook failed {resp.status_code}: {resp.text}")
        raise Exception(f"Webhook failed: {resp.status_code}")


# ── 6. Send email via Resend (optional) ───────────────────────────────────────
def build_email_html(payload):
    """Convert the payload into a clean HTML email."""
    p = payload
    sc = {"bullish":("#EAF3DE","#3B6D11","▲"),"bearish":("#FCEBEB","#A32D2D","▼"),"neutral":("#F5F5F5","#666666","•")}
    cat_bg = {"Macro":"#E6F1FB","Geopolitics":"#FCEBEB","Trade":"#FAEEDA","Energy":"#EAF3DE","Finance":"#EEEDFE","Policy":"#F1EFE8"}

    def mkt_row(m):
        pct = m.get("change_pct",0)
        col = "#27AE60" if pct>0 else ("#E74C3C" if pct<0 else "#888")
        note = f'<span style="font-size:10px;color:#aaa"> {m.get("note","")}</span>' if m.get("note") else ""
        return f'<tr><td style="padding:5px 8px;font-weight:600;font-size:13px">{m["name"]}</td><td style="padding:5px 8px;font-size:14px;font-weight:700">{m["price"]}</td><td style="padding:5px 8px;font-weight:700;color:{col};font-size:13px">{m.get("change_label","—")}</td><td style="padding:5px 8px;font-size:10px;color:#aaa">{m.get("note","")}</td></tr>'

    mkt_equity = "".join(mkt_row(m) for m in p["market"]["equity"])
    mkt_comm   = "".join(mkt_row(m) for m in p["market"]["commodities"])
    mkt_macro  = "".join(mkt_row(m) for m in p["market"]["macro"])

    mk_rows = ""
    for item in p["must_know"]["items"]:
        bg = cat_bg.get(item.get("category",""),"#F9F9F9")
        link = f'<a href="{item["url"]}" style="color:#666;font-size:10px">source ↗</a>' if item.get("url") else ""
        mk_rows += f'<tr><td style="padding:10px 12px;background:{bg};border-bottom:1px solid #eee"><div style="display:flex;justify-content:space-between;margin-bottom:4px"><span style="font-size:10px;font-weight:700;padding:1px 7px;border-radius:10px;background:{bg};color:#333">{item.get("category","")}</span>{link}</div><div style="font-weight:700;font-size:13px;margin-bottom:3px">{item.get("headline","")}</div><div style="font-size:12px;color:#555">{item.get("detail","")}</div></td></tr>'

    topic_rows = ""
    for topic in p["topics"]:
        overall = topic.get("overall","neutral")
        bg0,fg0,_ = sc.get(overall,sc["neutral"])
        topic_rows += f'<tr><td style="padding:8px 12px;background:#2C3E50"><span style="color:#fff;font-weight:700;font-size:13px">{topic["name"]}</span><span style="float:right;padding:1px 8px;border-radius:10px;background:{bg0};color:{fg0};font-size:10px;font-weight:700">{overall.upper()}</span></td></tr>'
        for item in topic.get("items",[]):
            s = item.get("sentiment","neutral")
            bg,fg,dot = sc.get(s,sc["neutral"])
            link = f' <a href="{item["url"]}" style="color:#aaa;font-size:10px">↗</a>' if item.get("url") else ""
            topic_rows += f'<tr><td style="padding:7px 12px 7px 20px;background:{bg};border-bottom:1px solid #f0f0f0"><span style="color:{fg};font-weight:700">{dot}</span> <span style="font-size:12px;color:#333">{item.get("text","")}</span>{link}</td></tr>'
        topic_rows += '<tr><td style="height:4px;background:#f9f9f9"></td></tr>'

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f0f0f0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
<table width="100%" style="background:#f0f0f0;padding:20px 0"><tr><td align="center">
<table width="600" style="max-width:600px;background:#fff;border-radius:10px;overflow:hidden">
<tr><td style="background:#1a1a1a;padding:20px 24px"><p style="margin:0;font-size:18px;font-weight:700;color:#fff">📊 Daily Market Brief</p><p style="margin:4px 0 0;font-size:11px;color:#aaa">{p["date"]} · Generated {p["generated_at"]}</p></td></tr>

<tr><td style="background:#C0392B;padding:10px 24px"><p style="margin:0;font-size:12px;font-weight:700;color:#fff;letter-spacing:.05em">🌍 MUST KNOW TODAY</p></td></tr>
<tr><td style="padding:12px 24px;background:#FEF9F9;font-style:italic;font-size:12px;color:#555">{p["must_know"]["summary"]}</td></tr>
<tr><td style="padding:0 24px 16px"><table width="100%">{mk_rows}</table></td></tr>

<tr><td style="background:#1A5276;padding:10px 24px"><p style="margin:0;font-size:12px;font-weight:700;color:#fff;letter-spacing:.05em">📈 MARKET DASHBOARD</p></td></tr>
<tr><td style="padding:12px 24px">
  <p style="margin:0 0 6px;font-size:10px;font-weight:700;color:#2980B9;letter-spacing:.05em">EQUITY</p>
  <table width="100%">{mkt_equity}</table>
  <p style="margin:12px 0 6px;font-size:10px;font-weight:700;color:#D35400;letter-spacing:.05em">COMMODITIES</p>
  <table width="100%">{mkt_comm}</table>
  <p style="margin:12px 0 6px;font-size:10px;font-weight:700;color:#1E8449;letter-spacing:.05em">MACRO & SENTIMENT</p>
  <table width="100%">{mkt_macro}</table>
</td></tr>

<tr><td style="background:#2C3E50;padding:10px 24px"><p style="margin:0;font-size:12px;font-weight:700;color:#fff;letter-spacing:.05em">📰 TRACKED TOPICS</p></td></tr>
<tr><td style="padding:0 24px 16px"><table width="100%">{topic_rows}</table></td></tr>

<tr><td style="padding:14px 24px;text-align:center;background:#f9f9f9"><p style="margin:0;font-size:10px;color:#aaa">Sources: Anthropic Web Search · Yahoo Finance · CNN · Alternative.me · FRED · Not financial advice</p></td></tr>
</table></td></tr></table>
</body></html>"""


def send_email(html, subject):
    if not RESEND_API_KEY or not TO_EMAIL:
        print("⏭ Skipping email — RESEND_API_KEY or TO_EMAIL not set")
        return
    print(f"📧 Sending email via Resend to {TO_EMAIL}...")
    resp = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
        json={"from": "Daily Brief <onboarding@resend.dev>", "to": [TO_EMAIL], "subject": subject, "html": html},
        timeout=30,
    )
    if resp.status_code in (200, 201):
        print("✅ Email sent!")
    else:
        print(f"⚠️ Email failed {resp.status_code}: {resp.text}")


# ── 7. Main ───────────────────────────────────────────────────────────────────
def main():
    now       = datetime.now(TAIPEI_TZ)
    today_str = now.strftime("%A, %B %-d, %Y")
    now_str   = now.strftime("%I:%M %p TPE")
    print(f"🚀 Running Daily Brief for {today_str}")

    # Read topic config from sheet
    topic_configs = fetch_config()

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=120.0)

    print("📈 Fetching market data...")
    try:    market_data = fetch_market_data()
    except Exception as e: print(f"⚠️ Market data failed: {e}"); market_data = {}

    try:    fg = fetch_fear_greed()
    except: fg = {"score":None,"label":"Unavailable"}

    try:    cfg = fetch_crypto_fear_greed()
    except: cfg = {"score":None,"label":"Unavailable"}

    try:    cpi = fetch_us_cpi()
    except: cpi = {"value":"N/A","yoy":"N/A","date":"N/A"}

    print("🌍 Fetching must-know news...")
    try:    must_know = fetch_must_know(client, today_str)
    except Exception as e: print(f"⚠️ Must-know failed: {e}"); must_know = {"summary":"Could not fetch news today.","items":[]}

    print("📰 Fetching topic news...")
    try:    topic_news = fetch_topic_news(client, today_str, topic_configs)
    except Exception as e: print(f"⚠️ Topic news failed: {e}"); topic_news = {}

    payload = build_payload(today_str, now_str, must_know, topic_news, topic_configs, market_data, fg, cfg, cpi)

    try:    post_to_sheets(payload)
    except Exception as e: print(f"❌ Sheets failed: {e}")

    try:
        html = build_email_html(payload)
        send_email(html, f"📊 Daily Brief · {now.strftime('%b %-d, %Y')}")
    except Exception as e: print(f"⚠️ Email failed: {e}")

    print("✅ Done!")


if __name__ == "__main__":
    main()
