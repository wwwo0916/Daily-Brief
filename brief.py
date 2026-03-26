"""
Daily Market Brief — Email Generator
Runs every morning, fetches live data via Anthropic API, sends HTML email.
"""

import anthropic
import requests
import smtplib
import json
import os
import time
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ── Config (loaded from environment variables) ────────────────────────────────
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
GMAIL_ADDRESS     = os.environ["GMAIL_ADDRESS"]      # your Gmail address
GMAIL_APP_PASSWORD= os.environ["GMAIL_APP_PASSWORD"] # Gmail App Password (not your login password)
TO_EMAIL          = os.environ["TO_EMAIL"]           # who receives the brief (can be same as above)

# ── Tracked topics ────────────────────────────────────────────────────────────
TOPICS = [
    "Nvidia", "AMD", "Google", "Tesla Motors", "Tesla Energy", "Tesla Robotics",
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
    print("⏳ Waiting 65s before must-know fetch...")
    time.sleep(65)
    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1200,
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
        ["Nvidia", "AMD", "Google", "TSMC"],
        ["Tesla Motors", "Tesla Energy", "Tesla Robotics", "Bitcoin"],
        ["Ethereum", "Cardano", "RWA (Real World Assets)", "DeFi & Stablecoin", "Crypto Regulation"],
    ]
    all_results = {}
    for i, batch in enumerate(BATCHES):
        if i > 0:
            print(f"⏳ Waiting 65s before batch {i+1} to respect rate limits...")
            time.sleep(65)
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
                max_tokens=2000,
                tools=[{"type": "web_search_20250305", "name": "web_search"}],
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


# ── 3. Build HTML email ───────────────────────────────────────────────────────
CAT_COLORS = {
    "Macro":       ("#E6F1FB", "#0C447C"),
    "Geopolitics": ("#FCEBEB", "#791F1F"),
    "Trade":       ("#FAEEDA", "#633806"),
    "Energy":      ("#EAF3DE", "#27500A"),
    "Finance":     ("#EEEDFE", "#3C3489"),
    "Policy":      ("#F1EFE8", "#444441"),
}

SENTIMENT_COLORS = {
    "bullish": ("#EAF3DE", "#3B6D11", "#639922"),
    "bearish": ("#FCEBEB", "#A32D2D", "#E24B4A"),
    "neutral": ("#F1EFE8", "#5F5E5A", "#888780"),
}


def fear_greed_color(score):
    if score is None: return "#888780"
    if score <= 25:  return "#E24B4A"
    if score <= 45:  return "#E07820"
    if score <= 55:  return "#888780"
    if score <= 75:  return "#639922"
    return "#1D9E75"


def fear_greed_label(score):
    if score is None: return "N/A"
    if score <= 25:  return "Extreme Fear"
    if score <= 45:  return "Fear"
    if score <= 55:  return "Neutral"
    if score <= 75:  return "Greed"
    return "Extreme Greed"


def market_change_style(change_pct):
    if change_pct > 0:  return "color:#3B6D11;background:#EAF3DE"
    if change_pct < 0:  return "color:#A32D2D;background:#FCEBEB"
    return "color:#5F5E5A;background:#F1EFE8"


def build_html(today_str, must_know, topic_news, market_data, fg, cfg, cpi):
    now_taipei = datetime.now(TAIPEI_TZ).strftime("%I:%M %p TPE")

    # ── Helpers ──
    def section_header(title, color):
        return f"""
        <tr><td style="padding:28px 0 10px">
          <table width="100%" cellpadding="0" cellspacing="0"><tr>
            <td style="width:3px;background:{color};border-radius:2px">&nbsp;</td>
            <td style="padding-left:10px;font-size:13px;font-weight:600;color:#1a1a1a;letter-spacing:.04em;text-transform:uppercase">{title}</td>
          </tr></table>
        </td></tr>"""

    def cat_pill(cat):
        bg, col = CAT_COLORS.get(cat, ("#F1EFE8", "#444441"))
        return f'<span style="display:inline-block;padding:2px 9px;border-radius:20px;font-size:11px;font-weight:600;background:{bg};color:{col}">{cat}</span>'

    def sentiment_dot(s):
        _, _, dot = SENTIMENT_COLORS.get(s, ("#F1EFE8","#5F5E5A","#888"))
        return f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{dot};margin-right:6px;vertical-align:middle"></span>'

    def sentiment_pill(s):
        bg, col, dot = SENTIMENT_COLORS.get(s, ("#F1EFE8","#5F5E5A","#888"))
        label = s.capitalize()
        return f'<span style="display:inline-flex;align-items:center;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:600;background:{bg};color:{col}">{sentiment_dot(s)}{label}</span>'

    def metric_card(name, d, extra_html=""):
        ch = d.get("change_pct", 0)
        style = market_change_style(ch)
        return f"""
        <td style="padding:5px">
          <div style="background:#fff;border:1px solid #e8e8e8;border-radius:10px;padding:13px 14px;min-width:120px">
            <p style="margin:0 0 6px;font-size:11px;color:#666;line-height:1.3">{name}</p>
            <p style="margin:0 0 7px;font-size:19px;font-weight:600;color:#1a1a1a;line-height:1">{d['price']}</p>
            <span style="display:inline-block;padding:2px 7px;border-radius:20px;font-size:11px;font-weight:600;{style}">
              {d['arrow']} {d['change_label']}
            </span>
            {extra_html}
          </div>
        </td>"""

    def fg_card(name, data):
        score = data.get("score")
        label = data.get("label", "N/A")
        color = fear_greed_color(score)
        pct = score if score else 0
        score_str = str(score) if score is not None else "—"
        bar_html = f"""
        <div style="margin-top:8px">
          <div style="height:4px;background:#eee;border-radius:2px;overflow:hidden">
            <div style="height:100%;width:{pct}%;background:{color};border-radius:2px"></div>
          </div>
          <p style="margin:4px 0 0;font-size:10px;color:{color};font-weight:600">{label} · {score_str}/100</p>
        </div>"""
        d = {"price": score_str, "change_pct": 0, "change_label": "", "arrow": ""}
        return metric_card(name, d, bar_html)

    def cpi_card():
        yoy_val = cpi.get("yoy", "N/A")
        date_val = cpi.get("date", "")
        try:
            yoy_float = float(yoy_val.replace("%",""))
            ch_style = market_change_style(yoy_float)
            arrow = "▲" if yoy_float > 0 else "▼"
        except:
            ch_style = "color:#5F5E5A;background:#F1EFE8"
            arrow = "—"
        d = {"price": yoy_val, "change_pct": 0, "change_label": f"YoY · {date_val}", "arrow": arrow}
        return metric_card("US CPI (YoY)", d)

    # ── Must Know section ──
    must_know_rows = ""
    for item in must_know.get("items", []):
        try:
            url_str = f'<a href="{item["url"]}" style="font-size:11px;color:#999;text-decoration:none">source ↗</a>' if item.get("url") else ""
            must_know_rows += f"""
        <tr><td style="padding:12px 16px;border-bottom:1px solid #f0f0f0">
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr><td>{cat_pill(item.get('category',''))}</td><td align="right">{url_str}</td></tr>
            <tr><td colspan="2" style="padding-top:6px;font-size:14px;font-weight:600;color:#1a1a1a;line-height:1.4">{item.get('headline','')}</td></tr>
            <tr><td colspan="2" style="padding-top:4px;font-size:13px;color:#555;line-height:1.55">{item.get('detail','')}</td></tr>
          </table>
        </td></tr>"""
        except Exception as e:
            print(f"⚠️ Skipping must-know item: {e}")

    # ── Tracked Topics section ──
    topic_rows = ""
    for topic in TOPICS:
        try:
            items = topic_news.get(topic, [])
            if not items:
                continue
            b = sum(1 for i in items if i.get("sentiment") == "bullish")
            r = sum(1 for i in items if i.get("sentiment") == "bearish")
            overall = "bullish" if b > r else ("bearish" if r > b else "neutral")
            bullets = ""
            for item in items:
                try:
                    s = item.get("sentiment", "neutral")
                    _, col, dot = SENTIMENT_COLORS.get(s, ("#F1EFE8","#5F5E5A","#888"))
                    url_str = f' <a href="{item["url"]}" style="font-size:11px;color:#aaa;text-decoration:none">↗</a>' if item.get("url") else ""
                    bullets += f"""
                <tr><td style="padding:8px 0;border-bottom:1px solid #f5f5f5;vertical-align:top">
                  <table cellpadding="0" cellspacing="0"><tr>
                    <td style="padding-top:5px;width:14px"><span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:{dot}"></span></td>
                    <td style="font-size:13px;color:#333;line-height:1.55;padding-left:4px">{item.get('text','')}{url_str}</td>
                  </tr></table>
                </td></tr>"""
                except Exception as e:
                    print(f"⚠️ Skipping bullet for {topic}: {e}")
            topic_rows += f"""
        <tr><td style="padding:14px 0 6px">
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr>
              <td style="font-size:15px;font-weight:600;color:#1a1a1a">{topic}</td>
              <td align="right">{sentiment_pill(overall)}</td>
            </tr>
          </table>
          <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:4px">{bullets}</table>
        </td></tr>
        <tr><td style="height:1px;background:#eeeeee"></td></tr>"""
        except Exception as e:
            print(f"⚠️ Skipping topic {topic}: {e}")

    # ── Market rows ──
    equity_cards = "".join(metric_card(n, market_data[n]) for n in ["S&P 500","Nasdaq 100","Nikkei 225","Hang Seng","DAX"] if n in market_data)
    commodity_cards = "".join(metric_card(n, market_data[n]) for n in ["WTI Crude Oil","Natural Gas","Gold","Silver"] if n in market_data)
    macro_cards = (
        (metric_card("VIX", market_data["VIX"]) if "VIX" in market_data else "") +
        (metric_card("DXY Dollar Index", market_data["DXY Dollar Index"]) if "DXY Dollar Index" in market_data else "") +
        fg_card("CNN Fear & Greed", fg) +
        fg_card("Crypto Fear & Greed", cfg) +
        cpi_card()
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Daily Market Brief · {today_str}</title></head>
<body style="margin:0;padding:0;background:#f4f4f0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f0;padding:24px 0">
<tr><td align="center">
<table width="640" cellpadding="0" cellspacing="0" style="max-width:640px;width:100%">

  <!-- Header -->
  <tr><td style="background:#1a1a1a;border-radius:12px 12px 0 0;padding:22px 28px">
    <table width="100%" cellpadding="0" cellspacing="0"><tr>
      <td><p style="margin:0;font-size:20px;font-weight:700;color:#fff">Daily Market Brief</p>
          <p style="margin:4px 0 0;font-size:12px;color:#aaa">{today_str} · Generated {now_taipei}</p></td>
      <td align="right"><p style="margin:0;font-size:11px;color:#666">13 topics tracked</p></td>
    </tr></table>
  </td></tr>

  <!-- Body -->
  <tr><td style="background:#ffffff;padding:0 28px 28px;border-radius:0 0 12px 12px">
    <table width="100%" cellpadding="0" cellspacing="0">

      <!-- MUST KNOW -->
      {section_header("Must Know Today", "#E24B4A")}
      <tr><td style="padding:4px 0 8px;font-size:13px;color:#555;font-style:italic;line-height:1.65">
        {must_know.get('summary','')}
      </td></tr>
      <tr><td>
        <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #eee;border-radius:10px;overflow:hidden">
          {must_know_rows}
        </table>
      </td></tr>

      <!-- MARKET DASHBOARD -->
      {section_header("Market Dashboard", "#378ADD")}

      <tr><td style="padding:0 0 6px;font-size:12px;color:#999;font-weight:600;letter-spacing:.03em">EQUITY INDICES</td></tr>
      <tr><td><table cellpadding="0" cellspacing="0"><tr>{equity_cards}</tr></table></td></tr>

      <tr><td style="padding:14px 0 6px;font-size:12px;color:#999;font-weight:600;letter-spacing:.03em">COMMODITIES</td></tr>
      <tr><td><table cellpadding="0" cellspacing="0"><tr>{commodity_cards}</tr></table></td></tr>

      <tr><td style="padding:14px 0 6px;font-size:12px;color:#999;font-weight:600;letter-spacing:.03em">MACRO & SENTIMENT</td></tr>
      <tr><td><table cellpadding="0" cellspacing="0"><tr>{macro_cards}</tr></table></td></tr>

      <!-- TRACKED TOPICS -->
      {section_header("Tracked Topics", "#639922")}
      {topic_rows}

    </table>
  </td></tr>

  <!-- Footer -->
  <tr><td style="padding:16px 0;text-align:center">
    <p style="margin:0;font-size:11px;color:#aaa">
      Sources: Anthropic Web Search · Yahoo Finance · CNN · Alternative.me · FRED<br>
      Generated automatically for personal use · Not financial advice
    </p>
  </td></tr>

</table>
</td></tr>
</table>
</body></html>"""
    return html


# ── 4. Send email ─────────────────────────────────────────────────────────────
def send_email(html, subject, to_addr, from_addr, app_password):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = to_addr
    msg.attach(MIMEText(html, "html"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(from_addr, app_password)
        server.sendmail(from_addr, to_addr, msg.as_string())
    print(f"✅ Email sent to {to_addr}")


# ── 5. Main ───────────────────────────────────────────────────────────────────
def main():
    now = datetime.now(TAIPEI_TZ)
    today_str = now.strftime("%A, %B %-d, %Y")
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

    print("📰 Fetching topic news (batch 1/3)...")
    try:
        topic_news = fetch_topic_news(client, today_str)
    except Exception as e:
        print(f"⚠️ Topic news failed: {e}")
        topic_news = {}

    print("✉️  Building and sending email...")
    try:
        html = build_html(today_str, must_know, topic_news, market_data, fg, cfg, cpi)
        subject = f"📊 Daily Brief · {now.strftime('%b %-d, %Y')}"
        send_email(html, subject, TO_EMAIL, GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
    except Exception as e:
        print(f"❌ Email failed: {type(e).__name__}: {e}")
        raise

    print("✅ Done!")


if __name__ == "__main__":
    main()
