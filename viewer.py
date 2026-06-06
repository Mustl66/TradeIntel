"""
viewer.py — TradeIntel Public Sentiment Viewer
===============================================
Read-only leaderboard on http://localhost:8000
Shows ranked sentiment scores with full article details.
Run: python viewer.py
"""

import json
import threading
import time
import webbrowser
from datetime import datetime

import psycopg2
import psycopg2.extras
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from config import DB_CONFIG

app = FastAPI(title="TradeIntel Viewer", docs_url=None, redoc_url=None)


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def score_color(s):
    s = float(s or 0)
    if s >  0.30: return "#4ade80"
    if s >  0.10: return "#86efac"
    if s >  0.05: return "#fbbf24"
    if s < -0.30: return "#f87171"
    if s < -0.10: return "#fca5a5"
    if s < -0.05: return "#fb923c"
    return "#94a3b8"


def score_label(s):
    s = float(s or 0)
    if s >  0.30: return ("STRONG BUY",  "#4ade80")
    if s >  0.10: return ("BUY",          "#86efac")
    if s >  0.05: return ("WEAK BUY",    "#fbbf24")
    if s < -0.30: return ("STRONG SELL", "#f87171")
    if s < -0.10: return ("SELL",         "#fca5a5")
    if s < -0.05: return ("WEAK SELL",   "#fb923c")
    return ("NEUTRAL", "#94a3b8")


def fmt_score(s):
    return f"{float(s):+.4f}" if s is not None else "—"


BASE_STYLE = """
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0d1117;
    color: #e2e8f0;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-size: 14px;
    min-height: 100vh;
  }
  a { color: #60a5fa; text-decoration: none; }
  a:hover { text-decoration: underline; }

  .header {
    background: #161b27;
    border-bottom: 1px solid #1e2535;
    padding: 14px 20px;
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
    position: sticky;
    top: 0;
    z-index: 100;
  }
  .header h1 { font-size: 18px; font-weight: 800; color: #f8fafc; }
  .badge {
    background: #1e293b;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 600;
  }

  .controls {
    padding: 14px 20px;
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    align-items: center;
    border-bottom: 1px solid #1a2133;
  }
  .btn-sort {
    background: #1e293b;
    border: 1px solid #334155;
    color: #94a3b8;
    padding: 6px 14px;
    border-radius: 8px;
    cursor: pointer;
    font-size: 12px;
    font-weight: 600;
    transition: all .15s;
    text-decoration: none;
    display: inline-block;
  }
  .btn-sort:hover { background: #2d3f5a; color: #e2e8f0; }
  .btn-sort.active { background: #1d4ed8; border-color: #3b82f6; color: #fff; }
  .search-box {
    background: #1e293b;
    border: 1px solid #334155;
    color: #e2e8f0;
    padding: 6px 14px;
    border-radius: 8px;
    font-size: 13px;
    width: 220px;
    outline: none;
  }
  .search-box::placeholder { color: #475569; }
  .search-box:focus { border-color: #3b82f6; }

  /* Grid layout: cards on mobile, table on desktop */
  .leaderboard { padding: 16px 20px; }

  /* Desktop table */
  .lb-table {
    width: 100%;
    border-collapse: collapse;
    display: table;
  }
  .lb-table th {
    background: #1a2133;
    color: #475569;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .5px;
    padding: 10px 14px;
    text-align: left;
    position: sticky;
    top: 57px;
  }
  .lb-table td {
    padding: 10px 14px;
    border-bottom: 1px solid #1a2133;
    vertical-align: middle;
  }
  .lb-table tr:hover td { background: #121826; cursor: pointer; }

  /* Mobile cards */
  .lb-cards { display: none; }
  .card {
    background: #161b27;
    border: 1px solid #1e2535;
    border-radius: 12px;
    padding: 14px 16px;
    margin-bottom: 10px;
    cursor: pointer;
    transition: border-color .15s;
  }
  .card:hover { border-color: #3b82f6; }
  .card-top { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px; }
  .card-symbol { font-size: 17px; font-weight: 800; color: #60a5fa; }
  .card-name { font-size: 11px; color: #64748b; margin-top: 2px; }
  .card-score { font-size: 22px; font-weight: 900; text-align: right; }
  .card-label { font-size: 10px; font-weight: 700; text-align: right; margin-top: 2px; }
  .card-meta { display: flex; gap: 8px; flex-wrap: wrap; font-size: 11px; color: #475569; }

  @media (max-width: 700px) {
    .lb-table { display: none; }
    .lb-cards { display: block; }
    .controls { padding: 10px 14px; }
    .leaderboard { padding: 12px 14px; }
    .header { padding: 12px 14px; }
    .search-box { width: 100%; }
  }

  /* Detail modal */
  .modal-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,.75);
    z-index: 200;
    padding: 20px;
    overflow-y: auto;
  }
  .modal-overlay.open { display: flex; align-items: flex-start; justify-content: center; }
  .modal {
    background: #161b27;
    border: 1px solid #1e2535;
    border-radius: 16px;
    max-width: 820px;
    width: 100%;
    padding: 24px;
    position: relative;
    margin: auto;
  }
  .modal-close {
    position: absolute;
    top: 16px; right: 16px;
    background: #1e293b;
    border: 1px solid #334155;
    color: #94a3b8;
    width: 32px; height: 32px;
    border-radius: 8px;
    cursor: pointer;
    font-size: 16px;
    display: flex; align-items: center; justify-content: center;
  }
  .modal-close:hover { color: #fff; }
  .detail-section { margin-bottom: 16px; }
  .detail-label {
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .5px;
    margin-bottom: 6px;
  }
  .detail-box {
    background: #0f172a;
    border: 1px solid #1e2535;
    border-radius: 8px;
    padding: 12px;
    font-size: 13px;
    color: #cbd5e1;
    line-height: 1.6;
    white-space: pre-wrap;
    word-break: break-word;
    max-height: 250px;
    overflow-y: auto;
  }
  .stat-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 10px;
    margin-bottom: 16px;
  }
  .stat-card {
    background: #0f172a;
    border: 1px solid #1e2535;
    border-radius: 8px;
    padding: 12px;
    text-align: center;
  }
  .stat-label { font-size: 10px; color: #475569; font-weight: 700; text-transform: uppercase; letter-spacing: .4px; margin-bottom: 4px; }
  .stat-val { font-size: 22px; font-weight: 900; }

  @media (max-width: 500px) {
    .stat-grid { grid-template-columns: repeat(2, 1fr); }
    .modal { padding: 16px; }
  }

  /* Article list in detail */
  .art-item {
    background: #0f172a;
    border: 1px solid #1e2535;
    border-radius: 8px;
    padding: 10px 14px;
    margin-bottom: 8px;
    cursor: pointer;
    transition: border-color .15s;
  }
  .art-item:hover { border-color: #3b82f6; }
  .art-title { font-size: 13px; color: #e2e8f0; margin-bottom: 4px; line-height: 1.4; }
  .art-meta { font-size: 11px; color: #475569; display: flex; gap: 10px; flex-wrap: wrap; }
  .art-score { font-weight: 800; }

  .back-btn {
    background: #1e293b;
    border: 1px solid #334155;
    color: #94a3b8;
    padding: 6px 14px;
    border-radius: 8px;
    cursor: pointer;
    font-size: 12px;
    font-weight: 600;
    margin-bottom: 16px;
    display: inline-block;
  }
  .back-btn:hover { color: #fff; }

  .empty { text-align: center; padding: 60px 20px; color: #475569; font-size: 14px; }
  .spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid #334155; border-top-color: #3b82f6; border-radius: 50%; animation: spin .6s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
"""


# ── Main leaderboard page ─────────────────────────────────────────────────────

def _fetch_leaderboard(sort="final_score", dir="desc"):
    """Shared DB fetch for leaderboard — used by index and partial."""
    allowed = {"final_score", "symbol", "score_updated_at", "industry", "company_name"}
    sort = sort if sort in allowed else "final_score"
    order = "DESC" if dir != "asc" else "ASC"
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT s.id, s.symbol, s.company_name, s.industry, s.exchange,
                       s.final_score, s.score_updated_at,
                       COUNT(na.id) FILTER (WHERE na.sentiment_score IS NOT NULL) AS scored_count,
                       COUNT(na.id) AS total_count
                FROM symbols s
                LEFT JOIN news_articles na ON na.symbol_id = s.id
                WHERE s.final_score IS NOT NULL AND s.status = TRUE
                GROUP BY s.id
                ORDER BY {sort} {order} NULLS LAST
            """)
            rows = cur.fetchall()
            cur.execute("""
                SELECT COUNT(*) AS n,
                       AVG(final_score) AS avg_fs,
                       MAX(final_score) AS max_fs,
                       MIN(final_score) AS min_fs,
                       COUNT(CASE WHEN final_score > 0.05 THEN 1 END) AS bullish,
                       COUNT(CASE WHEN final_score < -0.05 THEN 1 END) AS bearish
                FROM symbols WHERE final_score IS NOT NULL AND status = TRUE
            """)
            stats = cur.fetchone()
    finally:
        conn.close()
    return rows, stats


@app.get("/throughput.json")
def throughput_json():
    """Rolling throughput estimate: articles/hr + symbols/hr.

    Sample = up to last 200 events per kind in 24h window.
    rate = N / hours_span. More events → tighter estimate.
    """
    SAMPLE_N = 200
    MIN_SPAN_S = 30
    out = {"tier": None, "article": {"rate": None, "n": 0},
           "symbol":  {"rate": None, "n": 0}}
    try:
        import config as _cfg
        t = getattr(_cfg, "ACTIVE_TIER", None)
        if t is not None:
            out["tier"] = f"T{t}"
    except Exception:
        pass

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for kind in ("article", "symbol"):
                try:
                    cur.execute("""
                        SELECT scored_at FROM scoring_events
                        WHERE kind = %s AND scored_at >= NOW() - INTERVAL '24 hours'
                        ORDER BY scored_at DESC LIMIT %s
                    """, (kind, SAMPLE_N))
                    rows = cur.fetchall()
                except Exception:
                    conn.rollback()
                    rows = []
                n = len(rows)
                if n >= 2:
                    span_s = max(MIN_SPAN_S, (rows[0][0] - rows[-1][0]).total_seconds())
                    out[kind] = {"rate": n / (span_s / 3600.0), "n": n}
                else:
                    out[kind] = {"rate": None, "n": n}
    finally:
        conn.close()
    return out


@app.get("/leaderboard-partial")
def leaderboard_partial(sort: str = "final_score", dir: str = "desc"):
    """Returns updated rows + stats as JSON for seamless live refresh."""
    rows, stats = _fetch_leaderboard(sort, dir)
    now = datetime.now().strftime("%H:%M:%S")

    table_rows = ""
    card_items = ""
    for i, r in enumerate(rows, 1):
        sc = r["final_score"]
        col = score_color(sc)
        lbl, lcol = score_label(sc)
        upd = r["score_updated_at"].strftime("%m-%d %H:%M") if r["score_updated_at"] else "—"
        sym_id = r["id"]
        scored = r["scored_count"] or 0
        total  = r["total_count"] or 0
        table_rows += f"""
        <tr onclick="openSymbol({sym_id})">
          <td style="color:#475569;font-size:12px">{i}</td>
          <td><span style="font-weight:800;color:#60a5fa;font-size:14px">{r['symbol']}</span></td>
          <td style="color:#94a3b8;font-size:12px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{r.get('company_name') or '—'}</td>
          <td style="color:#475569;font-size:11px">{r.get('industry') or '—'}</td>
          <td style="text-align:right">
            <span style="font-size:15px;font-weight:900;color:{col}">{fmt_score(sc)}</span>
          </td>
          <td style="text-align:center">
            <span style="font-size:10px;font-weight:700;color:{lcol};background:#1e293b;padding:2px 8px;border-radius:10px">{lbl}</span>
          </td>
          <td style="text-align:center;font-size:11px;color:#475569">{scored}/{total}</td>
          <td style="text-align:center;font-size:11px;color:#475569">{upd}</td>
        </tr>"""

        card_items += f"""
        <div class="card" onclick="openSymbol({sym_id})">
          <div class="card-top">
            <div>
              <div class="card-symbol">{r['symbol']}</div>
              <div class="card-name">{r.get('company_name') or '—'}</div>
            </div>
            <div>
              <div class="card-score" style="color:{col}">{fmt_score(sc)}</div>
              <div class="card-label" style="color:{lcol}">{lbl}</div>
            </div>
          </div>
          <div class="card-meta">
            <span>📊 {r.get('industry') or '—'}</span>
            <span>📰 {scored}/{total} articles</span>
            <span>🕐 {upd}</span>
          </div>
        </div>"""

    n = stats["n"] or 0
    avg_col = score_color(stats["avg_fs"] or 0)
    return JSONResponse({
        "table_rows": table_rows,
        "card_items": card_items,
        "n": n,
        "bullish": stats["bullish"] or 0,
        "bearish": stats["bearish"] or 0,
        "avg_fs": fmt_score(stats["avg_fs"]),
        "avg_col": avg_col,
        "max_fs": fmt_score(stats["max_fs"]),
        "min_fs": fmt_score(stats["min_fs"]),
        "now": now,
    })


@app.get("/", response_class=HTMLResponse)
def index(sort: str = "final_score", dir: str = "desc"):
    rows, stats = _fetch_leaderboard(sort, dir)

    def sort_link(col, label):
        nd = "asc" if (sort == col and dir == "desc") else "desc"
        arrow = " ▼" if (sort == col and dir == "desc") else " ▲" if (sort == col and dir == "asc") else ""
        active = " active" if sort == col else ""
        return f'<a href="/?sort={col}&dir={nd}" class="btn-sort{active}">{label}{arrow}</a>'

    # Build table rows
    table_rows = ""
    card_items = ""
    for i, r in enumerate(rows, 1):
        sc = r["final_score"]
        col = score_color(sc)
        lbl, lcol = score_label(sc)
        upd = r["score_updated_at"].strftime("%m-%d %H:%M") if r["score_updated_at"] else "—"
        sym_id = r["id"]
        scored = r["scored_count"] or 0
        total  = r["total_count"] or 0

        table_rows += f"""
        <tr onclick="openSymbol({sym_id})">
          <td style="color:#475569;font-size:12px">{i}</td>
          <td><span style="font-weight:800;color:#60a5fa;font-size:14px">{r['symbol']}</span></td>
          <td style="color:#94a3b8;font-size:12px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{r.get('company_name') or '—'}</td>
          <td style="color:#475569;font-size:11px">{r.get('industry') or '—'}</td>
          <td style="text-align:right">
            <span style="font-size:15px;font-weight:900;color:{col}">{fmt_score(sc)}</span>
          </td>
          <td style="text-align:center">
            <span style="font-size:10px;font-weight:700;color:{lcol};background:#1e293b;padding:2px 8px;border-radius:10px">{lbl}</span>
          </td>
          <td style="text-align:center;font-size:11px;color:#475569">{scored}/{total}</td>
          <td style="text-align:center;font-size:11px;color:#475569">{upd}</td>
        </tr>"""

        card_items += f"""
        <div class="card" onclick="openSymbol({sym_id})">
          <div class="card-top">
            <div>
              <div class="card-symbol">{r['symbol']}</div>
              <div class="card-name">{r.get('company_name') or '—'}</div>
            </div>
            <div>
              <div class="card-score" style="color:{col}">{fmt_score(sc)}</div>
              <div class="card-label" style="color:{lcol}">{lbl}</div>
            </div>
          </div>
          <div class="card-meta">
            <span>📊 {r.get('industry') or '—'}</span>
            <span>📰 {scored}/{total} articles</span>
            <span>🕐 {upd}</span>
          </div>
        </div>"""

    n = stats["n"] or 0
    avg_col = score_color(stats["avg_fs"] or 0)
    now = datetime.now().strftime("%H:%M:%S")

    empty_msg = '<tr><td colspan="8" class="empty">No symbols scored yet — run sentiment_scoring.py</td></tr>' if not table_rows else ""
    empty_cards = '<div class="empty">No symbols scored yet.</div>' if not card_items else ""

    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en">
<head>
  <title>TradeIntel — Sentiment Viewer</title>
  {BASE_STYLE}
</head>
<body>

<div class="header">
  <h1>📈 TradeIntel</h1>
  <span class="badge" style="color:#fcd34d">{n} symbols</span>
  <span class="badge" style="color:#4ade80">🟢 {stats['bullish']} bullish</span>
  <span class="badge" style="color:#f87171">🔴 {stats['bearish']} bearish</span>
  <span class="badge" style="color:{avg_col}">avg {fmt_score(stats['avg_fs'])}</span>
  <span class="badge" style="color:#86efac">↑ {fmt_score(stats['max_fs'])}</span>
  <span class="badge" style="color:#fca5a5">↓ {fmt_score(stats['min_fs'])}</span>
  <span id="throughput-badge" class="badge" style="color:#94a3b8;font-size:11px">⚡ loading…</span>
  <span class="badge" style="color:#475569;margin-left:auto">🕐 {now}</span>
</div>
<script>
(function() {{
  async function refreshThroughput() {{
    try {{
      const r = await fetch('/throughput.json');
      if (!r.ok) return;
      const d = await r.json();
      const el = document.getElementById('throughput-badge');
      if (!el) return;
      const fmt = (s) => {{
        if (s.rate === null) return `— /h <span style="color:#475569">(n=${{s.n}})</span>`;
        const rate = s.rate >= 10 ? Math.round(s.rate).toLocaleString() : s.rate.toFixed(1);
        const conf = Math.min(1, s.n / 200);
        const dot = conf < 0.3 ? '#ef4444' : (conf < 0.7 ? '#fbbf24' : '#4ade80');
        return `<b style="color:#e2e8f0">${{rate}}</b><span style="color:#64748b">/h</span> <span style="color:${{dot}}">●</span> <span style="color:#475569">n=${{s.n}}</span>`;
      }};
      const tier = d.tier ? `<span style="color:#a5b4fc;font-weight:700;margin-right:6px">${{d.tier}}</span>` : '';
      el.innerHTML = `${{tier}}📄 ${{fmt(d.article)}} <span style="color:#1e293b">│</span> 📊 ${{fmt(d.symbol)}}`;
    }} catch(e) {{}}
  }}
  refreshThroughput();
  setInterval(refreshThroughput, 10000);
}})();
</script>

<div class="controls">
  <input class="search-box" type="text" placeholder="🔍 Filter symbol or company…" oninput="filterRows(this.value)" id="search">
  {sort_link('final_score', '🏆 Score')}
  {sort_link('symbol', '🔤 Symbol')}
  {sort_link('company_name', '🏢 Name')}
  {sort_link('industry', '🏭 Industry')}
  {sort_link('score_updated_at', '🕐 Updated')}
  <a href="/" class="btn-sort" style="margin-left:auto">↻ Refresh</a>
</div>

<div class="leaderboard">
  <!-- Score math legend -->
  <details style="margin-bottom:16px;background:#161b27;border:1px solid #1e2535;border-radius:8px;padding:12px 16px">
    <summary style="cursor:pointer;font-size:12px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px;user-select:none">
      📐 How scores are calculated
    </summary>
    <div style="margin-top:14px;font-size:12px;color:#94a3b8;line-height:1.7">
      <div style="margin-bottom:12px">
        <span style="color:#60a5fa;font-weight:700">Step 1 — Raw article score</span><br>
        Each article is scored by the LLM on a scale from <b style="color:#f87171">−1.0</b> (very bearish) to <b style="color:#4ade80">+1.0</b> (very bullish), using the scoring rubric:
        <table style="margin-top:8px;border-collapse:collapse;width:100%">
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#4ade80">+0.70 → +1.00</td><td style="padding:4px 8px">Very positive — earnings beat &gt;10%, major acquisition, FDA approval, large contract win</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#86efac">+0.30 → +0.69</td><td style="padding:4px 8px">Positive — in-line earnings + guidance, new product, analyst upgrade</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#d1fae5">+0.05 → +0.29</td><td style="padding:4px 8px">Mildly positive — minor partnership, conference mention, routine filing</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#94a3b8">−0.04 → +0.04</td><td style="padding:4px 8px">Neutral — no material information</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#fca5a5">−0.29 → −0.05</td><td style="padding:4px 8px">Mildly negative — minor setback, noise</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#f87171">−0.69 → −0.30</td><td style="padding:4px 8px">Negative — earnings miss, guidance cut, litigation</td></tr>
          <tr><td style="padding:4px 8px;color:#ef4444">−1.00 → −0.70</td><td style="padding:4px 8px">Very negative — fraud, bankruptcy, regulatory shutdown</td></tr>
        </table>
      </div>
      <div style="margin-bottom:12px">
        <span style="color:#60a5fa;font-weight:700">Step 2 — Time decay</span><br>
        Old news matters less. Each score is decayed by age:<br>
        <code style="background:#0f1117;padding:2px 6px;border-radius:4px;color:#fcd34d">weighted = score × e^(−0.02 × hours_old)</code><br>
        <span style="color:#64748b">Example: a +0.95 article from 48 hours ago → +0.95 × e^(−0.96) ≈ +0.36</span>
      </div>
      <div style="margin-bottom:12px">
        <span style="color:#60a5fa;font-weight:700">Step 3 — Symbol final score</span><br>
        <code style="background:#0f1117;padding:2px 6px;border-radius:4px;color:#fcd34d">final_score = avg(all weighted scores) × sector_multiplier</code><br>
        <span style="color:#64748b">The sector multiplier (from macro_multiplier step) adjusts for macro tailwinds/headwinds. Default = 1.0.</span>
      </div>
      <div>
        <span style="color:#60a5fa;font-weight:700">Why high raw scores → low final score?</span><br>
        If most articles are old (weeks/months), their time-decayed weighted scores approach 0.00 even if raw scores were high. Many neutral (0.00) articles also dilute the average.
        The score reflects <em>current</em> sentiment momentum, not historical highs.
      </div>
      <div style="margin-top:14px;border-top:1px solid #1e2535;padding-top:10px">
        <span style="color:#60a5fa;font-weight:700">Signal legend</span><br>
        <span style="color:#4ade80">⬆ Strong Buy</span> &gt; +0.30 &nbsp;·&nbsp;
        <span style="color:#86efac">↑ Buy</span> +0.10 → +0.30 &nbsp;·&nbsp;
        <span style="color:#fbbf24">↗ Weak Buy</span> +0.05 → +0.10 &nbsp;·&nbsp;
        <span style="color:#94a3b8">→ Neutral</span> −0.05 → +0.05 &nbsp;·&nbsp;
        <span style="color:#fb923c">↙ Weak Sell</span> −0.10 → −0.05 &nbsp;·&nbsp;
        <span style="color:#fca5a5">↓ Sell</span> −0.30 → −0.10 &nbsp;·&nbsp;
        <span style="color:#f87171">⬇ Strong Sell</span> &lt; −0.30
      </div>
    </div>
  </details>

  <!-- Desktop table -->
  <table class="lb-table" id="lb-table">
    <thead>
      <tr>
        <th>#</th>
        <th>Symbol</th>
        <th>Company</th>
        <th>Industry</th>
        <th style="text-align:right">Score</th>
        <th style="text-align:center">Signal</th>
        <th style="text-align:center">Articles</th>
        <th style="text-align:center">Updated</th>
      </tr>
    </thead>
    <tbody id="table-body">
      {table_rows}
      {empty_msg}
    </tbody>
  </table>

  <!-- Mobile cards -->
  <div class="lb-cards" id="lb-cards">
    {card_items}
    {empty_cards}
  </div>
</div>

<!-- Symbol detail modal -->
<div class="modal-overlay" id="modal-overlay" onclick="closeModal(event)">
  <div class="modal" id="modal-content">
    <button class="modal-close" onclick="closeOverlay()">✕</button>
    <div id="modal-body"><div class="empty"><span class="spinner"></span> Loading…</div></div>
  </div>
</div>

<script>
function filterRows(q) {{
  q = q.toLowerCase();
  // table rows
  document.querySelectorAll('#table-body tr').forEach(tr => {{
    const txt = tr.textContent.toLowerCase();
    tr.style.display = txt.includes(q) ? '' : 'none';
  }});
  // cards
  document.querySelectorAll('#lb-cards .card').forEach(c => {{
    const txt = c.textContent.toLowerCase();
    c.style.display = txt.includes(q) ? '' : 'none';
  }});
}}

function openSymbol(id) {{
  window.location.href = '/symbol/' + id;
}}

// Event delegation for article expand — works even after innerHTML replacement
document.addEventListener('click', function(e) {{
  const item = e.target.closest('.art-item');
  if (!item) return;
  // don't collapse when clicking a link inside
  if (e.target.tagName === 'A') return;
  const d = item.querySelector('.art-detail');
  if (d) {{
    d.style.display = d.style.display === 'none' ? 'block' : 'none';
    item.classList.toggle('art-open', d.style.display === 'block');
  }}
}});

function closeOverlay() {{
  document.getElementById('modal-overlay').classList.remove('open');
}}

function closeModal(e) {{
  if (e.target === document.getElementById('modal-overlay')) closeOverlay();
}}

document.addEventListener('keydown', e => {{ if (e.key === 'Escape') closeOverlay(); }});

// ── Live auto-refresh every 30s ─────────────────────────────────────────────
(function() {{
  const params = new URLSearchParams(window.location.search);
  const sort = params.get('sort') || 'final_score';
  const dir  = params.get('dir')  || 'desc';

  function refresh() {{
    fetch('/leaderboard-partial?sort=' + sort + '&dir=' + dir)
      .then(r => r.json())
      .then(d => {{
        document.getElementById('table-body').innerHTML = d.table_rows;
        document.getElementById('lb-cards').innerHTML   = d.card_items;
        // update stat badges
        const badges = document.querySelectorAll('.header .badge');
        if (badges.length >= 6) {{
          badges[0].textContent = d.n + ' symbols';
          badges[1].textContent = '🟢 ' + d.bullish + ' bullish';
          badges[2].textContent = '🔴 ' + d.bearish + ' bearish';
          badges[3].textContent = 'avg ' + d.avg_fs;
          badges[3].style.color = d.avg_col;
          badges[4].textContent = '↑ ' + d.max_fs;
          badges[5].textContent = '↓ ' + d.min_fs;
          badges[6].textContent = '🕐 ' + d.now;
        }}
        // re-apply search filter if active
        const q = document.getElementById('search')?.value;
        if (q) filterRows(q);
      }})
      .catch(() => {{}});  // silent fail — no network noise
  }}

  setInterval(refresh, 30000);
}})();
</script>

</body>
</html>""")


# ── Symbol detail (full page) ──────────────────────────────────────────────────

@app.get("/symbol/{sym_id}", response_class=HTMLResponse)
def symbol_detail(sym_id: int):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Symbol + TV data
            cur.execute("""
                SELECT s.id, s.symbol, s.company_name, s.industry, s.exchange,
                       s.final_score, s.score_updated_at,
                       s.symbol_master_summary, s.symbol_forecast_narrative,
                       s.close_price, s.price_change, s.market_cap_formatted,
                       s.rsi, s.sma200, s.price_52_week_high,
                       s.average_volume_30d_calc, s.relative_volume_10d_calc,
                       s.price_earnings_ttm, s.price_book_ratio, s.price_sales_ratio,
                       s.gross_margin, s.operating_margin, s.net_margin,
                       s.return_on_equity, s.debt_to_equity, s.current_ratio,
                       s.earnings_per_share_basic_ttm, s.earnings_release_date,
                       s.dividend_yield_recent, s.number_of_employees,
                       s.total_revenue, s.net_income,
                       sm.macro_multiplier, sm.rationale AS sector_rationale,
                       sm.sector_name
                FROM symbols s
                LEFT JOIN sectors_macro sm ON sm.id = s.sector_id
                WHERE s.id = %s
            """, (sym_id,))
            sym = cur.fetchone()
            if not sym:
                return HTMLResponse('<html><body style="background:#0d1117;color:#e2e8f0;font-family:sans-serif;padding:40px">Symbol not found. <a href="/" style="color:#60a5fa">← Back</a></body></html>')

            # Latest TV snapshot
            cur.execute("""
                SELECT data FROM symbol_daily_snapshots
                WHERE symbol_id = %s
                ORDER BY snapshot_date DESC LIMIT 1
            """, (sym_id,))
            snap_row = cur.fetchone()
            tv_snap = snap_row["data"] if snap_row else {}

            # Market research articles — proper sector-based link via sectors_macro
            # Use exact sector_name/industry_name from the joined sectors_macro row
            _sector_name   = (sym.get("sector_name") or "")[:80]
            _industry_name = ""
            # Fetch all industry names for this symbol's sector from sectors_macro
            if sym.get("sector_name"):
                cur.execute("""
                    SELECT industry_name FROM sectors_macro
                    WHERE sector_name ILIKE %s
                    ORDER BY macro_multiplier DESC
                """, (f"%{sym['sector_name']}%",))
                _industry_names = [r["industry_name"] for r in cur.fetchall()]
            else:
                _industry_names = []

            _mr_keywords = [k for k in ([_sector_name] + _industry_names) if len(k) > 2]
            if _mr_keywords:
                _conditions = " OR ".join(
                    ["(mra.title ILIKE %s OR mra.summary ILIKE %s)" for _ in _mr_keywords]
                )
                _params = []
                for k in _mr_keywords:
                    _params += [f"%{k}%", f"%{k}%"]
                cur.execute(f"""
                    SELECT mra.title, mra.url, mra.published_at, mra.source_name, mra.summary
                    FROM market_research_articles mra
                    WHERE mra.llm_processed = TRUE AND ({_conditions})
                    ORDER BY mra.published_at DESC
                    LIMIT 8
                """, _params)
            else:
                cur.execute("""
                    SELECT mra.title, mra.url, mra.published_at, mra.source_name, mra.summary
                    FROM market_research_articles mra
                    WHERE mra.llm_processed = TRUE
                    ORDER BY mra.published_at DESC
                    LIMIT 5
                """)
            mr_articles = cur.fetchall()

            # Scored articles
            cur.execute("""
                SELECT id, title, url, published_at, source_name,
                       sentiment_score, weighted_sentiment,
                       article_summary, score_rationale, forecast_until_earnings,
                       key_events, pre_summary_data, master_summary_snapshot,
                       stage2_prompt, company_connections
                FROM news_articles
                WHERE symbol_id = %s AND sentiment_score IS NOT NULL
                ORDER BY published_at DESC NULLS LAST
                LIMIT 50
            """, (sym_id,))
            articles = cur.fetchall()
    finally:
        conn.close()

    sc = sym["final_score"]
    col = score_color(sc)
    lbl, lcol = score_label(sc)
    upd = sym["score_updated_at"].strftime("%Y-%m-%d %H:%M") if sym["score_updated_at"] else "—"

    def fmt(v, suffix="", decimals=2, prefix=""):
        if v is None: return "—"
        try: return f"{prefix}{float(v):.{decimals}f}{suffix}"
        except: return str(v)

    def pct(v):
        if v is None: return "—"
        try: return f"{float(v)*100:.1f}%"
        except: return str(v)

    def vol_fmt(v):
        if v is None: return "—"
        try:
            v = float(v)
            if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
            if v >= 1_000: return f"{v/1_000:.0f}K"
            return str(int(v))
        except: return str(v)

    # TradingView stats grid
    earnings_date = sym["earnings_release_date"].strftime("%Y-%m-%d") if sym.get("earnings_release_date") else "—"
    tv_stats = [
        ("Price", fmt(sym.get("close_price"), prefix="$")),
        ("Change", fmt(sym.get("price_change"), suffix="%")),
        ("Market Cap", sym.get("market_cap_formatted") or "—"),
        ("Next Earnings", earnings_date),
        ("RSI", fmt(sym.get("rsi"))),
        ("SMA 200", fmt(sym.get("sma200"), prefix="$")),
        ("52W High", fmt(sym.get("price_52_week_high"), prefix="$")),
        ("Avg Vol 30d", vol_fmt(sym.get("average_volume_30d_calc"))),
        ("Rel Vol", fmt(sym.get("relative_volume_10d_calc"))),
        ("P/E TTM", fmt(sym.get("price_earnings_ttm"))),
        ("P/B", fmt(sym.get("price_book_ratio"))),
        ("P/S", fmt(sym.get("price_sales_ratio"))),
        ("EPS TTM", fmt(sym.get("earnings_per_share_basic_ttm"), prefix="$")),
        ("Gross Margin", pct(sym.get("gross_margin"))),
        ("Op Margin", pct(sym.get("operating_margin"))),
        ("Net Margin", pct(sym.get("net_margin"))),
        ("ROE", pct(sym.get("return_on_equity"))),
        ("D/E", fmt(sym.get("debt_to_equity"))),
        ("Current Ratio", fmt(sym.get("current_ratio"))),
        ("Div Yield", pct(sym.get("dividend_yield_recent"))),
        ("Employees", f"{sym['number_of_employees']:,}" if sym.get("number_of_employees") else "—"),
        ("Sector Mult", fmt(sym.get("macro_multiplier"))),
    ]

    tv_grid = "".join(
        f'<div class="stat-card"><div class="stat-label">{lbl}</div><div class="stat-val" style="font-size:14px;color:#e2e8f0">{val}</div></div>'
        for lbl, val in tv_stats if val != "—"
    )

    # Collect all company connections across articles
    all_connections = {"competitors": set(), "partners": set(), "suppliers": set()}
    for a in articles:
        cc = a.get("company_connections")
        if cc:
            if isinstance(cc, str):
                try: cc = json.loads(cc)
                except: cc = {}
            if not isinstance(cc, dict):
                cc = {}
            for k in ("competitors", "partners", "suppliers"):
                for item in (cc.get(k) or []):
                    if item and str(item).strip():
                        all_connections[k].add(str(item).strip())

    def conn_badges(items, color):
        if not items: return '<span style="color:#475569">None identified</span>'
        return " ".join(f'<a href="/?q={i}" style="background:#1e293b;color:{color};padding:3px 10px;border-radius:12px;font-size:12px;font-weight:600;text-decoration:none;display:inline-block;margin:2px">{i}</a>' for i in sorted(items))

    connections_html = f"""
    <div style="margin-bottom:20px">
      <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#f87171;margin-bottom:6px">⚔ Competitors</div>
      <div>{conn_badges(all_connections['competitors'], '#f87171')}</div>
    </div>
    <div style="margin-bottom:20px">
      <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#4ade80;margin-bottom:6px">🤝 Partners / Customers</div>
      <div>{conn_badges(all_connections['partners'], '#4ade80')}</div>
    </div>
    <div style="margin-bottom:20px">
      <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#fbbf24;margin-bottom:6px">📦 Suppliers</div>
      <div>{conn_badges(all_connections['suppliers'], '#fbbf24')}</div>
    </div>
    """

    # Market research
    mr_html = ""
    for mr in mr_articles:
        mr_pub = mr["published_at"].strftime("%Y-%m-%d") if mr.get("published_at") else "?"
        mr_title = (mr.get("title") or "").replace("<","&lt;")
        mr_src = mr.get("source_name") or ""
        mr_url = mr.get("url") or ""
        mr_summary = (mr.get("summary") or "").replace("<","&lt;")[:300]
        mr_html += f"""
        <div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:10px 14px;margin-bottom:8px">
          <div style="font-size:13px;color:#e2e8f0;margin-bottom:4px">{mr_title}</div>
          <div style="font-size:11px;color:#475569;margin-bottom:6px">📅 {mr_pub} · {mr_src} {'<a href="'+mr_url+'" target="_blank" style="color:#60a5fa">↗ Source</a>' if mr_url else ''}</div>
          {('<div style="font-size:12px;color:#94a3b8">'+mr_summary+'</div>') if mr_summary else ''}
        </div>"""
    if not mr_html:
        mr_html = '<div style="color:#475569;font-size:13px">No market research articles available.</div>'

    # Articles HTML
    arts_html = ""
    for i, a in enumerate(articles):
        ascore = a["sentiment_score"]
        acol = score_color(ascore)
        albl, alcol = score_label(ascore)
        pub = a["published_at"].strftime("%Y-%m-%d %H:%M") if a["published_at"] else "?"
        title = (a["title"] or "Untitled").replace("<","&lt;").replace(">","&gt;")
        src_link = f'<a href="{a["url"]}" target="_blank" style="color:#60a5fa">↗ Source</a>' if a.get("url") else ""

        # Key events
        ke = a.get("key_events") or {}
        if isinstance(ke, str):
            try: ke = json.loads(ke)
            except: ke = {}
        ke_items = [(k.replace("_"," ").title(), v) for k, v in ke.items() if v and v != "null" and v is not None]
        ke_html = ""
        if ke_items:
            ke_html = "<div style='margin-top:8px'><div style='font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;margin-bottom:4px'>KEY EVENTS</div>"
            for k, v in ke_items:
                ke_html += f'<div style="font-size:12px;color:#cbd5e1;margin-bottom:2px"><span style="color:#64748b">{k}:</span> {str(v).replace("<","&lt;")[:200]}</div>'
            ke_html += "</div>"

        # Article-level company connections
        a_cc = a.get("company_connections") or {}
        if isinstance(a_cc, str):
            try: a_cc = json.loads(a_cc)
            except: a_cc = {}
        if not isinstance(a_cc, dict):
            a_cc = {}
        a_cc_parts = []
        for k, color in [("competitors","#f87171"),("partners","#4ade80"),("suppliers","#fbbf24")]:
            items = [x for x in (a_cc.get(k) or []) if x]
            if items:
                a_cc_parts.append(f'<span style="color:#475569">{k.title()}:</span> ' + ", ".join(f'<span style="color:{color}">{x}</span>' for x in items))
        a_cc_html = ""
        if a_cc_parts:
            a_cc_html = '<div style="margin-top:8px;font-size:12px">' + " &nbsp;|&nbsp; ".join(a_cc_parts) + "</div>"

        # Collapsible LLM input section
        pre_data = a.get("pre_summary_data") or {}
        if isinstance(pre_data, str):
            try: pre_data = json.loads(pre_data)
            except: pre_data = {}
        master_snap = (a.get("master_summary_snapshot") or "").replace("<","&lt;")
        stage2_p = (a.get("stage2_prompt") or "").replace("<","&lt;")
        pre_data_str = json.dumps(pre_data, indent=2).replace("<","&lt;") if pre_data else ""

        llm_input_html = f"""
        <details style="margin-top:10px">
          <summary style="cursor:pointer;font-size:10px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:.5px;user-select:none">🔬 What LLM received (click to expand)</summary>
          <div style="margin-top:8px">
            {'<div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;margin-bottom:4px">STAGE 1 PRE-SUMMARY (extracted facts)</div><div style="background:#060c18;border:1px solid #1e2535;border-radius:6px;padding:8px;font-size:11px;color:#94a3b8;white-space:pre-wrap;max-height:200px;overflow-y:auto;font-family:monospace">' + pre_data_str + '</div>' if pre_data_str else ''}
            {'<div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;margin-top:8px;margin-bottom:4px">MASTER SUMMARY (context given to LLM)</div><div style="background:#060c18;border:1px solid #1e2535;border-radius:6px;padding:8px;font-size:11px;color:#94a3b8;white-space:pre-wrap;max-height:150px;overflow-y:auto">' + master_snap + '</div>' if master_snap else ''}
            {'<div style="font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;margin-top:8px;margin-bottom:4px">FULL STAGE 2 PROMPT (JSON sent to LLM)</div><div style="background:#060c18;border:1px solid #1e2535;border-radius:6px;padding:8px;font-size:11px;color:#94a3b8;white-space:pre-wrap;max-height:200px;overflow-y:auto;font-family:monospace">' + stage2_p + '</div>' if stage2_p else ''}
          </div>
        </details>"""

        arts_html += f"""
        <div style="background:#0f172a;border:1px solid #1e2535;border-radius:10px;padding:14px;margin-bottom:10px">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:8px">
            <div style="font-size:13px;color:#e2e8f0;font-weight:600;line-height:1.4;flex:1">{title}</div>
            <div style="text-align:right;flex-shrink:0">
              <div style="font-size:18px;font-weight:900;color:{acol}">{fmt_score(ascore)}</div>
              <div style="font-size:10px;font-weight:700;color:{alcol}">{albl}</div>
            </div>
          </div>
          <div style="font-size:11px;color:#475569;display:flex;gap:12px;flex-wrap:wrap;margin-bottom:8px">
            <span>📅 {pub}</span>
            <span>🔗 {a.get('source_name') or '?'}</span>
            <span>⚖ weighted: {fmt_score(a.get('weighted_sentiment'))}</span>
            {src_link}
          </div>
          {('<div style="font-size:13px;color:#94a3b8;margin-bottom:6px">'+a['article_summary'].replace('<','&lt;')+'</div>') if a.get('article_summary') else ''}
          {('<details style="margin-bottom:6px"><summary style="cursor:pointer;font-size:10px;font-weight:700;color:#f59e0b;text-transform:uppercase;letter-spacing:.5px">Rationale</summary><div style="margin-top:6px;font-size:12px;color:#cbd5e1;line-height:1.6">' + a['score_rationale'].replace('<','&lt;') + '</div></details>') if a.get('score_rationale') else ''}
          {('<details style="margin-bottom:6px"><summary style="cursor:pointer;font-size:10px;font-weight:700;color:#a78bfa;text-transform:uppercase;letter-spacing:.5px">Forecast</summary><div style="margin-top:6px;font-size:12px;color:#cbd5e1;line-height:1.6">' + a['forecast_until_earnings'].replace('<','&lt;') + '</div></details>') if a.get('forecast_until_earnings') else ''}
          {ke_html}
          {a_cc_html}
          {llm_input_html}
        </div>"""

    master = (sym.get("symbol_master_summary") or "").replace("<","&lt;")
    forecast = (sym.get("symbol_forecast_narrative") or "").replace("<","&lt;")
    sector_rat = (sym.get("sector_rationale") or "").replace("<","&lt;")

    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en">
<head>
  <title>{sym['symbol']} — TradeIntel</title>
  {BASE_STYLE}
  <style>
    .detail-page {{ max-width: 1100px; margin: 0 auto; padding: 24px 20px; }}
    .section-title {{ font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#475569;margin-bottom:10px;margin-top:24px;padding-bottom:6px;border-bottom:1px solid #1e2535; }}
    .tv-grid {{ display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:8px;margin-bottom:4px; }}
    .tv-card {{ background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:10px 12px; }}
    .tv-label {{ font-size:10px;color:#475569;font-weight:700;text-transform:uppercase;letter-spacing:.4px;margin-bottom:3px; }}
    .tv-val {{ font-size:14px;font-weight:700;color:#e2e8f0; }}
    .two-col {{ display:grid;grid-template-columns:1fr 1fr;gap:20px; }}
    @media(max-width:700px) {{ .two-col {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
<div class="header">
  <a href="/" class="back-btn">← Leaderboard</a>
  <h1 style="font-size:16px">📈 {sym['symbol']}</h1>
  <span class="badge" style="color:#94a3b8">{sym.get('company_name') or '—'}</span>
  <span class="badge" style="color:#60a5fa">{sym.get('industry') or '—'}</span>
  <span class="badge" style="color:#475569">{sym.get('exchange') or '—'}</span>
  {f'<span class="badge" style="color:#64748b">{sym["sector_name"]}</span>' if sym.get('sector_name') else ''}
</div>

<div class="detail-page">

  <!-- Score hero -->
  <div style="display:flex;align-items:center;gap:20px;margin-bottom:24px;padding:20px;background:#161b27;border:1px solid #1e2535;border-radius:12px">
    <div>
      <div style="font-size:48px;font-weight:900;color:{col}">{fmt_score(sc)}</div>
      <div style="font-size:14px;font-weight:700;color:{lcol}">{lbl}</div>
    </div>
    <div style="flex:1">
      <div style="font-size:12px;color:#475569">Updated: {upd}</div>
      <div style="font-size:12px;color:#475569">Articles scored: {len(articles)}</div>
      {''.join([
        f'<div style="font-size:12px;color:#94a3b8;margin-top:8px;line-height:1.8">',
        f'<span style="color:#64748b">Base avg (time-decayed): </span><span style="color:#fcd34d;font-weight:700">{fmt(round(sc / sym["macro_multiplier"], 6) if sym.get("macro_multiplier") and sym["macro_multiplier"] != 0 else sc)}</span><br>',
        f'<span style="color:#64748b">Sector multiplier (</span><span style="color:#60a5fa">{sym.get("sector_name") or "—"}</span><span style="color:#64748b">): </span><span style="color:#34d399;font-weight:700">×{fmt(sym.get("macro_multiplier"))}</span>',
        f'<span style="color:#64748b;margin-left:8px;font-size:11px">(+{fmt(round((sym["macro_multiplier"]-1)*100 if sym.get("macro_multiplier") else 0, 2))}% boost)</span><br>' if sym.get("macro_multiplier") and sym["macro_multiplier"] > 1 else '<br>',
        f'<span style="color:#64748b">Final score: </span><span style="color:{col};font-weight:700">{fmt_score(sc)}</span>',
        f'</div>'
      ]) if sym.get("macro_multiplier") else f'<div style="font-size:12px;color:#475569;margin-top:4px">No sector multiplier</div>'}
    </div>
  </div>

  <!-- TradingView data -->
  <div class="section-title">📊 TradingView Screener Data</div>
  <div class="tv-grid">{tv_grid}</div>

  <!-- Forecast -->
  {f'<div class="section-title">🔮 Symbol Forecast</div><div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:14px;font-size:13px;color:#cbd5e1;line-height:1.7;white-space:pre-wrap">{forecast}</div>' if forecast else ''}

  <!-- Master summary -->
  {f'<div class="section-title">📋 Master Summary</div><div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:14px;font-size:13px;color:#cbd5e1;line-height:1.7;white-space:pre-wrap">{master}</div>' if master else ''}

  <!-- Company connections -->
  <div class="section-title">🔗 Company Connections (extracted from news)</div>
  <div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:14px">
    {connections_html}
  </div>

  <!-- Sector / macro -->
  {f'<div class="section-title">🌐 Sector Macro Context</div><div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:14px;font-size:13px;color:#94a3b8;line-height:1.6">{sector_rat}</div>' if sector_rat else ''}

  <!-- Market research -->
  <div class="section-title">📚 Recent Market Research</div>
  {mr_html}

  <!-- Articles -->
  <div class="section-title">📰 Scored Articles ({len(articles)}) — expand for details</div>
  {"".join(arts_html) if arts_html else '<div style="color:#475569;font-size:13px">No scored articles yet.</div>'}

</div>
</body>
</html>""")


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = 8000
    def _open():
        time.sleep(1.2)
        webbrowser.open(f"http://localhost:{port}")
    threading.Thread(target=_open, daemon=True).start()
    print(f"TradeIntel Viewer → http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
