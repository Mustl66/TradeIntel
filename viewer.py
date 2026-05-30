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
from fastapi.responses import HTMLResponse

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

@app.get("/", response_class=HTMLResponse)
def index(sort: str = "final_score", dir: str = "desc"):
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
  <span class="badge" style="color:#475569;margin-left:auto">🕐 {now}</span>
</div>

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
  document.getElementById('modal-overlay').classList.add('open');
  document.getElementById('modal-body').innerHTML = '<div class="empty"><span class="spinner"></span> Loading…</div>';
  fetch('/symbol/' + id)
    .then(r => r.text())
    .then(html => {{ document.getElementById('modal-body').innerHTML = html; }})
    .catch(() => {{ document.getElementById('modal-body').innerHTML = '<div class="empty">Failed to load.</div>'; }});
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
</script>

</body>
</html>""")


# ── Symbol detail (loaded inside modal) ───────────────────────────────────────

@app.get("/symbol/{sym_id}", response_class=HTMLResponse)
def symbol_detail(sym_id: int):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, symbol, company_name, industry, exchange,
                       final_score, score_updated_at, symbol_master_summary
                FROM symbols WHERE id = %s
            """, (sym_id,))
            sym = cur.fetchone()
            if not sym:
                return HTMLResponse('<div class="empty">Symbol not found.</div>')

            cur.execute("""
                SELECT id, title, url, published_at, source_name,
                       sentiment_score, weighted_sentiment,
                       article_summary, score_rationale, forecast_until_earnings,
                       key_events
                FROM news_articles
                WHERE symbol_id = %s AND sentiment_score IS NOT NULL
                ORDER BY sentiment_score DESC NULLS LAST
                LIMIT 50
            """, (sym_id,))
            articles = cur.fetchall()
    finally:
        conn.close()

    sc = sym["final_score"]
    col = score_color(sc)
    lbl, lcol = score_label(sc)
    upd = sym["score_updated_at"].strftime("%Y-%m-%d %H:%M") if sym["score_updated_at"] else "—"

    arts_html = ""
    for a in articles:
        ascore = a["sentiment_score"]
        acol = score_color(ascore)
        pub = a["published_at"].strftime("%Y-%m-%d %H:%M") if a["published_at"] else "?"
        title = (a["title"] or "Untitled").replace("<","&lt;").replace(">","&gt;")
        arts_html += f"""
        <div class="art-item" onclick="toggleDetail(this)" data-id="{a['id']}">
          <div class="art-title">{title}</div>
          <div class="art-meta">
            <span class="art-score" style="color:{acol}">{fmt_score(ascore)}</span>
            <span>📅 {pub}</span>
            <span>🔗 {a.get('source_name') or '?'}</span>
            {"<a href='" + a['url'] + "' target='_blank' onclick='event.stopPropagation()'>↗ Source</a>" if a.get('url') else ''}
          </div>
          <div class="art-detail" style="display:none;margin-top:12px">
            {"<div class='detail-label' style='color:#60a5fa'>SUMMARY</div><div class='detail-box'>" + (a['article_summary'] or '—').replace('<','&lt;') + "</div>" if a.get('article_summary') else ''}
            {"<div class='detail-label' style='color:#f59e0b;margin-top:10px'>RATIONALE</div><div class='detail-box'>" + (a['score_rationale'] or '—').replace('<','&lt;') + "</div>" if a.get('score_rationale') else ''}
            {"<div class='detail-label' style='color:#a78bfa;margin-top:10px'>FORECAST</div><div class='detail-box'>" + (a['forecast_until_earnings'] or '—').replace('<','&lt;') + "</div>" if a.get('forecast_until_earnings') else ''}
          </div>
        </div>"""

    master = (sym.get("symbol_master_summary") or "").replace("<","&lt;")

    return HTMLResponse(f"""
<div>
  <div style="margin-bottom:16px">
    <div style="font-size:24px;font-weight:900;color:#60a5fa">{sym['symbol']}</div>
    <div style="font-size:13px;color:#64748b;margin-top:2px">{sym.get('company_name') or '—'} &nbsp;·&nbsp; {sym.get('industry') or '—'} &nbsp;·&nbsp; {sym.get('exchange') or '—'}</div>
  </div>

  <div class="stat-grid">
    <div class="stat-card">
      <div class="stat-label">Final Score</div>
      <div class="stat-val" style="color:{col}">{fmt_score(sc)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Signal</div>
      <div class="stat-val" style="color:{lcol};font-size:14px;padding-top:6px">{lbl}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Updated</div>
      <div class="stat-val" style="font-size:13px;color:#94a3b8;padding-top:6px">{upd}</div>
    </div>
  </div>

  {"<div class='detail-section'><div class='detail-label' style='color:#fb923c'>MASTER SUMMARY</div><div class='detail-box'>" + master + "</div></div>" if master else ""}

  <div style="font-size:12px;color:#475569;font-weight:700;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px">
    Scored Articles ({len(articles)}) — click to expand
  </div>
  {"".join(arts_html) if arts_html else "<div class='empty'>No scored articles yet.</div>"}
</div>
""")


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = 8000
    def _open():
        time.sleep(1.2)
        webbrowser.open(f"http://localhost:{port}")
    threading.Thread(target=_open, daemon=True).start()
    print(f"TradeIntel Viewer → http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
