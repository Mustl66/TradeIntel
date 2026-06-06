"""
admin.py — TradeIntel RSS Feed Manager
=======================================
Standalone admin panel — completely independent of main.py.

Run:
    python admin.py
    → opens http://localhost:8055

Features:
  - Browse all symbols (search by ticker or name)
  - View all RSS feeds per symbol
  - Add new feed (with live validation before saving)
  - Edit existing feed URL
  - Toggle feed active/inactive
  - Delete feed
"""

import sys
import os
import json
import webbrowser
import threading
import logging
import time
import feedparser
import requests
import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(__file__))
from config import DB_CONFIG

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse
import uvicorn

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

app = FastAPI(title="TradeIntel Admin", docs_url=None, redoc_url=None)

# ── DB helper ─────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def _ensure_priority_queue_table():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS priority_queue (
                    symbol_id INT PRIMARY KEY REFERENCES symbols(id) ON DELETE CASCADE,
                    rank      INT NOT NULL,
                    added_at  TIMESTAMP DEFAULT NOW()
                )
            """)
        conn.commit()
    finally:
        conn.close()


# ── HTML shell ────────────────────────────────────────────────────────────────

def page(body: str, title: str = "TradeIntel Admin") -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<script src="https://unpkg.com/htmx.org@1.9.10"></script>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    background: #0f1117;
    color: #e2e8f0;
    min-height: 100vh;
    padding: 24px;
  }}
  h1 {{ font-size: 1.4rem; font-weight: 700; color: #f8fafc; letter-spacing: -0.3px; }}
  h2 {{ font-size: 1rem; font-weight: 600; color: #94a3b8; margin-bottom: 12px; }}
  .header {{ display: flex; align-items: center; gap: 16px; margin-bottom: 28px; }}
  .badge {{
    background: #1e3a5f; color: #60a5fa;
    font-size: 0.7rem; font-weight: 700;
    padding: 3px 9px; border-radius: 99px; letter-spacing: 0.5px;
  }}
  .layout {{ display: grid; grid-template-columns: 340px 1fr; gap: 20px; height: calc(100vh - 100px); }}
  .panel {{
    background: #161b27; border: 1px solid #1e2535;
    border-radius: 10px; overflow: hidden; display: flex; flex-direction: column;
  }}
  .panel-head {{
    padding: 14px 16px; border-bottom: 1px solid #1e2535;
    background: #1a2133;
  }}
  .panel-body {{ overflow-y: auto; flex: 1; }}
  input[type=text], input[type=url] {{
    width: 100%; background: #0f1117; border: 1px solid #2d3748;
    color: #e2e8f0; border-radius: 6px; padding: 8px 12px;
    font-size: 0.85rem; outline: none; transition: border-color .15s;
  }}
  input[type=text]:focus, input[type=url]:focus {{ border-color: #3b82f6; }}
  .search-wrap {{ padding: 10px 12px; }}
  .sym-row {{
    display: flex; align-items: center; justify-content: space-between;
    padding: 10px 16px; cursor: pointer; border-bottom: 1px solid #1a2133;
    transition: background .1s;
  }}
  .sym-row:hover {{ background: #1e2a3a; }}
  .sym-row.active {{ background: #1e3a5f; border-left: 3px solid #3b82f6; }}
  .sym-ticker {{ font-weight: 700; font-size: 0.9rem; color: #60a5fa; }}
  .sym-name {{ font-size: 0.75rem; color: #64748b; max-width: 180px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .feed-count {{
    font-size: 0.7rem; font-weight: 700; color: #475569;
    background: #1a2133; padding: 2px 7px; border-radius: 99px;
  }}
  .fetch-btn {{
    font-size: 0.65rem; padding: 2px 7px; border-radius: 99px;
    background: #1e3a5f; color: #60a5fa; border: 1px solid #2563eb;
    cursor: pointer; transition: background 0.15s;
    line-height: 1.4;
  }}
  .fetch-btn:hover {{ background: #2563eb; color: #fff; }}
  .fetch-btn:active {{ transform: scale(0.95); }}
  .fetch-log {{
    font-family: monospace; font-size: 0.75rem; color: #94a3b8;
    background: #0f172a; border-radius: 8px; padding: 14px 16px;
    margin: 16px; white-space: pre-wrap; word-break: break-all;
    max-height: 500px; overflow-y: auto; border: 1px solid #1e2535;
  }}
  .fetch-log .ok  {{ color: #4ade80; }}
  .fetch-log .err {{ color: #f87171; }}
  .fetch-log .hdr {{ color: #38bdf8; font-weight: bold; }}
  .feed-card {{
    background: #1a2133; border: 1px solid #1e2535;
    border-radius: 8px; padding: 14px 16px; margin: 0 16px 12px;
  }}
  .feed-card:first-child {{ margin-top: 16px; }}
  .feed-url {{
    font-size: 0.8rem; color: #93c5fd; word-break: break-all;
    text-decoration: none;
  }}
  .feed-url:hover {{ color: #bfdbfe; }}
  .feed-meta {{ display: flex; gap: 10px; margin-top: 6px; flex-wrap: wrap; }}
  .chip {{
    font-size: 0.68rem; font-weight: 600; padding: 2px 8px;
    border-radius: 99px; letter-spacing: 0.3px;
  }}
  .chip-green {{ background: #14532d; color: #4ade80; }}
  .chip-red   {{ background: #450a0a; color: #f87171; }}
  .chip-blue  {{ background: #1e3a5f; color: #60a5fa; }}
  .chip-gray  {{ background: #1e2535; color: #94a3b8; }}
  .feed-actions {{ display: flex; gap: 8px; margin-top: 10px; }}
  .btn {{
    font-size: 0.75rem; font-weight: 600; padding: 5px 12px;
    border-radius: 6px; border: none; cursor: pointer; transition: opacity .15s;
  }}
  .btn:hover {{ opacity: .8; }}
  .btn-primary {{ background: #2563eb; color: #fff; }}
  .btn-danger  {{ background: #991b1b; color: #fca5a5; }}
  .btn-warn    {{ background: #78350f; color: #fcd34d; }}
  .btn-ghost   {{ background: #1e2535; color: #94a3b8; }}
  .add-form {{
    margin: 0 16px 16px; background: #0f1117;
    border: 1px dashed #2d3748; border-radius: 8px; padding: 14px;
  }}
  .add-form h3 {{ font-size: 0.8rem; color: #94a3b8; margin-bottom: 10px; font-weight: 600; }}
  .form-row {{ display: flex; gap: 8px; align-items: center; }}
  .select-sm {{
    background: #0f1117; border: 1px solid #2d3748; color: #e2e8f0;
    border-radius: 6px; padding: 8px 10px; font-size: 0.8rem; outline: none;
  }}
  .validation-box {{
    margin-top: 10px; padding: 10px 12px; border-radius: 6px;
    font-size: 0.8rem; line-height: 1.5;
  }}
  .val-ok  {{ background: #14532d33; border: 1px solid #166534; color: #4ade80; }}
  .val-err {{ background: #450a0a33; border: 1px solid #7f1d1d; color: #f87171; }}
  .edit-form {{ margin-top: 10px; display: flex; gap: 8px; }}
  .empty-state {{
    padding: 40px 16px; text-align: center; color: #475569; font-size: 0.85rem;
  }}
  .htmx-indicator {{ opacity: 0; transition: opacity 200ms; }}
  .htmx-request .htmx-indicator {{ opacity: 1; }}
  .spinner {{ display: inline-block; width: 12px; height: 12px;
    border: 2px solid #334155; border-top-color: #3b82f6;
    border-radius: 50%; animation: spin .6s linear infinite; }}
  @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
  .detail-header {{
    padding: 16px; border-bottom: 1px solid #1e2535;
    background: #1a2133;
  }}
  .detail-ticker {{ font-size: 1.2rem; font-weight: 800; color: #60a5fa; }}
  .detail-name   {{ font-size: 0.8rem; color: #64748b; margin-top: 2px; }}
  .tabs {{ display: flex; gap: 0; border-bottom: 1px solid #1e2535; }}
  .tab-btn {{
    padding: 10px 20px; font-size: 0.82rem; font-weight: 600;
    background: none; border: none; color: #64748b; cursor: pointer;
    border-bottom: 2px solid transparent; margin-bottom: -1px;
    transition: color .15s, border-color .15s;
  }}
  .tab-btn:hover {{ color: #94a3b8; }}
  .tab-btn.active {{ color: #60a5fa; border-bottom-color: #3b82f6; }}
  .news-card {{
    background: #1a2133; border: 1px solid #1e2535;
    border-radius: 8px; padding: 14px 16px; margin: 0 16px 12px;
  }}
  .news-card:first-child {{ margin-top: 16px; }}
  .news-title {{
    font-size: 0.88rem; font-weight: 600; color: #e2e8f0;
    text-decoration: none; line-height: 1.4;
  }}
  .news-title:hover {{ color: #93c5fd; }}
  .news-meta {{ display: flex; gap: 10px; margin-top: 5px; flex-wrap: wrap; align-items: center; }}
  .news-date {{ font-size: 0.72rem; color: #475569; }}
  .news-preview {{
    font-size: 0.78rem; color: #64748b; margin-top: 8px;
    line-height: 1.5; display: -webkit-box;
    -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden;
  }}
  .pagination {{ display: flex; gap: 8px; padding: 16px; justify-content: center; }}
  .news-count {{ font-size: 0.72rem; color: #475569; padding: 2px 7px; background: #1a2133; border-radius: 99px; }}
  .filter-bar {{ display: flex; gap: 5px; flex-wrap: wrap; padding: 8px 12px; border-bottom: 1px solid #1e2535; background: #161b27; }}
  .filter-btn {{
    font-size: 0.68rem; font-weight: 700; padding: 3px 9px;
    border-radius: 99px; border: 1px solid #2d3748;
    background: #0f1117; color: #64748b; cursor: pointer;
    transition: all .15s; letter-spacing: 0.3px;
  }}
  .filter-btn:hover {{ background: #1e2535; color: #94a3b8; }}
  .filter-btn.active {{ background: #1e3a5f; color: #60a5fa; border-color: #3b82f6; }}
  .filter-btn.warn {{ }}
  .filter-btn.warn.active {{ background: #451a03; color: #fb923c; border-color: #c2410c; }}
  .filter-btn.danger.active {{ background: #450a0a; color: #f87171; border-color: #991b1b; }}
  .art-count {{
    font-size: 0.68rem; font-weight: 700; padding: 2px 7px;
    border-radius: 99px; min-width: 28px; text-align: center;
  }}
  .art-zero  {{ background: #450a0a22; color: #f87171; border: 1px solid #7f1d1d44; }}
  .art-low   {{ background: #451a0322; color: #fb923c; border: 1px solid #c2410c44; }}
  .art-ok    {{ background: #14532d22; color: #4ade80; border: 1px solid #15803d44; }}
  .sort-bar {{ display: flex; gap: 5px; padding: 6px 12px; border-bottom: 1px solid #1e2535; align-items: center; }}
  .sort-label {{ font-size: 0.65rem; color: #475569; font-weight: 600; letter-spacing: 0.5px; text-transform: uppercase; }}
  .sort-btn {{
    font-size: 0.68rem; font-weight: 600; padding: 2px 8px;
    border-radius: 99px; border: 1px solid transparent;
    background: none; color: #475569; cursor: pointer;
  }}
  .sort-btn.active {{ color: #60a5fa; border-color: #3b82f6; background: #1e3a5f; }}
  .prio-btn {{
    font-size: 0.75rem; padding: 2px 6px; border-radius: 99px;
    background: none; border: 1px solid transparent;
    color: #475569; cursor: pointer; transition: all .15s; line-height: 1.4;
  }}
  .prio-btn:hover {{ color: #fbbf24; border-color: #fbbf24; }}
  .prio-btn.prio-active {{ color: #fbbf24; border-color: #b45309; background: #451a0333; }}
  .prio-rank {{
    font-size: 0.65rem; font-weight: 800; color: #fbbf24;
    background: #451a0344; border: 1px solid #b4530966;
    padding: 1px 5px; border-radius: 99px;
    display: inline-block; min-width: 18px; text-align: center;
  }}
</style>
</head>
<body>
<div class="header">
  <h1>TradeIntel</h1>
  <span class="badge">RSS MANAGER</span>
  <div style="margin-left:auto;display:flex;align-items:center;gap:10px">
    <button
      hx-get="/market-research"
      hx-target="#feed-panel"
      hx-swap="innerHTML"
      style="background:#1e293b;border:1px solid #6366f1;color:#a5b4fc;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px">
      📊 Market Research
    </button>
    <button
      hx-get="/priority-panel"
      hx-target="#feed-panel"
      hx-swap="innerHTML"
      style="background:#1e293b;border:1px solid #fbbf24;color:#fcd34d;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px">
      ⭐ Priority Queue
    </button>
    <button
      hx-get="/market-scores"
      hx-target="#feed-panel"
      hx-swap="innerHTML"
      style="background:#1e293b;border:1px solid #10b981;color:#6ee7b7;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px">
      📈 Market Scores
    </button>
    <button
      hx-get="/sentiment-leaderboard"
      hx-target="#feed-panel"
      hx-swap="innerHTML"
      style="background:#1e293b;border:1px solid #f59e0b;color:#fcd34d;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px">
      🏆 Sentiment Scores
    </button>
    <button
      onclick="if(!confirm('Reset ALL sentiment scores? This cannot be undone.')) return false;"
      hx-post="/admin/reset-scores"
      hx-target="#feed-panel"
      hx-swap="innerHTML"
      style="background:#1e293b;border:1px solid #ef4444;color:#fca5a5;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px">
      🗑️ Reset Scores
    </button>
    <button
      hx-post="/admin/dedup"
      hx-target="#dedup-result"
      hx-swap="innerHTML"
      style="background:#1e293b;border:1px solid #334155;color:#94a3b8;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px"
      onclick="this.textContent='Running…'">
      🧹 Dedup Articles
    </button>
    <div id="dedup-result" style="font-size:12px;color:#94a3b8"></div>
    <button
      hx-post="/admin/dedup-lang"
      hx-target="#dedup-result"
      hx-swap="innerHTML"
      style="background:#1e293b;border:1px solid #a855f7;color:#d8b4fe;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px"
      onclick="this.textContent='Running…'">
      🌐 Dedup Languages
    </button>
    <button
      hx-post="/admin/dedup-titles"
      hx-target="#dedup-result"
      hx-swap="innerHTML"
      style="background:#1e293b;border:1px solid #a855f7;color:#d8b4fe;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px"
      onclick="this.textContent='Running…'">
      🧹 Dedup Titles (global)
    </button>
    <button
      hx-delete="/admin/sec/all"
      hx-target="#dedup-result"
      hx-swap="innerHTML"
      style="background:#1e293b;border:1px solid #ef4444;color:#fca5a5;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px"
      onclick="if(!confirm('Delete ALL SEC filings from the entire DB? Cannot be undone.')) return false; this.textContent='Deleting…'">
      🗑️ Delete All SEC (Global)
    </button>
  </div>
</div>
<div class="layout">
  <!-- LEFT: symbol list -->
  <div class="panel">
    <div class="panel-head">
      <h2>Symbols</h2>
      <div class="search-wrap" style="padding:0">
        <input type="text" id="sym-search" placeholder="Search ticker, name, or feed source…"
          hx-get="/symbols"
          hx-trigger="keyup changed delay:200ms"
          hx-target="#sym-list"
          hx-include="#sym-search, #sym-filter, #sym-sort"
          name="q"
          autocomplete="off"
        />
      </div>
    </div>
    <!-- filter bar -->
    <div class="filter-bar">
      <span style="font-size:0.65rem;color:#475569;font-weight:700;letter-spacing:0.5px;text-transform:uppercase;align-self:center;">Filter</span>
      <button class="filter-btn active" onclick="setFilter(this,'all')" title="All symbols">All</button>
      <button class="filter-btn danger" onclick="setFilter(this,'zero')" title="No articles yet — needs fixing">0 articles</button>
      <button class="filter-btn warn"   onclick="setFilter(this,'low')"  title="1-4 articles — might be broken">Low &lt;5</button>
      <button class="filter-btn"        onclick="setFilter(this,'ok')"   title="5+ articles">OK</button>
      <button class="filter-btn"        onclick="setFilter(this,'nofeed')" title="No feed URL at all">No feed</button>
      <button class="filter-btn"        onclick="setFilter(this,'priority')" title="Priority queue only">⭐ Priority</button>
    </div>
    <!-- sort bar -->
    <div class="sort-bar">
      <span class="sort-label">Sort</span>
      <button class="sort-btn active" onclick="setSort(this,'alpha')"    title="A→Z">A-Z</button>
      <button class="sort-btn"        onclick="setSort(this,'articles')" title="Fewest articles first">Fewest first</button>
      <button class="sort-btn"        onclick="setSort(this,'most')"     title="Most articles first">Most first</button>
    </div>
    <input type="hidden" id="sym-filter" name="filter" value="all"/>
    <input type="hidden" id="sym-sort"   name="sort"   value="alpha"/>
    <script>
      function setFilter(btn, val) {{
        document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById('sym-filter').value = val;
        htmx.trigger('#sym-search', 'keyup');
      }}
      function setSort(btn, val) {{
        document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById('sym-sort').value = val;
        htmx.trigger('#sym-search', 'keyup');
      }}
    </script>
    <div class="panel-body" id="sym-list"
      hx-get="/symbols" hx-trigger="load, every 30s" hx-target="#sym-list" hx-swap="innerHTML"
      hx-include="#sym-search, #sym-filter, #sym-sort">
      <div class="empty-state"><span class="spinner"></span></div>
    </div>
  </div>

  <!-- RIGHT: feed detail -->
  <div class="panel" id="feed-panel">
    <div class="empty-state" style="padding-top:80px">
      <svg width="40" height="40" fill="none" stroke="#334155" stroke-width="1.5" viewBox="0 0 24 24" style="margin:0 auto 12px">
        <path stroke-linecap="round" d="M6.75 7.5l3 2.25-3 2.25m4.5 0h3M5 20.25h14A2.25 2.25 0 0021.25 18V6A2.25 2.25 0 0019 3.75H5A2.25 2.25 0 002.75 6v12A2.25 2.25 0 005 20.25z"/>
      </svg>
      Select a symbol to manage its RSS feeds
    </div>
  </div>
</div>
</body>
</html>"""


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(page(""))


@app.get("/symbols", response_class=HTMLResponse)
async def symbols(q: str = "", filter: str = "all", sort: str = "alpha"):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # base WHERE
            where_clauses = []
            params = []
            if q.strip():
                where_clauses.append("(s.symbol ILIKE %s OR s.company_name ILIKE %s OR f.feed_url ILIKE %s)")
                params += [f"%{q}%", f"%{q}%", f"%{q}%"]

            # article count filter
            having_clause = ""
            if filter == "zero":
                having_clause = "HAVING COUNT(na.id) = 0"
            elif filter == "low":
                having_clause = "HAVING COUNT(na.id) BETWEEN 1 AND 4"
            elif filter == "ok":
                having_clause = "HAVING COUNT(na.id) >= 5"
            elif filter == "nofeed":
                having_clause = "HAVING COUNT(f.id) = 0"
            elif filter == "priority":
                having_clause = "HAVING COUNT(pq.symbol_id) > 0"

            # sort
            order = {
                "articles": "article_count ASC, s.symbol ASC",
                "most":     "article_count DESC, s.symbol ASC",
            }.get(sort, "s.symbol ASC")

            where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
            cur.execute(f"""
                SELECT s.id, s.symbol, s.company_name,
                       COUNT(DISTINCT f.id) FILTER (WHERE f.is_active) AS active_feeds,
                       COUNT(DISTINCT f.id) AS total_feeds,
                       COUNT(DISTINCT na.id) AS article_count,
                       MAX(pq.rank) AS prio_rank
                FROM symbols s
                LEFT JOIN rss_feeds f   ON f.symbol_id  = s.id
                LEFT JOIN news_articles na ON na.symbol_id = s.id
                LEFT JOIN priority_queue pq ON pq.symbol_id = s.id
                {where_sql}
                GROUP BY s.id
                {having_clause}
                ORDER BY {order}
                LIMIT 500
            """, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return HTMLResponse('<div class="empty-state">No symbols found</div>')

    html = ""
    for r in rows:
        feeds_label = f"{r['active_feeds']}/{r['total_feeds']}" if r['total_feeds'] else "0"
        art = r['article_count']
        if art == 0:
            art_cls = "art-count art-zero"
        elif art < 5:
            art_cls = "art-count art-low"
        else:
            art_cls = "art-count art-ok"
        art_label = str(art) if art < 1000 else f"{art//1000}k"
        prio_rank = r.get('prio_rank')
        prio_badge = f'<span class="prio-rank" title="Priority #{prio_rank}">#{prio_rank}</span>' if prio_rank else ''
        prio_btn_cls = "prio-btn prio-active" if prio_rank else "prio-btn"
        prio_title = "Remove from priority queue" if prio_rank else "Add to priority queue"
        html += f"""
        <div class="sym-row"
          hx-get="/symbol/{r['id']}/feeds"
          hx-target="#feed-panel"
          hx-swap="innerHTML"
          onclick="document.querySelectorAll('.sym-row').forEach(e=>e.classList.remove('active'));this.classList.add('active')"
        >
          <div>
            <div class="sym-ticker">{r['symbol']} {prio_badge}</div>
            <div class="sym-name">{r['company_name'] or '—'}</div>
          </div>
          <div style="display:flex;gap:5px;align-items:center;">
            <span class="{art_cls}" title="{art} articles">{art_label}</span>
            <span class="feed-count">{feeds_label}</span>
            <button
              class="{prio_btn_cls}"
              title="{prio_title}"
              hx-post="/priority/toggle/{r['id']}"
              hx-target="#sym-list"
              hx-swap="innerHTML"
              hx-include="#sym-search, #sym-filter, #sym-sort"
              onclick="event.stopPropagation()"
            >⭐</button>
            <button
              class="fetch-btn"
              title="Fetch news for {r['symbol']}"
              hx-post="/symbol/{r['id']}/fetch"
              hx-target="#feed-panel"
              hx-swap="innerHTML"
              hx-indicator="#fetch-spinner-{r['id']}"
              hx-timeout="600000"
              onclick="event.stopPropagation();document.querySelectorAll('.sym-row').forEach(e=>e.classList.remove('active'));this.closest('.sym-row').classList.add('active')"
            >▶</button>
            <span id="fetch-spinner-{r['id']}" class="htmx-indicator spinner" style="margin-left:4px"></span>
          </div>
        </div>"""
    return HTMLResponse(html)


@app.get("/symbol/{sym_id}/feeds", response_class=HTMLResponse)
async def symbol_feeds(sym_id: int):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, symbol, company_name FROM symbols WHERE id=%s", (sym_id,))
            sym = cur.fetchone()
            if not sym:
                raise HTTPException(404, "Symbol not found")
            cur.execute("""
                SELECT id, feed_url, feed_type, source, is_active,
                       discovered_at, last_checked_at
                FROM rss_feeds WHERE symbol_id=%s ORDER BY discovered_at
            """, (sym_id,))
            feeds = cur.fetchall()
    finally:
        conn.close()

    html = f"""
    <div class="detail-header">
      <div class="detail-ticker">{sym['symbol']}</div>
      <div class="detail-name">{sym['company_name'] or ''}</div>
    </div>
    <div class="tabs">
      <button class="tab-btn active" id="tab-feeds-{sym_id}"
        onclick="switchTab({sym_id},'feeds')">RSS Feeds</button>
      <button class="tab-btn" id="tab-news-{sym_id}"
        onclick="switchTab({sym_id},'news')">News</button>
      <button class="tab-btn" id="tab-sec-{sym_id}"
        onclick="switchTab({sym_id},'sec')">SEC Filings</button>
      <button class="tab-btn" id="tab-debug-{sym_id}"
        onclick="switchTab({sym_id},'debug')">🔬 LLM Debug</button>
      <button class="tab-btn" id="tab-intel-{sym_id}"
        onclick="switchTab({sym_id},'intel')">🧠 Intelligence</button>
    </div>
    <div id="tab-content-{sym_id}" class="panel-body" style="padding-bottom:20px">
    """

    if not feeds:
        html += '<div class="empty-state">No RSS feeds yet — add one below</div>'

    for f in feeds:
        active_chip = '<span class="chip chip-green">ACTIVE</span>' if f['is_active'] else '<span class="chip chip-red">INACTIVE</span>'
        source_chip = f'<span class="chip chip-blue">{f["source"]}</span>'
        type_chip   = f'<span class="chip chip-gray">{f["feed_type"]}</span>'
        toggle_label = "Deactivate" if f['is_active'] else "Activate"
        toggle_class = "btn btn-warn" if f['is_active'] else "btn btn-primary"

        html += f"""
        <div class="feed-card" id="feed-{f['id']}">
          <a class="feed-url" href="{f['feed_url']}" target="_blank">{f['feed_url']}</a>
          <div class="feed-meta">{active_chip}{source_chip}{type_chip}</div>
          <div class="feed-actions">
            <button class="{toggle_class}" hx-patch="/feeds/{f['id']}/toggle"
              hx-target="#feed-{f['id']}" hx-swap="outerHTML">{toggle_label}</button>
            <button class="btn btn-ghost" onclick="toggleEdit({f['id']})">Edit URL</button>
            <button class="btn btn-danger"
              hx-delete="/feeds/{f['id']}"
              hx-confirm="Delete this feed permanently?"
              hx-target="#feed-{f['id']}" hx-swap="outerHTML">Delete</button>
          </div>
          <div id="edit-{f['id']}" style="display:none">
            <div class="edit-form">
              <input type="url" id="edit-url-{f['id']}" value="{f['feed_url']}"
                placeholder="New feed URL" style="flex:1"/>
              <select class="select-sm" id="edit-type-{f['id']}">
                <option value="rss"    {'selected' if f['feed_type']=='rss'    else ''}>rss</option>
                <option value="atom"   {'selected' if f['feed_type']=='atom'   else ''}>atom</option>
                <option value="html"   {'selected' if f['feed_type']=='html'   else ''}>html</option>
                <option value="api"    {'selected' if f['feed_type']=='api'    else ''}>api</option>
                <option value="unknown"{'selected' if f['feed_type']=='unknown' else ''}>unknown</option>
              </select>
              <button class="btn btn-primary"
                onclick="saveEdit({f['id']})">Save</button>
              <button class="btn btn-ghost"
                onclick="toggleEdit({f['id']})">Cancel</button>
            </div>
            <div id="edit-val-{f['id']}"></div>
          </div>
        </div>"""

    # Add new feed form
    html += f"""
    <div class="add-form">
      <h3>+ Add new feed</h3>
      <div class="form-row">
        <input type="url" id="new-url-{sym_id}" placeholder="https://example.com/feed.xml" style="flex:1"/>
        <select class="select-sm" id="new-type-{sym_id}">
          <option value="rss">rss</option>
          <option value="atom">atom</option>
          <option value="html">html</option>
          <option value="api">api</option>
          <option value="unknown">unknown</option>
        </select>
        <select class="select-sm" id="new-source-{sym_id}">
          <option value="company_ir">Company IR</option>
          <option value="globenewswire">GlobeNewswire</option>
          <option value="other">Other</option>
        </select>
        <button class="btn btn-ghost"
          onclick="validateFeed({sym_id})">Validate</button>
        <button class="btn btn-primary"
          onclick="addFeed({sym_id})">Add</button>
      </div>
      <div id="val-result-{sym_id}"></div>
    </div>
    </div>  <!-- end tab-content -->

    <script>
    function switchTab(symId, tab) {{
      document.getElementById('tab-feeds-' + symId).classList.toggle('active', tab === 'feeds');
      document.getElementById('tab-news-' + symId).classList.toggle('active', tab === 'news');
      document.getElementById('tab-sec-' + symId).classList.toggle('active', tab === 'sec');
      document.getElementById('tab-debug-' + symId).classList.toggle('active', tab === 'debug');
      document.getElementById('tab-intel-' + symId).classList.toggle('active', tab === 'intel');
      const content = document.getElementById('tab-content-' + symId);
      if (tab === 'news') {{
        content.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';
        htmx.ajax('GET', '/symbol/' + symId + '/news?page=1', content);
      }} else if (tab === 'sec') {{
        content.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';
        htmx.ajax('GET', '/symbol/' + symId + '/sec?page=1', content);
      }} else if (tab === 'debug') {{
        content.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';
        htmx.ajax('GET', '/symbol/' + symId + '/debug?page=1', content);
      }} else if (tab === 'intel') {{
        content.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';
        htmx.ajax('GET', '/symbol/' + symId + '/intel', content);
      }} else {{
        htmx.ajax('GET', '/symbol/' + symId + '/feeds', '#feed-panel');
      }}
    }}
    function toggleEdit(id) {{
      const el = document.getElementById('edit-' + id);
      el.style.display = el.style.display === 'none' ? 'block' : 'none';
    }}

    async function validateFeed(symId) {{
      const url = document.getElementById('new-url-' + symId).value.trim();
      if (!url) return;
      const box = document.getElementById('val-result-' + symId);
      box.innerHTML = '<div class="validation-box" style="color:#94a3b8">Validating… <span class="spinner"></span></div>';
      const r = await fetch('/feeds/validate?url=' + encodeURIComponent(url));
      const data = await r.json();
      if (data.ok) {{
        box.innerHTML = `<div class="validation-box val-ok">
          ✓ Valid feed — <strong>${{data.title}}</strong><br>
          ${{data.entries}} entries found · feed type: ${{data.feed_type}}
        </div>`;
      }} else {{
        box.innerHTML = `<div class="validation-box val-err">✗ ${{data.error}}</div>`;
      }}
    }}

    async function addFeed(symId) {{
      const url    = document.getElementById('new-url-'    + symId).value.trim();
      const source = document.getElementById('new-source-' + symId).value;
      const ftype  = document.getElementById('new-type-'   + symId).value;
      if (!url) return;
      const r = await fetch('/feeds', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/x-www-form-urlencoded'}},
        body: 'symbol_id=' + symId + '&feed_url=' + encodeURIComponent(url) + '&source=' + source + '&feed_type=' + ftype
      }});
      if (r.ok) {{
        htmx.ajax('GET', '/symbol/' + symId + '/feeds', '#feed-panel');
      }} else {{
        const data = await r.json();
        alert('Error: ' + (data.detail || 'Unknown error'));
      }}
    }}

    async function saveEdit(feedId) {{
      const url    = document.getElementById('edit-url-'  + feedId).value.trim();
      const ftype  = document.getElementById('edit-type-' + feedId).value;
      if (!url) return;
      const valBox = document.getElementById('edit-val-' + feedId);
      // skip RSS validation for html/api types
      if (ftype === 'rss' || ftype === 'atom' || ftype === 'unknown') {{
        valBox.innerHTML = '<div class="validation-box" style="color:#94a3b8">Validating…</div>';
        const vr = await fetch('/feeds/validate?url=' + encodeURIComponent(url));
        const vd = await vr.json();
        if (!vd.ok) {{
          valBox.innerHTML = `<div class="validation-box val-err">✗ ${{vd.error}} — fix the URL before saving.</div>`;
          return;
        }}
      }}
      const r = await fetch('/feeds/' + feedId, {{
        method: 'PUT',
        headers: {{'Content-Type': 'application/x-www-form-urlencoded'}},
        body: 'feed_url=' + encodeURIComponent(url) + '&feed_type=' + ftype
      }});
      if (r.ok) {{
        const html = await r.text();
        document.getElementById('feed-' + feedId).outerHTML = html;
      }} else {{
        const data = await r.json();
        valBox.innerHTML = `<div class="validation-box val-err">✗ ${{data.detail}}</div>`;
      }}
    }}
    </script>
    """
    return HTMLResponse(html)


@app.get("/feeds/validate")
async def validate_feed(url: str):
    try:
        resp = requests.get(url, timeout=12,
                            headers={"User-Agent": "TradeIntel-Admin/1.0 (feed validator)"})
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        if parsed.bozo and not parsed.entries:
            return {"ok": False, "error": f"Not a valid RSS/Atom feed: {parsed.bozo_exception}"}
        title     = parsed.feed.get("title", "Untitled feed")
        entries   = len(parsed.entries)
        feed_type = "atom" if parsed.version and "atom" in parsed.version else "rss"
        return {"ok": True, "title": title, "entries": entries, "feed_type": feed_type}
    except requests.exceptions.Timeout:
        return {"ok": False, "error": "Request timed out (>12s)"}
    except requests.exceptions.RequestException as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── Fetch symbol pipeline ──────────────────────────────────────────────────────

@app.post("/symbol/{sym_id}/fetch", response_class=HTMLResponse)
async def fetch_symbol(sym_id: int):
    import subprocess, sys, time
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT symbol, company_name FROM symbols WHERE id=%s", (sym_id,))
            row = cur.fetchone()
            if not row:
                return HTMLResponse('<div class="fetch-log"><span class="err">Symbol not found</span></div>')
            symbol = row["symbol"]
            name   = row["company_name"] or symbol

            cur.execute("SELECT COUNT(*) AS c FROM news_articles WHERE symbol_id=%s", (sym_id,))
            before = cur.fetchone()["c"]
    finally:
        conn.close()

    script = os.path.join(os.path.dirname(__file__), "test_symbol.py")
    start  = time.time()
    try:
        result = subprocess.run(
            [sys.executable, script, symbol],
            capture_output=True, text=True, timeout=600,
            cwd=os.path.dirname(__file__)
        )
        output = (result.stdout + result.stderr).strip()
        elapsed = round(time.time() - start, 1)
    except subprocess.TimeoutExpired:
        output  = "ERROR: timed out after 600s"
        elapsed = 600

    conn2 = get_conn()
    try:
        with conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS c FROM news_articles WHERE symbol_id=%s", (sym_id,))
            after = cur.fetchone()["c"]
    finally:
        conn2.close()

    new_count = after - before

    # colorise output lines
    lines_html = ""
    for line in output.splitlines():
        ll = line.lower()
        if any(x in ll for x in ("error", "fail", "exception", "traceback")):
            lines_html += f'<span class="err">{line}</span>\n'
        elif any(x in ll for x in ("inserted", "new", "fetched", "ok", "✓", "articles")):
            lines_html += f'<span class="ok">{line}</span>\n'
        else:
            lines_html += f'{line}\n'

    summary_cls = "ok" if new_count > 0 else ("err" if new_count == 0 and before == 0 else "")
    summary = f'+{new_count} new articles inserted' if new_count > 0 else 'no new articles (all already in DB or no feeds)'

    return HTMLResponse(f"""
    <div style="padding:16px 16px 8px">
      <div style="font-size:0.85rem;color:#e2e8f0;margin-bottom:8px">
        <span class="fetch-log hdr" style="background:none;padding:0;margin:0">
          ▶ {symbol} — {name}
        </span>
        &nbsp;<span style="color:#64748b;font-size:0.75rem">{elapsed}s</span>
      </div>
      <div style="font-size:0.8rem;margin-bottom:8px">
        Before: <b>{before}</b> articles &nbsp;→&nbsp; After: <b>{after}</b> articles
        &nbsp;<span class="fetch-log {summary_cls}" style="background:none;padding:0;margin:0">({summary})</span>
      </div>
    </div>
    <div class="fetch-log">{lines_html}</div>
    <div style="padding:0 16px 16px">
      <button class="fetch-btn" style="font-size:0.75rem;padding:4px 12px"
        hx-get="/symbol/{sym_id}/feeds"
        hx-target="#feed-panel"
        hx-swap="innerHTML">
        ← Back to feeds
      </button>
    </div>
    """)


@app.post("/feeds", response_class=HTMLResponse)
async def add_feed(
    symbol_id: int = Form(...),
    feed_url:  str = Form(...),
    source:    str = Form("other"),
    feed_type: str = Form("unknown"),
):
    valid_sources = {"globenewswire", "company_ir", "other"}
    if source not in valid_sources:
        raise HTTPException(400, f"Invalid source. Must be one of: {valid_sources}")

    valid_types = {"rss", "atom", "html", "api", "unknown"}
    if feed_type not in valid_types:
        feed_type = "unknown"

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rss_feeds (symbol_id, feed_url, feed_type, source, is_active)
                VALUES (%s, %s, %s, %s, TRUE)
                ON CONFLICT (feed_url) DO NOTHING
            """, (symbol_id, feed_url, feed_type, source))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(400, str(e))
    finally:
        conn.close()

    return HTMLResponse("ok", status_code=200)


@app.put("/feeds/{feed_id}", response_class=HTMLResponse)
async def edit_feed(feed_id: int, feed_url: str = Form(...), feed_type: str = Form("unknown")):
    # use the type the user explicitly chose — don't re-detect and overwrite
    valid_types = {"rss", "atom", "html", "api", "unknown"}
    if feed_type not in valid_types:
        feed_type = "unknown"

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                UPDATE rss_feeds SET feed_url=%s, feed_type=%s
                WHERE id=%s RETURNING id, feed_url, feed_type, source, is_active, discovered_at, last_checked_at
            """, (feed_url, feed_type, feed_id))
            f = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(400, str(e))
    finally:
        conn.close()

    if not f:
        raise HTTPException(404, "Feed not found")

    return HTMLResponse(_feed_card_html(f))


@app.patch("/feeds/{feed_id}/toggle", response_class=HTMLResponse)
async def toggle_feed(feed_id: int):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                UPDATE rss_feeds SET is_active = NOT is_active
                WHERE id=%s
                RETURNING id, feed_url, feed_type, source, is_active, discovered_at, last_checked_at
            """, (feed_id,))
            f = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(400, str(e))
    finally:
        conn.close()

    if not f:
        raise HTTPException(404, "Feed not found")

    return HTMLResponse(_feed_card_html(f))


@app.delete("/feeds/{feed_id}", response_class=HTMLResponse)
async def delete_feed(feed_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM rss_feeds WHERE id=%s", (feed_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(400, str(e))
    finally:
        conn.close()
    # return empty string — HTMX replaces the card with nothing
    return HTMLResponse("")


# ── News route ────────────────────────────────────────────────────────────────

@app.get("/symbol/{sym_id}/news", response_class=HTMLResponse)
async def symbol_news(sym_id: int, page: int = 1, q: str = ""):
    per_page = 20
    offset   = (page - 1) * per_page
    keyword  = q.strip()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Dedup by normalized title in-query: keep the article with the highest
            # sentiment_score (scored > unscored), tie-break by newest published_at, then id.
            base_cte = """
                WITH ranked AS (
                    SELECT
                        na.id, na.title, na.url, na.published_at, na.inserted_at,
                        na.full_text, na.sentiment_score, na.weighted_sentiment,
                        na.article_summary, na.source_name,
                        rf.feed_url,
                        ROW_NUMBER() OVER (
                            PARTITION BY LOWER(TRIM(COALESCE(na.title, '')))
                            ORDER BY
                                (na.sentiment_score IS NOT NULL) DESC,
                                na.published_at DESC NULLS LAST,
                                na.id ASC
                        ) AS rn
                    FROM news_articles na
                    LEFT JOIN rss_feeds rf ON rf.id = na.feed_id
                    WHERE na.symbol_id = %s
                      AND (na.source_name IS NULL OR na.source_name != 'edgar_8k')
                      {kw_filter}
                ),
                deduped AS (SELECT * FROM ranked WHERE rn = 1)
            """
            if keyword:
                kw_filter = "AND (na.title ILIKE %s OR na.full_text ILIKE %s)"
                params_base = (sym_id, f"%{keyword}%", f"%{keyword}%")
            else:
                kw_filter = ""
                params_base = (sym_id,)

            cte = base_cte.format(kw_filter=kw_filter)

            cur.execute(cte + " SELECT COUNT(*) AS total FROM deduped", params_base)
            total = cur.fetchone()["total"]

            cur.execute(
                cte + " SELECT * FROM deduped ORDER BY published_at DESC NULLS LAST LIMIT %s OFFSET %s",
                params_base + (per_page, offset)
            )
            articles = cur.fetchall()
    finally:
        conn.close()

    # ── Search box ─────────────────────────────────────────────────────────────
    q_safe = keyword.replace('"', '&quot;')
    html = f"""
    <div style="padding:12px 16px 4px">
      <div style="display:flex;gap:8px;align-items:center">
        <input type="text" id="news-search-{sym_id}" placeholder="🔍 Search title or body (FDA, patent, earnings…)"
          value="{q_safe}"
          hx-get="/symbol/{sym_id}/news"
          hx-trigger="keyup changed delay:300ms"
          hx-target="#tab-content-{sym_id}"
          hx-include="#news-search-{sym_id}"
          name="q"
          autocomplete="off"
          style="flex:1"
        />
        <button
          onclick="if(!confirm('Delete duplicate-title articles for this symbol? Keeps the scored / newest one per title.')) return false;"
          hx-post="/symbol/{sym_id}/dedup-titles"
          hx-target="#tab-content-{sym_id}"
          hx-swap="innerHTML"
          style="background:#1e1b3b;border:1px solid #a855f7;color:#d8b4fe;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:12px;white-space:nowrap"
          title="Remove duplicate-title articles">
          🧹 Dedup Titles
        </button>
      </div>
    </div>
    """

    if not articles and page == 1:
        if keyword:
            html += f'<div class="empty-state">No articles matching <strong>{q_safe}</strong></div>'
        else:
            html += '<div class="empty-state">No news articles yet — run main.py to ingest</div>'
        return HTMLResponse(html)

    html += '<div style="padding:8px 12px">'
    for a in articles:
        pub = a["published_at"].strftime("%Y-%m-%d %H:%M") if a["published_at"] else "—"
        title = (a["title"] or "Untitled").replace("<", "&lt;").replace(">", "&gt;")
        summary = (a.get("article_summary") or "").replace("<", "&lt;").replace(">", "&gt;")
        preview_src = summary or (a["full_text"] or "")
        preview = preview_src[:280].replace("<", "&lt;").replace(">", "&gt;")
        if len(preview_src) > 280:
            preview += "…"

        score = a.get("sentiment_score")
        if score is None:
            score_chip = '<span style="background:#1e293b;color:#64748b;border:1px solid #334155;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600">⏳ unscored</span>'
            border_col = "#1e2535"
        else:
            sc = float(score)
            if sc > 0.05:
                col, bg, icon = "#4ade80", "#052e16", "▲"
                border_col = "#166534"
            elif sc < -0.05:
                col, bg, icon = "#f87171", "#450a0a", "▼"
                border_col = "#7f1d1d"
            else:
                col, bg, icon = "#fbbf24", "#451a03", "■"
                border_col = "#78350f"
            score_chip = f'<span style="background:{bg};color:{col};border:1px solid {col}55;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700">{icon} {sc:+.3f}</span>'

        feed_url = a.get("feed_url") or ""
        source = a.get("source_name") or (feed_url.split("/")[2] if "://" in feed_url else feed_url) or "unknown"
        source_short = source[:32] + ("…" if len(source) > 32 else "")

        html += f"""
        <div style="background:#0f172a;border:1px solid {border_col};border-left:3px solid {border_col};border-radius:8px;padding:12px 14px;margin-bottom:10px">
          <div style="display:flex;gap:10px;align-items:flex-start;margin-bottom:6px">
            <div style="flex:1;min-width:0">
              <a href="{a['url']}" target="_blank" style="color:#e2e8f0;font-weight:600;font-size:14px;line-height:1.35;text-decoration:none;display:block">
                {title}
              </a>
            </div>
            {score_chip}
          </div>
          <div style="display:flex;gap:10px;align-items:center;font-size:11px;color:#64748b;margin-bottom:{8 if preview else 0}px">
            <span>🕒 {pub}</span>
            <span style="color:#334155">·</span>
            <span title="{feed_url}">📡 {source_short}</span>
          </div>
          {'<div style="color:#94a3b8;font-size:12.5px;line-height:1.5">' + preview + '</div>' if preview else ''}
        </div>"""
    html += '</div>'

    # ── Pagination ────────────────────────────────────────────────────────────
    total_pages = max(1, (total + per_page - 1) // per_page)
    q_param = f"&q={q_safe}" if keyword else ""
    if total_pages > 1:
        html += '<div class="pagination">'
        if page > 1:
            html += f'<button class="btn btn-ghost" hx-get="/symbol/{sym_id}/news?page={page-1}{q_param}" hx-target="#tab-content-{sym_id}" hx-swap="innerHTML">← Prev</button>'
        html += f'<span style="color:#475569;font-size:.8rem;padding:5px 10px">Page {page} / {total_pages} &nbsp;·&nbsp; {total} unique titles</span>'
        if page < total_pages:
            html += f'<button class="btn btn-ghost" hx-get="/symbol/{sym_id}/news?page={page+1}{q_param}" hx-target="#tab-content-{sym_id}" hx-swap="innerHTML">Next →</button>'
        html += '</div>'

    return HTMLResponse(html)


@app.post("/symbol/{sym_id}/dedup-titles", response_class=HTMLResponse)
async def symbol_dedup_titles(sym_id: int):
    """Delete duplicate-title articles for a symbol. Keeps scored > unscored, then newest."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH ranked AS (
                    SELECT id,
                        ROW_NUMBER() OVER (
                            PARTITION BY LOWER(TRIM(COALESCE(title, '')))
                            ORDER BY
                                (sentiment_score IS NOT NULL) DESC,
                                published_at DESC NULLS LAST,
                                id ASC
                        ) AS rn
                    FROM news_articles
                    WHERE symbol_id = %s
                )
                DELETE FROM news_articles
                WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
            """, (sym_id,))
            deleted = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    # Reload news view
    resp = await symbol_news(sym_id=sym_id, page=1, q="")
    return resp


# ── SEC Filings route ─────────────────────────────────────────────────────────

@app.get("/symbol/{sym_id}/sec", response_class=HTMLResponse)
async def symbol_sec(sym_id: int, page: int = 1, q: str = ""):
    per_page = 20
    offset   = (page - 1) * per_page
    keyword  = q.strip()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if keyword:
                cur.execute("""
                    SELECT COUNT(*) AS total FROM news_articles
                    WHERE symbol_id = %s AND source_name = 'edgar_8k'
                      AND (title ILIKE %s OR full_text ILIKE %s)
                """, (sym_id, f"%{keyword}%", f"%{keyword}%"))
            else:
                cur.execute("""
                    SELECT COUNT(*) AS total FROM news_articles
                    WHERE symbol_id = %s AND source_name = 'edgar_8k'
                """, (sym_id,))
            total = cur.fetchone()["total"]

            if keyword:
                cur.execute("""
                    SELECT id, title, url, published_at, inserted_at, full_text
                    FROM news_articles
                    WHERE symbol_id = %s AND source_name = 'edgar_8k'
                      AND (title ILIKE %s OR full_text ILIKE %s)
                    ORDER BY published_at DESC NULLS LAST
                    LIMIT %s OFFSET %s
                """, (sym_id, f"%{keyword}%", f"%{keyword}%", per_page, offset))
            else:
                cur.execute("""
                    SELECT id, title, url, published_at, inserted_at, full_text
                    FROM news_articles
                    WHERE symbol_id = %s AND source_name = 'edgar_8k'
                    ORDER BY published_at DESC NULLS LAST
                    LIMIT %s OFFSET %s
                """, (sym_id, per_page, offset))
            filings = cur.fetchall()
    finally:
        conn.close()

    q_safe = keyword.replace('"', '&quot;')
    html = f"""
    <div style="padding:12px 16px 0;display:flex;gap:8px;align-items:center">
      <input type="text" id="sec-search-{sym_id}" placeholder="Search SEC filings (merger, acquisition, CEO…)"
        value="{q_safe}"
        hx-get="/symbol/{sym_id}/sec"
        hx-trigger="keyup changed delay:300ms"
        hx-target="#tab-content-{sym_id}"
        hx-include="#sec-search-{sym_id}"
        name="q"
        autocomplete="off"
        style="flex:1"
      />
      <button class="btn btn-danger"
        onclick="if(!confirm('Delete ALL {total} SEC filings for this symbol? Cannot be undone.')) return false;"
        hx-delete="/symbol/{sym_id}/sec/all"
        hx-target="#tab-content-{sym_id}"
        hx-swap="innerHTML"
        style="white-space:nowrap">🗑 Delete All ({total})</button>
    </div>
    """

    if not filings and page == 1:
        if keyword:
            html += f'<div class="empty-state">No SEC filings matching <strong>{q_safe}</strong></div>'
        else:
            html += '<div class="empty-state">No SEC filings yet — enable EDGAR pipeline in pipeline_config.py</div>'
        return HTMLResponse(html)

    for a in filings:
        pub = a["published_at"].strftime("%Y-%m-%d %H:%M") if a["published_at"] else "Unknown date"
        preview = ""
        if a["full_text"]:
            preview = a["full_text"][:300].replace("<", "&lt;").replace(">", "&gt;")
            if len(a["full_text"]) > 300:
                preview += "…"
        html += f"""
        <div class="news-card">
          <a class="news-title" href="{a['url']}" target="_blank">{a['title'] or 'Untitled 8-K'}</a>
          <div class="news-meta">
            <span class="news-date">📅 {pub}</span>
            <span class="chip chip-blue">SEC 8-K</span>
          </div>
          {'<div class="news-preview">' + preview + '</div>' if preview else ''}
        </div>"""

    total_pages = max(1, (total + per_page - 1) // per_page)
    q_param = f"&q={q_safe}" if keyword else ""
    if total_pages > 1:
        html += '<div class="pagination">'
        if page > 1:
            html += f'<button class="btn btn-ghost" hx-get="/symbol/{sym_id}/sec?page={page-1}{q_param}" hx-target="#tab-content-{sym_id}" hx-swap="innerHTML">← Prev</button>'
        html += f'<span style="color:#475569;font-size:.8rem;padding:5px 10px">Page {page} / {total_pages} &nbsp;·&nbsp; {total} filings</span>'
        if page < total_pages:
            html += f'<button class="btn btn-ghost" hx-get="/symbol/{sym_id}/sec?page={page+1}{q_param}" hx-target="#tab-content-{sym_id}" hx-swap="innerHTML">Next →</button>'
        html += '</div>'

    return HTMLResponse(html)


# ── Delete all SEC filings for a symbol ───────────────────────────────────────

@app.delete("/symbol/{sym_id}/sec/all", response_class=HTMLResponse)
async def delete_all_sec(sym_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM news_articles WHERE symbol_id = %s AND source_name = 'edgar_8k'",
                (sym_id,)
            )
            deleted = cur.rowcount
            conn.commit()
    finally:
        conn.close()
    return HTMLResponse(f"""
    <div style="padding:40px 16px;text-align:center;color:#4ade80;font-size:0.9rem">
      🗑 Deleted {deleted} SEC filings.
    </div>
    """)


# ── Global delete ALL SEC filings ─────────────────────────────────────────────

@app.delete("/admin/sec/all", response_class=HTMLResponse)
async def delete_all_sec_global():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM news_articles WHERE source_name = 'edgar_8k'")
            deleted = cur.rowcount
            conn.commit()
    finally:
        conn.close()
    color = "#4ade80" if deleted > 0 else "#94a3b8"
    return HTMLResponse(f"""
    <div style="padding:12px 16px;background:#1e1e2e;border-radius:8px;border:1px solid #333;font-family:monospace;font-size:13px">
      <div style="color:#888;margin-bottom:4px">Global SEC delete complete</div>
      <div>Deleted: <b style="color:{color}">{deleted:,}</b> SEC filings across all symbols</div>
    </div>""")


@app.get("/symbol/{sym_id}/debug", response_class=HTMLResponse)
async def symbol_debug(sym_id: int, page: int = 1, art_id: int = 0):
    """Show full LLM input/output for scored articles. art_id=0 → list view."""
    per_page = 15
    offset   = (page - 1) * per_page
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # If specific article requested, show full detail
            if art_id:
                cur.execute("""
                    SELECT id, title, url, published_at, source_name,
                           sentiment_score, weighted_sentiment,
                           article_summary, score_rationale, forecast_until_earnings,
                           stage2_prompt, pre_summary_data, key_events,
                           master_summary_snapshot, full_text
                    FROM news_articles WHERE id = %s AND symbol_id = %s
                """, (art_id, sym_id))
                art = cur.fetchone()
                if not art:
                    return HTMLResponse('<div class="empty-state">Article not found.</div>')

                def esc(s):
                    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

                score = art["sentiment_score"]
                score_color = "#4ade80" if (score or 0) > 0.1 else "#f87171" if (score or 0) < -0.1 else "#fbbf24"
                pub = art["published_at"].strftime("%Y-%m-%d %H:%M") if art["published_at"] else "?"

                html = f"""
                <div style="padding:14px 16px">
                  <button class="btn btn-ghost" style="margin-bottom:12px"
                    hx-get="/symbol/{sym_id}/debug?page={page}"
                    hx-target="#tab-content-{sym_id}" hx-swap="innerHTML">← Back to list</button>

                  <div style="font-weight:700;font-size:1rem;color:#e2e8f0;margin-bottom:4px">{esc(art['title'])}</div>
                  <div style="font-size:0.72rem;color:#475569;margin-bottom:12px">📅 {pub} &nbsp;·&nbsp; <a href="{art['url'] or '#'}" target="_blank" style="color:#60a5fa">source ↗</a></div>

                  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:16px">
                    <div style="background:#161b27;border:1px solid #1e2535;border-radius:8px;padding:12px">
                      <div style="font-size:0.65rem;color:#475569;font-weight:700;text-transform:uppercase;letter-spacing:.5px">Sentiment Score</div>
                      <div style="font-size:1.6rem;font-weight:800;color:{score_color}">{f"{score:+.3f}" if score is not None else "unscored"}</div>
                    </div>
                    <div style="background:#161b27;border:1px solid #1e2535;border-radius:8px;padding:12px">
                      <div style="font-size:0.65rem;color:#475569;font-weight:700;text-transform:uppercase;letter-spacing:.5px">Weighted (decay)</div>
                      <div style="font-size:1.6rem;font-weight:800;color:#94a3b8">{f"{art['weighted_sentiment']:+.3f}" if art['weighted_sentiment'] is not None else "—"}</div>
                    </div>
                    <div style="background:#161b27;border:1px solid #1e2535;border-radius:8px;padding:12px">
                      <div style="font-size:0.65rem;color:#475569;font-weight:700;text-transform:uppercase;letter-spacing:.5px">Source</div>
                      <div style="font-size:0.9rem;font-weight:700;color:#60a5fa">{esc(art['source_name'] or '?')}</div>
                    </div>
                  </div>

                  <div style="margin-bottom:12px">
                    <div style="font-size:0.72rem;color:#60a5fa;font-weight:700;margin-bottom:4px">SUMMARY</div>
                    <div style="background:#161b27;border:1px solid #1e2535;border-radius:8px;padding:12px;font-size:0.82rem;color:#e2e8f0;line-height:1.6">{esc(art['article_summary'] or '—')}</div>
                  </div>

                  <div style="margin-bottom:12px">
                    <div style="font-size:0.72rem;color:#f59e0b;font-weight:700;margin-bottom:4px">SCORE RATIONALE</div>
                    <div style="background:#161b27;border:1px solid #1e2535;border-radius:8px;padding:12px;font-size:0.82rem;color:#e2e8f0;line-height:1.6">{esc(art['score_rationale'] or '—')}</div>
                  </div>

                  <div style="margin-bottom:12px">
                    <div style="font-size:0.72rem;color:#a78bfa;font-weight:700;margin-bottom:4px">FORECAST UNTIL EARNINGS</div>
                    <div style="background:#161b27;border:1px solid #1e2535;border-radius:8px;padding:12px;font-size:0.82rem;color:#e2e8f0;line-height:1.6">{esc(art['forecast_until_earnings'] or '—')}</div>
                  </div>

                  <div style="margin-bottom:12px">
                    <div style="font-size:0.72rem;color:#34d399;font-weight:700;margin-bottom:4px">KEY EVENTS (JSON)</div>
                    <pre style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px;font-size:0.75rem;color:#94a3b8;white-space:pre-wrap;word-break:break-all;max-height:200px;overflow-y:auto">{esc(json.dumps(art['key_events'], indent=2, ensure_ascii=False) if art['key_events'] else '{}')}</pre>
                  </div>

                  <div style="margin-bottom:12px">
                    <div style="font-size:0.72rem;color:#38bdf8;font-weight:700;margin-bottom:4px">STAGE 1 OUTPUT (pre_summary_data)</div>
                    <pre style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px;font-size:0.75rem;color:#94a3b8;white-space:pre-wrap;word-break:break-all;max-height:200px;overflow-y:auto">{esc(json.dumps(art['pre_summary_data'], indent=2, ensure_ascii=False) if art['pre_summary_data'] else 'Stage 1 not run / no data')}</pre>
                  </div>

                  <div style="margin-bottom:12px">
                    <div style="font-size:0.72rem;color:#fb923c;font-weight:700;margin-bottom:4px">MASTER SUMMARY SNAPSHOT (at time of scoring)</div>
                    <div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px;font-size:0.78rem;color:#94a3b8;white-space:pre-wrap;max-height:150px;overflow-y:auto">{esc(art['master_summary_snapshot'] or '—')}</div>
                  </div>

                  <div style="margin-bottom:12px">
                    <div style="font-size:0.72rem;color:#f87171;font-weight:700;margin-bottom:4px">FULL STAGE 2 PROMPT (sent to LLM)</div>
                    <pre style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px;font-size:0.72rem;color:#64748b;white-space:pre-wrap;word-break:break-all;max-height:400px;overflow-y:auto">{esc(art['stage2_prompt'] or 'Not saved — scored before v3.0.4')}</pre>
                  </div>

                  <div style="margin-bottom:12px">
                    <div style="font-size:0.72rem;color:#475569;font-weight:700;margin-bottom:4px">RAW ARTICLE TEXT (full_text)</div>
                    <div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px;font-size:0.72rem;color:#64748b;white-space:pre-wrap;max-height:300px;overflow-y:auto">{esc((art['full_text'] or 'No full text scraped — title-only article'))}</div>
                  </div>
                </div>
                """
                return HTMLResponse(html)

            # List view: articles with scoring info
            cur.execute("""
                SELECT COUNT(*) AS total FROM news_articles
                WHERE symbol_id = %s
            """, (sym_id,))
            total = cur.fetchone()["total"]

            cur.execute("""
                SELECT id, title, published_at, source_name,
                       sentiment_score, weighted_sentiment,
                       article_summary, score_rationale,
                       CASE WHEN stage2_prompt IS NOT NULL AND stage2_prompt != '' THEN TRUE ELSE FALSE END as has_prompt,
                       CASE WHEN pre_summary_data IS NOT NULL THEN TRUE ELSE FALSE END as has_stage1
                FROM news_articles
                WHERE symbol_id = %s
                ORDER BY published_at DESC NULLS LAST
                LIMIT %s OFFSET %s
            """, (sym_id, per_page, offset))
            articles = cur.fetchall()
    finally:
        conn.close()

    html = f'<div style="padding:10px 16px 0;font-size:0.72rem;color:#475569">{total} total articles</div>'

    if not articles:
        html += '<div class="empty-state">No articles yet.</div>'
        return HTMLResponse(html)

    for a in articles:
        score = a["sentiment_score"]
        score_str = f"{score:+.3f}" if score is not None else "unscored"
        score_color = "#4ade80" if (score or 0) > 0.1 else "#f87171" if (score or 0) < -0.1 else "#fbbf24" if score is not None else "#475569"
        pub = a["published_at"].strftime("%Y-%m-%d %H:%M") if a["published_at"] else "?"
        stage1_chip = '<span class="chip chip-green" title="Stage1 ran">S1✓</span>' if a["has_stage1"] else '<span class="chip chip-gray" title="No Stage1">S1✗</span>'
        prompt_chip = '<span class="chip chip-blue" title="Prompt saved">PROMPT✓</span>' if a["has_prompt"] else '<span class="chip chip-gray" title="Prompt not saved">PROMPT✗</span>'
        rationale = (a["score_rationale"] or "")[:120]

        html += f"""
        <div class="news-card" style="cursor:pointer"
          hx-get="/symbol/{sym_id}/debug?art_id={a['id']}&page={page}"
          hx-target="#tab-content-{sym_id}" hx-swap="innerHTML">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px">
            <div class="news-title" style="flex:1">{(a['title'] or 'Untitled')[:120]}</div>
            <div style="font-size:1.1rem;font-weight:800;color:{score_color};white-space:nowrap">{score_str}</div>
          </div>
          <div class="news-meta">
            <span class="news-date">📅 {pub}</span>
            <span class="chip chip-gray">{a['source_name'] or '?'}</span>
            {stage1_chip}{prompt_chip}
          </div>
          {'<div class="news-preview">' + rationale + ('…' if len(a["score_rationale"] or '') > 120 else '') + '</div>' if rationale else ''}
        </div>"""

    total_pages = max(1, (total + per_page - 1) // per_page)
    if total_pages > 1:
        html += '<div class="pagination">'
        if page > 1:
            html += f'<button class="btn btn-ghost" hx-get="/symbol/{sym_id}/debug?page={page-1}" hx-target="#tab-content-{sym_id}" hx-swap="innerHTML">← Prev</button>'
        html += f'<span style="color:#475569;font-size:.8rem;padding:5px 10px">Page {page} / {total_pages}</span>'
        if page < total_pages:
            html += f'<button class="btn btn-ghost" hx-get="/symbol/{sym_id}/debug?page={page+1}" hx-target="#tab-content-{sym_id}" hx-swap="innerHTML">Next →</button>'
        html += '</div>'

    return HTMLResponse(html)



def _feed_card_html(f) -> str:
    active_chip = '<span class="chip chip-green">ACTIVE</span>' if f['is_active'] else '<span class="chip chip-red">INACTIVE</span>'
    source_chip = f'<span class="chip chip-blue">{f["source"]}</span>'
    type_chip   = f'<span class="chip chip-gray">{f["feed_type"]}</span>'
    toggle_label = "Deactivate" if f['is_active'] else "Activate"
    toggle_class = "btn btn-warn" if f['is_active'] else "btn btn-primary"
    fid = f['id']
    return f"""
    <div class="feed-card" id="feed-{fid}">
      <a class="feed-url" href="{f['feed_url']}" target="_blank">{f['feed_url']}</a>
      <div class="feed-meta">{active_chip}{source_chip}{type_chip}</div>
      <div class="feed-actions">
        <button class="{toggle_class}" hx-patch="/feeds/{fid}/toggle"
          hx-target="#feed-{fid}" hx-swap="outerHTML">{toggle_label}</button>
        <button class="btn btn-ghost" onclick="toggleEdit({fid})">Edit URL</button>
        <button class="btn btn-danger"
          hx-delete="/feeds/{fid}"
          hx-confirm="Delete this feed permanently?"
          hx-target="#feed-{fid}" hx-swap="outerHTML">Delete</button>
      </div>
      <div id="edit-{fid}" style="display:none">
        <div class="edit-form">
          <input type="url" id="edit-url-{fid}" value="{f['feed_url']}"
            placeholder="New feed URL" style="flex:1"/>
          <button class="btn btn-primary" onclick="saveEdit({fid})">Save</button>
          <button class="btn btn-ghost"  onclick="toggleEdit({fid})">Cancel</button>
        </div>
        <div id="edit-val-{fid}"></div>
      </div>
    </div>"""


# ── Symbol Intelligence tab ───────────────────────────────────────────────────

@app.get("/symbol/{sym_id}/intel", response_class=HTMLResponse)
async def symbol_intel(sym_id: int):
    import json as _json
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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
                       sm.macro_multiplier, sm.rationale AS sector_rationale, sm.sector_name,
                       s.ai_sector_pick, s.ai_sector_multiplier
                FROM symbols s
                LEFT JOIN sectors_macro sm ON sm.id = s.sector_id
                WHERE s.id = %s
            """, (sym_id,))
            sym = cur.fetchone()
            if not sym:
                return HTMLResponse('<div class="empty-state">Symbol not found.</div>')

            # Latest TV snapshot
            cur.execute("""
                SELECT data FROM symbol_daily_snapshots
                WHERE symbol_id = %s ORDER BY snapshot_date DESC LIMIT 1
            """, (sym_id,))
            snap_row = cur.fetchone()
            tv_snap = snap_row["data"] if snap_row else {}

            # Scored articles for connections
            cur.execute("""
                SELECT company_connections FROM news_articles
                WHERE symbol_id = %s AND sentiment_score IS NOT NULL
                  AND company_connections IS NOT NULL
            """, (sym_id,))
            cc_rows = cur.fetchall()
    finally:
        conn.close()

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

    # TradingView grid
    earnings_date = sym["earnings_release_date"].strftime("%Y-%m-%d") if sym.get("earnings_release_date") else "—"
    tv_rows = [
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
    tv_html = "".join(
        f'<div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:10px 12px">'
        f'<div style="font-size:10px;color:#475569;font-weight:700;text-transform:uppercase;margin-bottom:3px">{lbl}</div>'
        f'<div style="font-size:14px;font-weight:700;color:#e2e8f0">{val}</div></div>'
        for lbl, val in tv_rows if val != "—"
    )

    # Company connections
    all_conn = {"competitors": set(), "partners": set(), "suppliers": set()}
    for row in cc_rows:
        cc = row.get("company_connections") or {}
        if isinstance(cc, str):
            try: cc = _json.loads(cc)
            except: cc = {}
        for k in ("competitors", "partners", "suppliers"):
            for item in (cc.get(k) or []):
                if item and str(item).strip():
                    all_conn[k].add(str(item).strip())

    def conn_tags(items, color):
        if not items: return '<span style="color:#475569">None identified</span>'
        return " ".join(f'<span style="background:#1e293b;color:{color};padding:2px 10px;border-radius:12px;font-size:12px;font-weight:600;display:inline-block;margin:2px">{i}</span>' for i in sorted(items))

    forecast = (sym.get("symbol_forecast_narrative") or "").replace("<","&lt;")
    master   = (sym.get("symbol_master_summary") or "").replace("<","&lt;")
    sector_rat = (sym.get("sector_rationale") or "").replace("<","&lt;")
    industry = sym.get("industry") or "—"
    sector   = sym.get("sector_name") or "—"
    ai_sector_pick = sym.get("ai_sector_pick") or "—"
    ai_sector_mult = float(sym.get("ai_sector_multiplier") or 1.0)
    ai_sector_boost = fmt(round((ai_sector_mult - 1) * 100, 2))

    _score_col = "#4ade80" if (sym.get("final_score") or 0) > 0.05 else "#f87171" if (sym.get("final_score") or 0) < -0.05 else "#fbbf24"
    _macro_mult = float(sym.get("macro_multiplier") or 1.0)
    _base_avg  = fmt(round(sym["final_score"] / (_macro_mult * ai_sector_mult), 6) if sym.get("final_score") is not None and _macro_mult != 0 and ai_sector_mult != 0 else sym.get("final_score"))
    _mult_val  = fmt(sym.get("macro_multiplier"))
    _mult_boost = fmt(round((_macro_mult-1)*100, 2)) if sym.get("macro_multiplier") else "0.00"
    _final_str = f'{sym["final_score"]:+.4f}' if sym.get("final_score") is not None else "—"
    _score_breakdown = f'''<div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px;margin-bottom:16px">
        <div style="font-size:10px;color:#475569;font-weight:700;text-transform:uppercase;margin-bottom:8px">📐 Score Breakdown</div>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px">
          <div style="background:#161b27;border:1px solid #1e2535;border-radius:6px;padding:10px;text-align:center">
            <div style="font-size:10px;color:#475569;font-weight:700;text-transform:uppercase;margin-bottom:4px">Base Avg (decayed)</div>
            <div style="font-size:1.4rem;font-weight:800;color:#fcd34d">{_base_avg}</div>
          </div>
          <div style="background:#161b27;border:1px solid #1e2535;border-radius:6px;padding:10px;text-align:center">
            <div style="font-size:10px;color:#475569;font-weight:700;text-transform:uppercase;margin-bottom:4px">Sector Mult ×</div>
            <div style="font-size:1.4rem;font-weight:800;color:#34d399">{_mult_val}</div>
            <div style="font-size:10px;color:#475569">+{_mult_boost}% boost</div>
          </div>
          <div style="background:#161b27;border:1px solid #1e2535;border-radius:6px;padding:10px;text-align:center">
            <div style="font-size:10px;color:#475569;font-weight:700;text-transform:uppercase;margin-bottom:4px">AI Sector Mult ×</div>
            <div style="font-size:1.4rem;font-weight:800;color:#a78bfa">{fmt(ai_sector_mult)}</div>
            <div style="font-size:10px;color:#475569">+{ai_sector_boost}% boost</div>
            <div style="font-size:10px;color:#64748b;margin-top:4px">{ai_sector_pick}</div>
          </div>
          <div style="background:#161b27;border:1px solid #1e2535;border-radius:6px;padding:10px;text-align:center">
            <div style="font-size:10px;color:#475569;font-weight:700;text-transform:uppercase;margin-bottom:4px">Final Score</div>
            <div style="font-size:1.4rem;font-weight:800;color:{_score_col}">{_final_str}</div>
          </div>
        </div>
      </div>''' if sym.get("macro_multiplier") and sym.get("final_score") is not None else ""

    html = f"""
    <div style="padding:16px">

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">
        <div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px">
          <div style="font-size:10px;color:#475569;font-weight:700;text-transform:uppercase;margin-bottom:4px">Industry</div>
          <div style="font-size:14px;color:#60a5fa;font-weight:700">{industry}</div>
        </div>
        <div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px">
          <div style="font-size:10px;color:#475569;font-weight:700;text-transform:uppercase;margin-bottom:4px">Sector</div>
          <div style="font-size:14px;color:#60a5fa;font-weight:700">{sector}</div>
        </div>
        <div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px">
          <div style="font-size:10px;color:#475569;font-weight:700;text-transform:uppercase;margin-bottom:4px">🤖 AI Sector Pick</div>
          <div style="font-size:13px;color:#a78bfa;font-weight:700">{ai_sector_pick}</div>
        </div>
      </div>

      <!-- Score breakdown -->
      {_score_breakdown}

      <div style="margin-bottom:16px;text-align:right">
        <button
          onclick="if(!confirm('Wipe all sentiment for {sym["symbol"]} and queue as next? Cannot be undone.')) return false;"
          hx-post="/symbol/{sym_id}/rescore"
          hx-target="#feed-panel"
          hx-swap="innerHTML"
          style="background:#1e1b3b;border:1px solid #a855f7;color:#d8b4fe;padding:8px 16px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600">
          🔄 Redo Sentiment Scoring
        </button>
      </div>


      <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#475569;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid #1e2535">📊 TradingView Screener Data</div>
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:8px;margin-bottom:16px">
        {tv_html}
      </div>

      {"<div style='font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#475569;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid #1e2535'>🔮 Symbol Forecast</div><div style='background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px;font-size:13px;color:#cbd5e1;line-height:1.7;white-space:pre-wrap;margin-bottom:16px'>" + forecast + "</div>" if forecast else ""}

      {"<div style='font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#475569;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid #1e2535'>📋 Master Summary</div><div style='background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px;font-size:13px;color:#cbd5e1;line-height:1.7;white-space:pre-wrap;margin-bottom:16px'>" + master + "</div>" if master else ""}

      <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#475569;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid #1e2535">🔗 Company Connections</div>
      <div style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:14px;margin-bottom:16px">
        <div style="margin-bottom:12px">
          <div style="font-size:11px;font-weight:700;color:#f87171;margin-bottom:6px">⚔ Competitors</div>
          {conn_tags(all_conn['competitors'], '#f87171')}
        </div>
        <div style="margin-bottom:12px">
          <div style="font-size:11px;font-weight:700;color:#4ade80;margin-bottom:6px">🤝 Partners / Customers</div>
          {conn_tags(all_conn['partners'], '#4ade80')}
        </div>
        <div>
          <div style="font-size:11px;font-weight:700;color:#fbbf24;margin-bottom:6px">📦 Suppliers</div>
          {conn_tags(all_conn['suppliers'], '#fbbf24')}
        </div>
      </div>

      {"<div style='font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:#475569;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid #1e2535'>🌐 Sector Macro Context</div><div style='background:#0f172a;border:1px solid #1e2535;border-radius:8px;padding:12px;font-size:13px;color:#94a3b8;line-height:1.6;margin-bottom:16px'>" + sector_rat + "</div>" if sector_rat else ""}

    </div>"""
    return HTMLResponse(html)


# ── Global dedup route ────────────────────────────────────────────────────────

@app.post("/admin/dedup", response_class=HTMLResponse)
async def run_dedup():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT COUNT(*) as cnt FROM (
                    SELECT symbol_id, url FROM news_articles
                    GROUP BY symbol_id, url HAVING COUNT(*) > 1
                ) sub
            """)
            dupe_groups = cur.fetchone()["cnt"]

        with conn.cursor() as cur2:
            cur2.execute("""
                DELETE FROM news_articles a
                USING (
                    SELECT MIN(id) as keep_id, symbol_id, url
                    FROM news_articles
                    GROUP BY symbol_id, url
                ) b
                WHERE a.symbol_id = b.symbol_id
                  AND a.url = b.url
                  AND a.id <> b.keep_id
            """)
            deleted = cur2.rowcount
            conn.commit()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) as total FROM news_articles")
            total = cur.fetchone()["total"]
    finally:
        conn.close()

    color = "#22c55e" if deleted == 0 else "#f59e0b"
    return f"""
    <div style="padding:16px;background:#1e1e2e;border-radius:8px;border:1px solid #333;font-family:monospace;font-size:13px">
      <div style="color:#888;margin-bottom:8px">Dedup complete</div>
      <div>Duplicate URL groups found: <b style="color:{color}">{dupe_groups}</b></div>
      <div>Rows deleted: <b style="color:{color}">{deleted}</b></div>
      <div>Total articles remaining: <b style="color:#60a5fa">{total:,}</b></div>
      <div style="color:#888;margin-top:8px;font-size:11px">UNIQUE(symbol_id, url) prevents future dupes automatically.</div>
    </div>"""


# ── Global dedup-titles route ─────────────────────────────────────────────────
@app.post("/admin/dedup-titles", response_class=HTMLResponse)
async def run_dedup_titles():
    """Delete duplicate-title articles globally. Per (symbol_id, normalized title)
    keep scored > unscored, then newest, then lowest id."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT COUNT(*) AS cnt FROM (
                    SELECT symbol_id, LOWER(TRIM(COALESCE(title, ''))) AS t
                    FROM news_articles
                    WHERE title IS NOT NULL AND TRIM(title) != ''
                    GROUP BY symbol_id, t HAVING COUNT(*) > 1
                ) sub
            """)
            dupe_groups = cur.fetchone()["cnt"]

        with conn.cursor() as cur2:
            cur2.execute("""
                WITH ranked AS (
                    SELECT id,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol_id, LOWER(TRIM(COALESCE(title, '')))
                            ORDER BY
                                (sentiment_score IS NOT NULL) DESC,
                                published_at DESC NULLS LAST,
                                id ASC
                        ) AS rn
                    FROM news_articles
                    WHERE title IS NOT NULL AND TRIM(title) != ''
                )
                DELETE FROM news_articles
                WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
            """)
            deleted = cur2.rowcount
            conn.commit()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS total FROM news_articles")
            total = cur.fetchone()["total"]
    finally:
        conn.close()

    color = "#22c55e" if deleted == 0 else "#a855f7"
    return f"""
    <div style="padding:16px;background:#1e1e2e;border-radius:8px;border:1px solid #333;font-family:monospace;font-size:13px">
      <div style="color:#888;margin-bottom:8px">Title dedup complete</div>
      <div>Duplicate title groups: <b style="color:{color}">{dupe_groups}</b></div>
      <div>Rows deleted: <b style="color:{color}">{deleted}</b></div>
      <div>Total articles remaining: <b style="color:#60a5fa">{total:,}</b></div>
      <div style="color:#888;margin-top:8px;font-size:11px">Kept scored > unscored, then newest. Per symbol.</div>
    </div>"""


@app.post("/admin/dedup-lang", response_class=HTMLResponse)
async def run_dedup_lang():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Find groups with >1 article at same (symbol, ~time, source)
            cur.execute("""
                SELECT
                    symbol_id,
                    DATE_TRUNC('hour', published_at) AS hour_bucket,
                    EXTRACT(MINUTE FROM published_at)::int / 5 AS min_bucket,
                    source_name,
                    COUNT(*) as cnt,
                    ARRAY_AGG(id ORDER BY
                        CASE WHEN url ~ '/0/en/' THEN 0 ELSE 1 END,
                        COALESCE(sentiment_score, -99) DESC,
                        id ASC
                    ) AS ids
                FROM news_articles
                WHERE published_at IS NOT NULL
                GROUP BY symbol_id, hour_bucket, min_bucket, source_name
                HAVING COUNT(*) > 1
            """)
            groups = cur.fetchall()

        total_groups = len(groups)
        ids_to_delete = []
        for g in groups:
            # First id in the ordered array is the keeper
            keep = g["ids"][0]
            ids_to_delete.extend(g["ids"][1:])

        deleted = 0
        if ids_to_delete:
            with conn.cursor() as cur2:
                cur2.execute(
                    "DELETE FROM news_articles WHERE id = ANY(%s)",
                    (ids_to_delete,)
                )
                deleted = cur2.rowcount
                conn.commit()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) as total FROM news_articles")
            total = cur.fetchone()["total"]
    finally:
        conn.close()

    color = "#22c55e" if deleted == 0 else "#a855f7"
    return f"""
    <div style="padding:16px;background:#1e1e2e;border-radius:8px;border:1px solid #333;font-family:monospace;font-size:13px">
      <div style="color:#888;margin-bottom:8px">Language dedup complete</div>
      <div>Cross-language groups found: <b style="color:{color}">{total_groups}</b></div>
      <div>Rows deleted: <b style="color:{color}">{deleted}</b></div>
      <div>Total articles remaining: <b style="color:#60a5fa">{total:,}</b></div>
      <div style="color:#888;margin-top:8px;font-size:11px">Kept English (/0/en/) or highest-scored per group.</div>
    </div>"""




@app.get("/market-research", response_class=HTMLResponse)
def market_research_panel(request: Request):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT f.id, f.feed_url, f.source_name, f.description,
                       f.is_active, f.last_checked_at,
                       COUNT(a.id) as article_count
                FROM market_research_feeds f
                LEFT JOIN market_research_articles a ON a.feed_id = f.id
                GROUP BY f.id
                ORDER BY f.source_name, f.id
            """)
            feeds = cur.fetchall()
            cur.execute("SELECT COUNT(*) as total FROM market_research_articles")
            total_articles = cur.fetchone()["total"]
            cur.execute("SELECT COUNT(*) as pending FROM market_research_articles WHERE llm_processed = FALSE")
            pending_llm = cur.fetchone()["pending"]
    finally:
        conn.close()

    rows = ""
    for f in feeds:
        checked = f["last_checked_at"].strftime("%Y-%m-%d %H:%M") if f["last_checked_at"] else "never"
        active_chip = (
            '<span style="background:#166534;color:#4ade80;padding:2px 8px;border-radius:4px;font-size:11px">active</span>'
            if f["is_active"] else
            '<span style="background:#3b0764;color:#c084fc;padding:2px 8px;border-radius:4px;font-size:11px">paused</span>'
        )
        count_color = "#4ade80" if f["article_count"] > 0 else "#ef4444"
        rows += f"""
        <tr id="mr-row-{f['id']}">
          <td style="padding:10px 8px">
            <div style="font-weight:500;color:#e2e8f0;font-size:13px">{f['source_name'] or '—'}</div>
            <div style="color:#64748b;font-size:11px;margin-top:2px">{f['description'] or ''}</div>
          </td>
          <td style="padding:10px 8px;font-size:11px;color:#94a3b8;word-break:break-all;max-width:260px">{f['feed_url']}</td>
          <td style="padding:10px 8px;text-align:center">{active_chip}</td>
          <td style="padding:10px 8px;text-align:center">
            <span style="background:#1e293b;color:{count_color};padding:2px 10px;border-radius:12px;font-size:12px;font-weight:600">{f['article_count']}</span>
          </td>
          <td style="padding:10px 8px;text-align:center;color:#64748b;font-size:11px">{checked}</td>
          <td style="padding:10px 8px;text-align:center">
            <button onclick="deleteMRFeed({f['id']})"
              style="background:#7f1d1d;color:#fca5a5;border:none;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:11px">
              Delete
            </button>
          </td>
        </tr>"""

    return f"""
    <div style="padding:20px">
      <div style="display:flex;align-items:center;gap:16px;margin-bottom:20px">
        <h2 style="margin:0;color:#e2e8f0;font-size:18px">📊 Market Research Feeds</h2>
        <span style="background:#1e293b;color:#60a5fa;padding:3px 10px;border-radius:12px;font-size:12px">{total_articles:,} articles</span>
        <span style="background:#1e293b;color:#f59e0b;padding:3px 10px;border-radius:12px;font-size:12px">{pending_llm:,} pending LLM</span>
        <button hx-post="/market-research/run-llm" hx-target="#mr-llm-result" hx-swap="innerHTML"
          style="background:#1e293b;border:1px solid #6366f1;color:#a5b4fc;padding:5px 12px;border-radius:6px;cursor:pointer;font-size:12px">
          Run LLM Analysis
        </button>
        <div id="mr-llm-result" style="font-size:12px;color:#94a3b8"></div>
      </div>

      <!-- Add new feed form -->
      <div style="background:#1e1e2e;border:1px solid #2d2d3f;border-radius:8px;padding:16px;margin-bottom:20px">
        <div style="font-size:13px;color:#94a3b8;margin-bottom:10px;font-weight:600">Add Market Research Feed</div>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <input id="mr-url" placeholder="RSS/Atom feed URL"
            style="flex:2;min-width:260px;background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:7px 10px;border-radius:6px;font-size:13px"/>
          <input id="mr-source" placeholder="Source name (e.g. SNS Insider)"
            style="flex:1;min-width:160px;background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:7px 10px;border-radius:6px;font-size:13px"/>
          <input id="mr-desc" placeholder="Description (optional)"
            style="flex:2;min-width:200px;background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:7px 10px;border-radius:6px;font-size:13px"/>
          <button onclick="addMRFeed()"
            style="background:#4f46e5;color:#fff;border:none;padding:7px 18px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600">
            Add Feed
          </button>
        </div>
        <div id="mr-add-result" style="margin-top:8px;font-size:12px;color:#94a3b8"></div>
      </div>

      <!-- Feed table -->
      <table style="width:100%;border-collapse:collapse" id="mr-table">
        <thead>
          <tr style="border-bottom:1px solid #1e293b;color:#64748b;font-size:11px;text-transform:uppercase">
            <th style="padding:8px;text-align:left">Source</th>
            <th style="padding:8px;text-align:left">Feed URL</th>
            <th style="padding:8px;text-align:center">Status</th>
            <th style="padding:8px;text-align:center">Articles</th>
            <th style="padding:8px;text-align:center">Last Checked</th>
            <th style="padding:8px;text-align:center">Actions</th>
          </tr>
        </thead>
        <tbody id="mr-tbody">
          {rows if rows else '<tr><td colspan="6" style="text-align:center;padding:40px;color:#64748b">No market research feeds yet. Add one above.</td></tr>'}
        </tbody>
      </table>
    </div>

    <script>
    async function addMRFeed() {{
      const url    = document.getElementById('mr-url').value.trim();
      const source = document.getElementById('mr-source').value.trim();
      const desc   = document.getElementById('mr-desc').value.trim();
      if (!url) {{ document.getElementById('mr-add-result').textContent = 'URL required'; return; }}
      const res = await fetch('/market-research/feeds', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/x-www-form-urlencoded'}},
        body: new URLSearchParams({{feed_url: url, source_name: source, description: desc}})
      }});
      const data = await res.json();
      const el = document.getElementById('mr-add-result');
      if (data.ok) {{
        el.style.color = '#4ade80';
        el.textContent = 'Feed added: ' + data.source_name;
        document.getElementById('mr-url').value = '';
        document.getElementById('mr-source').value = '';
        document.getElementById('mr-desc').value = '';
        htmx.ajax('GET', '/market-research', '#feed-panel');
      }} else {{
        el.style.color = '#ef4444';
        el.textContent = data.error || 'Failed';
      }}
    }}

    async function deleteMRFeed(id) {{
      if (!confirm('Delete this market research feed?')) return;
      const res = await fetch('/market-research/feeds/' + id, {{method: 'DELETE'}});
      const data = await res.json();
      if (data.ok) htmx.ajax('GET', '/market-research', '#feed-panel');
    }}
    </script>
    """


@app.post("/market-research/feeds")
async def add_mr_feed(
    feed_url: str = Form(...),
    source_name: str = Form(""),
    description: str = Form(""),
):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO market_research_feeds (feed_url, source_name, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (feed_url) DO NOTHING
                RETURNING id, source_name
            """, (feed_url.strip(), source_name.strip(), description.strip()))
            row = cur.fetchone()
        conn.commit()
        if row:
            return {"ok": True, "id": row[0], "source_name": row[1] or feed_url}
        return {"ok": False, "error": "Feed URL already exists"}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


@app.delete("/market-research/feeds/{feed_id}")
def delete_mr_feed(feed_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM market_research_feeds WHERE id = %s", (feed_id,))
        conn.commit()
        return {"ok": True}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


@app.post("/market-research/run-llm", response_class=HTMLResponse)
def run_mr_llm():
    """Trigger macro_multiplier pipeline from admin UI."""
    try:
        from pipeline.macro_multiplier import run as mr_run
        result = mr_run(limit=50)   # batch of 50 to avoid long blocking
        return (
            f'<span style="color:#4ade80">Done: {result["processed"]} processed, '
            f'{result["industries_updated"]} industries updated, '
            f'{result["duration_s"]}s</span>'
        )
    except Exception as e:
        return f'<span style="color:#ef4444">Error: {e}</span>'


# ── Market Scores (sectors_macro viewer) ──────────────────────────────────────

@app.get("/market-scores", response_class=HTMLResponse)
def market_scores_panel():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, sector_name, industry_name,
                       macro_multiplier, rationale,
                       last_llm_run_at, updated_at
                FROM sectors_macro
                ORDER BY macro_multiplier DESC, sector_name, industry_name
            """)
            rows = cur.fetchall()
            total = len(rows)
    finally:
        conn.close()

    def score_color(m):
        m = float(m)
        if m >= 1.040: return "#4ade80"   # strong green
        if m >= 1.025: return "#86efac"   # light green
        if m >= 1.010: return "#fbbf24"   # amber
        return "#94a3b8"                   # gray/neutral

    row_html = ""
    for r in rows:
        m = float(r["macro_multiplier"])
        bar_pct = int((m - 1.000) / 0.050 * 100)   # 1.000=0%, 1.050=100%
        bar_pct = max(0, min(100, bar_pct))
        col = score_color(m)
        ran = r["last_llm_run_at"].strftime("%Y-%m-%d %H:%M") if r["last_llm_run_at"] else "—"
        upd = r["updated_at"].strftime("%Y-%m-%d") if r["updated_at"] else "—"
        rationale_safe = (r["rationale"] or "").replace("<", "&lt;").replace(">", "&gt;")
        row_html += f"""
        <tr id="ms-row-{r['id']}" style="border-bottom:1px solid #1e2535">
          <td style="padding:10px 12px">
            <span style="font-size:11px;color:#94a3b8;background:#1e2535;padding:2px 7px;border-radius:4px">{r['sector_name']}</span>
          </td>
          <td style="padding:10px 12px;font-weight:600;color:#e2e8f0;font-size:13px">{r['industry_name']}</td>
          <td style="padding:10px 12px;text-align:center">
            <div style="display:flex;align-items:center;gap:8px">
              <div style="flex:1;height:6px;background:#1e2535;border-radius:3px;overflow:hidden">
                <div style="width:{bar_pct}%;height:100%;background:{col};border-radius:3px"></div>
              </div>
              <span style="font-size:14px;font-weight:700;color:{col};min-width:48px;text-align:right">{m:.3f}×</span>
            </div>
          </td>
          <td style="padding:10px 12px;font-size:12px;color:#94a3b8;max-width:340px">{rationale_safe or '—'}</td>
          <td style="padding:10px 12px;text-align:center;font-size:11px;color:#475569">{ran}</td>
          <td style="padding:10px 12px;text-align:center">
            <button onclick="deleteMSRow({r['id']})"
              style="background:#7f1d1d;color:#fca5a5;border:none;padding:3px 10px;border-radius:4px;cursor:pointer;font-size:11px">
              Delete
            </button>
          </td>
        </tr>"""

    empty = '<tr><td colspan="6" style="text-align:center;padding:40px;color:#64748b">No market scores yet — run LLM Analysis from Market Research panel.</td></tr>'

    return f"""
    <div style="padding:20px">
      <div style="display:flex;align-items:center;gap:16px;margin-bottom:20px;flex-wrap:wrap">
        <h2 style="margin:0;color:#e2e8f0;font-size:18px">📈 Market Scores</h2>
        <span style="background:#1e293b;color:#6ee7b7;padding:3px 10px;border-radius:12px;font-size:12px">{total} industries tracked</span>
        <button onclick="deleteAllMS()"
          style="background:#7f1d1d;color:#fca5a5;border:1px solid #991b1b;padding:5px 14px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;margin-left:auto">
          🗑 Delete All Rankings
        </button>
        <div id="ms-action-result" style="font-size:12px;color:#94a3b8"></div>
      </div>

      <div style="background:#161b27;border:1px solid #1e2535;border-radius:10px;overflow-y:auto;max-height:65vh">
        <table style="width:100%;border-collapse:collapse" id="ms-table">
          <thead>
            <tr style="background:#1a2133;color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:0.5px">
              <th style="padding:10px 12px;text-align:left">Sector</th>
              <th style="padding:10px 12px;text-align:left">Industry</th>
              <th style="padding:10px 12px;text-align:left;min-width:220px">Multiplier</th>
              <th style="padding:10px 12px;text-align:left">LLM Rationale</th>
              <th style="padding:10px 12px;text-align:center">Last LLM Run</th>
              <th style="padding:10px 12px;text-align:center">Actions</th>
            </tr>
          </thead>
          <tbody id="ms-tbody">
            {row_html if row_html else empty}
          </tbody>
        </table>
      </div>

      <div style="margin-top:12px;font-size:11px;color:#475569">
        Multiplier scale: 1.000 = neutral · 1.010 = mild positive · 1.025 = moderate growth · 1.040 = strong · 1.050 = exceptional
        &nbsp;·&nbsp; {total} total industries
      </div>
    </div>

    <script>
    async function deleteMSRow(id) {{
      if (!confirm('Delete this industry ranking?')) return;
      const res = await fetch('/market-scores/' + id, {{method: 'DELETE'}});
      const data = await res.json();
      if (data.ok) {{
        const row = document.getElementById('ms-row-' + id);
        if (row) row.remove();
        document.getElementById('ms-action-result').textContent = 'Row deleted.';
      }} else {{
        document.getElementById('ms-action-result').textContent = data.error || 'Error';
      }}
    }}
    async function deleteAllMS() {{
      if (!confirm('Delete ALL market scores/rankings? This cannot be undone.')) return;
      const res = await fetch('/market-scores/all', {{method: 'DELETE'}});
      const data = await res.json();
      const el = document.getElementById('ms-action-result');
      if (data.ok) {{
        document.getElementById('ms-tbody').innerHTML =
          '<tr><td colspan="6" style="text-align:center;padding:40px;color:#64748b">All rankings deleted.</td></tr>';
        el.style.color = '#4ade80';
        el.textContent = 'Deleted ' + data.deleted + ' rows.';
      }} else {{
        el.style.color = '#ef4444';
        el.textContent = data.error || 'Error';
      }}
    }}
    </script>
    """


@app.delete("/market-scores/all")
def delete_all_market_scores():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sectors_macro")
            deleted = cur.rowcount
        conn.commit()
        return {"ok": True, "deleted": deleted}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


@app.delete("/market-scores/{row_id}")
def delete_market_score(row_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sectors_macro WHERE id = %s", (row_id,))
            deleted = cur.rowcount
        conn.commit()
        if deleted == 0:
            return {"ok": False, "error": "Row not found"}
        return {"ok": True}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


# ── Reset all sentiment scores ────────────────────────────────────────────────

@app.post("/admin/reset-scores", response_class=HTMLResponse)
def reset_scores():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE news_articles SET
                    sentiment_score          = NULL,
                    weighted_sentiment       = NULL,
                    article_summary          = NULL,
                    score_rationale          = NULL,
                    key_events               = NULL,
                    pre_summary_data         = NULL,
                    master_summary_snapshot  = NULL,
                    forecast_until_earnings  = NULL
            """)
            articles_reset = cur.rowcount
            cur.execute("""
                UPDATE symbols SET
                    final_score              = NULL,
                    score_updated_at         = NULL,
                    symbol_master_summary    = NULL
            """)
            symbols_reset = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return f"""
    <div style="background:#0f172a;border:1px solid #ef4444;border-radius:10px;padding:24px;color:#f1f5f9;text-align:center">
      <div style="font-size:2rem;margin-bottom:8px">🗑️</div>
      <div style="font-size:1.1rem;font-weight:700;color:#f87171;margin-bottom:8px">Scores Reset</div>
      <div style="color:#94a3b8;font-size:0.9rem">{articles_reset} articles cleared &nbsp;·&nbsp; {symbols_reset} symbol scores cleared</div>
      <div style="margin-top:16px;color:#64748b;font-size:0.8rem">Ready for a fresh sentiment scoring run.</div>
    </div>
    """


@app.post("/symbol/{sym_id}/rescore", response_class=HTMLResponse)
async def symbol_rescore(sym_id: int):
    """Wipe sentiment for one symbol + queue it as rank #1 (runs next)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Verify symbol exists
            cur.execute("SELECT symbol FROM symbols WHERE id = %s", (sym_id,))
            row = cur.fetchone()
            if not row:
                return HTMLResponse('<div class="empty-state">Symbol not found.</div>')
            sym_label = row[0]

            # Wipe per-article sentiment so pipeline picks them up again
            cur.execute("""
                UPDATE news_articles SET
                    sentiment_score          = NULL,
                    weighted_sentiment       = NULL,
                    article_summary          = NULL,
                    score_rationale          = NULL,
                    key_events               = NULL,
                    pre_summary_data         = NULL,
                    master_summary_snapshot  = NULL,
                    forecast_until_earnings  = NULL,
                    stage2_prompt            = NULL,
                    company_connections      = NULL
                WHERE symbol_id = %s
            """, (sym_id,))
            articles_cleared = cur.rowcount

            # Wipe symbol-level score
            cur.execute("""
                UPDATE symbols SET
                    final_score              = NULL,
                    score_updated_at         = NULL,
                    symbol_master_summary    = NULL,
                    symbol_forecast_narrative= NULL,
                    ai_sector_pick           = NULL,
                    ai_sector_multiplier     = 1.000
                WHERE id = %s
            """, (sym_id,))

            # Push everyone down +1, insert this at rank 1
            cur.execute("UPDATE priority_queue SET rank = rank + 1")
            cur.execute("""
                INSERT INTO priority_queue (symbol_id, rank, added_at)
                VALUES (%s, 1, NOW())
                ON CONFLICT (symbol_id) DO UPDATE SET rank = 1, added_at = NOW()
            """, (sym_id,))
            # Compact ranks (in case of duplicates from ON CONFLICT path)
            cur.execute("""
                WITH ranked AS (
                    SELECT symbol_id, ROW_NUMBER() OVER (ORDER BY rank ASC, added_at ASC) AS new_rank
                    FROM priority_queue
                )
                UPDATE priority_queue pq SET rank = ranked.new_rank
                FROM ranked WHERE pq.symbol_id = ranked.symbol_id
            """)
        conn.commit()
    finally:
        conn.close()

    return HTMLResponse(f"""
    <div style="background:#0f172a;border:1px solid #a855f7;border-radius:10px;padding:24px;color:#f1f5f9;text-align:center">
      <div style="font-size:2rem;margin-bottom:8px">🔄</div>
      <div style="font-size:1.1rem;font-weight:700;color:#d8b4fe;margin-bottom:8px">Rescore Queued: {sym_label}</div>
      <div style="color:#94a3b8;font-size:0.9rem">{articles_cleared} articles cleared · queued as priority #1</div>
      <div style="margin-top:16px;color:#64748b;font-size:0.8rem">Will run next on the active/next sentiment scoring pass.</div>
      <button
        hx-get="/symbol/{sym_id}/intel"
        hx-target="#feed-panel"
        hx-swap="innerHTML"
        style="margin-top:14px;background:#1e293b;border:1px solid #334155;color:#94a3b8;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px">
        ← Back to intel
      </button>
    </div>
    """)


# ── Sentiment leaderboard ──────────────────────────────────────────────────────

@app.get("/sentiment-leaderboard", response_class=HTMLResponse)
def sentiment_leaderboard(sort: str = "final_score", dir: str = "desc", limit: int = 320):
    """Top symbols by final_score — shows all 320 scored articles aggregate."""
    allowed_sort = {"final_score", "symbol", "score_updated_at", "industry"}
    sort = sort if sort in allowed_sort else "final_score"
    order = "DESC" if dir != "asc" else "ASC"
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT symbol, company_name, industry, final_score,
                       score_updated_at, exchange,
                       (SELECT COUNT(*) FROM news_articles na
                        WHERE na.symbol_id = s.id AND na.sentiment_score IS NOT NULL) AS scored_count,
                       (SELECT COUNT(*) FROM news_articles na
                        WHERE na.symbol_id = s.id) AS total_count
                FROM symbols s
                WHERE final_score IS NOT NULL AND status = TRUE
                ORDER BY {sort} {order}
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            total = len(rows)
            # summary stats
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

    def score_color(s):
        s = float(s or 0)
        if s >  0.30: return "#4ade80"
        if s >  0.10: return "#86efac"
        if s >  0.05: return "#fbbf24"
        if s < -0.30: return "#f87171"
        if s < -0.10: return "#fca5a5"
        if s < -0.05: return "#fb923c"
        return "#94a3b8"

    def fmt_score(s):
        return f"{float(s):+.4f}" if s is not None else "—"

    def th(label, col):
        arrow = " ▼" if (sort == col and dir == "desc") else " ▲" if (sort == col and dir == "asc") else ""
        nd = "asc" if (sort == col and dir == "desc") else "desc"
        return (f'<th style="padding:9px 12px;text-align:left;cursor:pointer" '
                f'hx-get="/sentiment-leaderboard?sort={col}&dir={nd}&limit={limit}" '
                f'hx-target="#sent-panel" hx-swap="innerHTML">{label}{arrow}</th>')

    rows_html = ""
    for i, r in enumerate(rows, 1):
        sc = r["final_score"]
        col = score_color(sc)
        upd = r["score_updated_at"].strftime("%m-%d %H:%M") if r["score_updated_at"] else "—"
        rows_html += f"""
        <tr style="border-bottom:1px solid #1e2535">
          <td style="padding:8px 12px;color:#64748b;font-size:12px">{i}</td>
          <td style="padding:8px 12px;font-weight:700;color:#60a5fa;font-size:13px">{r['symbol']}</td>
          <td style="padding:8px 12px;font-size:12px;color:#94a3b8;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{r.get('company_name') or '—'}</td>
          <td style="padding:8px 12px;font-size:11px;color:#64748b">{r.get('industry') or '—'}</td>
          <td style="padding:8px 12px;text-align:right">
            <span style="font-size:14px;font-weight:800;color:{col}">{fmt_score(sc)}</span>
          </td>
          <td style="padding:8px 12px;text-align:center;font-size:11px;color:#64748b">{r.get('scored_count',0)}/{r.get('total_count',0)}</td>
          <td style="padding:8px 12px;text-align:center;font-size:11px;color:#475569">{upd}</td>
        </tr>"""

    avg_s = stats["avg_fs"]
    avg_col = score_color(avg_s)

    return f"""
    <div id="sent-panel" style="padding:20px">
      <div style="display:flex;align-items:center;gap:16px;margin-bottom:16px;flex-wrap:wrap">
        <h2 style="margin:0;color:#e2e8f0;font-size:18px">🏆 Sentiment Leaderboard</h2>
        <span style="background:#1e293b;color:#fcd34d;padding:3px 10px;border-radius:12px;font-size:12px">{total} symbols scored</span>
        <span style="background:#1e293b;color:#4ade80;padding:3px 10px;border-radius:12px;font-size:12px">🟢 {stats['bullish']} bullish</span>
        <span style="background:#1e293b;color:#f87171;padding:3px 10px;border-radius:12px;font-size:12px">🔴 {stats['bearish']} bearish</span>
        <span style="background:#1e293b;color:{avg_col};padding:3px 10px;border-radius:12px;font-size:12px">avg {fmt_score(avg_s)}</span>
        <span style="background:#1e293b;color:#86efac;padding:3px 10px;border-radius:12px;font-size:12px">max {fmt_score(stats['max_fs'])}</span>
        <span style="background:#1e293b;color:#fca5a5;padding:3px 10px;border-radius:12px;font-size:12px">min {fmt_score(stats['min_fs'])}</span>
      </div>
      <div style="background:#161b27;border:1px solid #1e2535;border-radius:10px;overflow-y:auto;max-height:70vh">
        <table style="width:100%;border-collapse:collapse">
          <thead style="background:#1a2133;color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;position:sticky;top:0">
            <tr>
              {th('#', 'final_score')}
              {th('Symbol', 'symbol')}
              <th style="padding:9px 12px">Name</th>
              {th('Industry', 'industry')}
              {th('Final Score', 'final_score')}
              <th style="padding:9px 12px;text-align:center">Articles</th>
              {th('Updated', 'score_updated_at')}
            </tr>
          </thead>
          <tbody>
            {''.join(rows_html) if rows_html else '<tr><td colspan="7" style="text-align:center;padding:40px;color:#64748b">No symbols scored yet — run sentiment_scoring.py</td></tr>'}
          </tbody>
        </table>
      </div>
    </div>
    """





# ── Priority Queue routes ─────────────────────────────────────────────────────

@app.post("/priority/toggle/{sym_id}", response_class=HTMLResponse)
async def priority_toggle(sym_id: int, q: str = "", filter: str = "all", sort: str = "alpha"):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT symbol_id FROM priority_queue WHERE symbol_id = %s", (sym_id,))
            exists = cur.fetchone()
            if exists:
                cur.execute("DELETE FROM priority_queue WHERE symbol_id = %s", (sym_id,))
                # Compact ranks after removal
                cur.execute("""
                    WITH ranked AS (
                        SELECT symbol_id, ROW_NUMBER() OVER (ORDER BY rank, added_at) AS new_rank
                        FROM priority_queue
                    )
                    UPDATE priority_queue pq SET rank = ranked.new_rank
                    FROM ranked WHERE pq.symbol_id = ranked.symbol_id
                """)
            else:
                cur.execute("SELECT COALESCE(MAX(rank), 0) + 1 AS next_rank FROM priority_queue")
                next_rank = cur.fetchone()["next_rank"]
                cur.execute(
                    "INSERT INTO priority_queue (symbol_id, rank) VALUES (%s, %s) ON CONFLICT (symbol_id) DO NOTHING",
                    (sym_id, next_rank)
                )
        conn.commit()
    finally:
        conn.close()
    # Re-render the symbol list with current filters
    from fastapi import Request
    from fastapi.responses import RedirectResponse
    # Delegate to the symbols endpoint
    return await symbols(q=q, filter=filter, sort=sort)


@app.get("/priority-panel", response_class=HTMLResponse)
async def priority_panel():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT pq.rank, pq.symbol_id, s.symbol, s.company_name,
                       COUNT(na.id) FILTER (WHERE na.sentiment_score IS NULL) AS unscored,
                       COUNT(na.id) AS total_articles
                FROM priority_queue pq
                JOIN symbols s ON s.id = pq.symbol_id
                LEFT JOIN news_articles na ON na.symbol_id = pq.symbol_id
                GROUP BY pq.rank, pq.symbol_id, s.symbol, s.company_name
                ORDER BY pq.rank ASC
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        queue_html = '<div style="color:#475569;font-size:13px;padding:20px;text-align:center">No symbols in priority queue.<br>Click ⭐ next to any symbol in the list to add it.</div>'
    else:
        queue_html = ""
        for r in rows:
            queue_html += f"""
            <div style="display:flex;align-items:center;gap:12px;padding:10px 16px;border-bottom:1px solid #1e2535">
              <span class="prio-rank">#{r['rank']}</span>
              <div style="flex:1">
                <span style="font-weight:700;color:#60a5fa;font-size:14px">{r['symbol']}</span>
                <span style="color:#64748b;font-size:12px;margin-left:8px">{r['company_name'] or ''}</span>
              </div>
              <span style="font-size:11px;color:#475569">{r['unscored']} unscored / {r['total_articles']} total</span>
              <button
                class="btn btn-danger"
                style="font-size:11px;padding:3px 9px"
                hx-post="/priority/toggle/{r['symbol_id']}"
                hx-target="#feed-panel"
                hx-swap="innerHTML"
                hx-vals='{{"filter":"priority"}}'
              >Remove</button>
            </div>"""

    return HTMLResponse(f"""
    <div style="padding:20px">
      <div style="display:flex;align-items:center;gap:16px;margin-bottom:16px;flex-wrap:wrap">
        <h2 style="margin:0;color:#fcd34d;font-size:18px">⭐ Priority Queue</h2>
        <span style="background:#451a0344;color:#fbbf24;border:1px solid #b4530966;padding:3px 10px;border-radius:12px;font-size:12px">{len(rows)} symbols</span>
        <button
          hx-post="/priority/clear"
          hx-target="#feed-panel"
          hx-swap="innerHTML"
          hx-confirm="Reset entire priority queue?"
          class="btn btn-danger"
          style="margin-left:auto;font-size:12px"
        >🗑 Reset All Priority</button>
      </div>
      <div style="background:#161b27;border:1px solid #1e2535;border-radius:10px;overflow:hidden">
        <div style="padding:10px 16px;background:#1a2133;border-bottom:1px solid #1e2535;font-size:11px;color:#475569;font-weight:700;text-transform:uppercase;letter-spacing:0.5px">
          These symbols will be scored first on the next (and current) sentiment scoring run, in rank order.
        </div>
        {queue_html}
      </div>
    </div>
    """)


@app.post("/priority/clear", response_class=HTMLResponse)
async def priority_clear():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM priority_queue")
        conn.commit()
    finally:
        conn.close()
    return await priority_panel()


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _ensure_priority_queue_table()
    port = 8055
    def _open():
        time.sleep(1.2)
        webbrowser.open(f"http://localhost:{port}")
    threading.Thread(target=_open, daemon=True).start()
    logger.info(f"TradeIntel Admin → http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
