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
</style>
</head>
<body>
<div class="header">
  <h1>TradeIntel</h1>
  <span class="badge">RSS MANAGER</span>
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
      hx-get="/symbols" hx-trigger="load" hx-target="#sym-list" hx-swap="innerHTML"
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
                       COUNT(DISTINCT na.id) AS article_count
                FROM symbols s
                LEFT JOIN rss_feeds f   ON f.symbol_id  = s.id
                LEFT JOIN news_articles na ON na.symbol_id = s.id
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
        html += f"""
        <div class="sym-row"
          hx-get="/symbol/{r['id']}/feeds"
          hx-target="#feed-panel"
          hx-swap="innerHTML"
          onclick="document.querySelectorAll('.sym-row').forEach(e=>e.classList.remove('active'));this.classList.add('active')"
        >
          <div>
            <div class="sym-ticker">{r['symbol']}</div>
            <div class="sym-name">{r['company_name'] or '—'}</div>
          </div>
          <div style="display:flex;gap:5px;align-items:center;">
            <span class="{art_cls}" title="{art} articles">{art_label}</span>
            <span class="feed-count">{feeds_label}</span>
            <button
              class="fetch-btn"
              title="Fetch news for {r['symbol']}"
              hx-post="/symbol/{r['id']}/fetch"
              hx-target="#feed-panel"
              hx-swap="innerHTML"
              hx-indicator="#fetch-spinner-{r['id']}"
              onclick="event.stopPropagation();document.querySelectorAll('.sym-row').forEach(e=>e.classList.remove('active'));this.closest('.sym-row').classList.add('active')"
            >▶</button>
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
      const content = document.getElementById('tab-content-' + symId);
      if (tab === 'news') {{
        content.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';
        htmx.ajax('GET', '/symbol/' + symId + '/news?page=1', content);
      }} else if (tab === 'sec') {{
        content.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';
        htmx.ajax('GET', '/symbol/' + symId + '/sec?page=1', content);
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
            capture_output=True, text=True, timeout=120,
            cwd=os.path.dirname(__file__)
        )
        output = (result.stdout + result.stderr).strip()
        elapsed = round(time.time() - start, 1)
    except subprocess.TimeoutExpired:
        output  = "ERROR: timed out after 120s"
        elapsed = 120

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
            if keyword:
                cur.execute("""
                    SELECT COUNT(*) AS total FROM news_articles
                    WHERE symbol_id = %s
                      AND (source_name IS NULL OR source_name != 'edgar_8k')
                      AND (title ILIKE %s OR full_text ILIKE %s)
                """, (sym_id, f"%{keyword}%", f"%{keyword}%"))
            else:
                cur.execute("""
                    SELECT COUNT(*) AS total FROM news_articles
                    WHERE symbol_id = %s
                      AND (source_name IS NULL OR source_name != 'edgar_8k')
                """, (sym_id,))
            total = cur.fetchone()["total"]

            if keyword:
                cur.execute("""
                    SELECT
                        na.id, na.title, na.url, na.published_at, na.inserted_at,
                        na.full_text,
                        rf.feed_url
                    FROM news_articles na
                    LEFT JOIN rss_feeds rf ON rf.id = na.feed_id
                    WHERE na.symbol_id = %s
                      AND (na.source_name IS NULL OR na.source_name != 'edgar_8k')
                      AND (na.title ILIKE %s OR na.full_text ILIKE %s)
                    ORDER BY na.published_at DESC NULLS LAST
                    LIMIT %s OFFSET %s
                """, (sym_id, f"%{keyword}%", f"%{keyword}%", per_page, offset))
            else:
                cur.execute("""
                    SELECT
                        na.id, na.title, na.url, na.published_at, na.inserted_at,
                        na.full_text,
                        rf.feed_url
                    FROM news_articles na
                    LEFT JOIN rss_feeds rf ON rf.id = na.feed_id
                    WHERE na.symbol_id = %s
                      AND (na.source_name IS NULL OR na.source_name != 'edgar_8k')
                    ORDER BY na.published_at DESC NULLS LAST
                    LIMIT %s OFFSET %s
                """, (sym_id, per_page, offset))
            articles = cur.fetchall()
    finally:
        conn.close()

    # ── Search box (always shown at top of news tab) ──────────────────────────
    q_safe = keyword.replace('"', '&quot;')
    html = f"""
    <div style="padding:12px 16px 0">
      <input type="text" id="news-search-{sym_id}" placeholder="Search keywords (FDA, patent, groundbreaking…)"
        value="{q_safe}"
        hx-get="/symbol/{sym_id}/news"
        hx-trigger="keyup changed delay:300ms"
        hx-target="#tab-content-{sym_id}"
        hx-include="#news-search-{sym_id}"
        name="q"
        autocomplete="off"
        style="width:100%"
      />
    </div>
    """

    if not articles and page == 1:
        if keyword:
            html += f'<div class="empty-state">No articles matching <strong>{q_safe}</strong></div>'
        else:
            html += '<div class="empty-state">No news articles yet — run main.py to ingest</div>'
        return HTMLResponse(html)

    for a in articles:
        pub = a["published_at"].strftime("%Y-%m-%d %H:%M") if a["published_at"] else "Unknown date"
        preview = ""
        if a["full_text"]:
            preview = a["full_text"][:300].replace("<", "&lt;").replace(">", "&gt;")
            if len(a["full_text"]) > 300:
                preview += "…"
        feed_chip = f'<span class="chip chip-gray" title="{a["feed_url"] or ""}">{(a["feed_url"] or "?")[:40]}…</span>' \
                    if a["feed_url"] and len(a["feed_url"]) > 40 \
                    else f'<span class="chip chip-gray">{a["feed_url"] or "unknown feed"}</span>'
        html += f"""
        <div class="news-card">
          <a class="news-title" href="{a['url']}" target="_blank">{a['title'] or 'Untitled'}</a>
          <div class="news-meta">
            <span class="news-date">📅 {pub}</span>
            {feed_chip}
          </div>
          {'<div class="news-preview">' + preview + '</div>' if preview else ''}
        </div>"""

    # ── Pagination (preserves keyword) ────────────────────────────────────────
    total_pages = max(1, (total + per_page - 1) // per_page)
    q_param = f"&q={q_safe}" if keyword else ""
    if total_pages > 1:
        html += '<div class="pagination">'
        if page > 1:
            html += f'<button class="btn btn-ghost" hx-get="/symbol/{sym_id}/news?page={page-1}{q_param}" hx-target="#tab-content-{sym_id}" hx-swap="innerHTML">← Prev</button>'
        html += f'<span style="color:#475569;font-size:.8rem;padding:5px 10px">Page {page} / {total_pages} &nbsp;·&nbsp; {total} articles</span>'
        if page < total_pages:
            html += f'<button class="btn btn-ghost" hx-get="/symbol/{sym_id}/news?page={page+1}{q_param}" hx-target="#tab-content-{sym_id}" hx-swap="innerHTML">Next →</button>'
        html += '</div>'

    return HTMLResponse(html)


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
    <div style="padding:12px 16px 0">
      <input type="text" id="sec-search-{sym_id}" placeholder="Search SEC filings (merger, acquisition, CEO…)"
        value="{q_safe}"
        hx-get="/symbol/{sym_id}/sec"
        hx-trigger="keyup changed delay:300ms"
        hx-target="#tab-content-{sym_id}"
        hx-include="#sec-search-{sym_id}"
        name="q"
        autocomplete="off"
        style="width:100%"
      />
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


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = 8055
    def _open():
        time.sleep(1.2)
        webbrowser.open(f"http://localhost:{port}")
    threading.Thread(target=_open, daemon=True).start()
    logger.info(f"TradeIntel Admin → http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
