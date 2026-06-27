"""
viewer.py — TradeIntel Public Sentiment Viewer
===============================================
Read-only leaderboard on http://localhost:8000
Shows ranked sentiment scores with full article details.
Run: python viewer.py
"""

import json
import logging
import math
import threading
import time
import webbrowser
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import uvicorn
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse

from config import DB_CONFIG

app = FastAPI(title="TradeIntel Viewer", docs_url=None, redoc_url=None)

_viewer_log = logging.getLogger("tradeintel.viewer")

# ── Mount SEC Intelligence Dashboard ─────────────────────────────────────────
try:
    from sec_dashboard import sec_router
    app.include_router(sec_router)
    _viewer_log.info("SEC dashboard mounted at /sec/{symbol}")
except Exception as _sec_e:
    _viewer_log.warning(f"SEC dashboard not loaded: {_sec_e}")


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ── Decay constants (must match pipeline_config.py) ───────────────────────────
_DECAY_LAMBDA      = 0.001   # per hour — matches SENTIMENT_LAMBDA in pipeline_config.py
_DECAY_GRACE_MONTHS = 1      # months before decay starts

def _decay_factor(published_at: datetime) -> float:
    """Return e^(-λt) multiplier for this article's age. 1.0 inside grace window."""
    now = datetime.now(timezone.utc)
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    age_h = max(0.0, (now - published_at).total_seconds() / 3600.0)
    grace_h = _DECAY_GRACE_MONTHS * 30.44 * 24
    if age_h <= grace_h:
        return 1.0
    return math.exp(-_DECAY_LAMBDA * (age_h - grace_h))


def _news_contrib_html(articles: list, sc: float, macro: float, ai_mult: float) -> str:
    """Build the News Score Contribution panel shown below the score hero."""
    scored = [a for a in articles if a.get("sentiment_score") is not None]
    if not scored:
        return ""

    total_abs = sum(abs(float(a["weighted_sentiment"] or 0)) for a in scored)
    n = len(scored)
    avg_w = sum(float(a["weighted_sentiment"] or 0) for a in scored) / n if n else 0.0

    rows_html = ""
    for a in sorted(scored, key=lambda x: abs(float(x["weighted_sentiment"] or 0)), reverse=True):
        raw   = float(a["sentiment_score"])
        w     = float(a["weighted_sentiment"] or 0)
        pub   = a["published_at"]
        df    = _decay_factor(pub) if pub else 1.0
        contrib = abs(w) / total_abs * 100 if total_abs > 0 else 0.0
        sc_col  = score_color(raw)
        df_col  = "#4ade80" if df > 0.5 else "#f59e0b" if df > 0.05 else "#f87171"
        w_col   = score_color(w)
        bar_w   = min(contrib, 100)
        title_s = (a["title"] or "")[:65].replace("<", "&lt;")

        # age label
        if pub:
            if pub.tzinfo is None:
                pub_utc = pub.replace(tzinfo=timezone.utc)
            else:
                pub_utc = pub
            age_d = (datetime.now(timezone.utc) - pub_utc).total_seconds() / 86400
            grace_d = _DECAY_GRACE_MONTHS * 30.44
            if age_d <= grace_d:
                age_lbl = f"{age_d:.0f}d (grace)"
                age_col = "#4ade80"
            else:
                age_lbl = f"{age_d:.0f}d old"
                age_col = "#64748b"
        else:
            age_lbl = "?"
            age_col = "#475569"

        rows_html += (
            f'<div style="display:grid;grid-template-columns:64px 10px 52px 10px 64px 1fr 44px 180px;'
            f'align-items:center;gap:4px;margin-bottom:5px;padding:6px 8px;'
            f'background:#0f172a;border-radius:6px;border-left:3px solid {sc_col}">'
            f'<div style="text-align:right;font-size:13px;font-weight:900;color:{sc_col}">{fmt_score(raw)}</div>'
            f'<div style="text-align:center;font-size:10px;color:#475569">×</div>'
            f'<div style="text-align:right;font-size:12px;color:{df_col}">{df:.4f}</div>'
            f'<div style="text-align:center;font-size:10px;color:#475569">=</div>'
            f'<div style="text-align:right;font-size:13px;font-weight:700;color:{w_col}">{fmt_score(w)}</div>'
            f'<div style="padding:0 6px">'
            f'  <div style="background:#1e2535;border-radius:3px;height:6px;overflow:hidden">'
            f'    <div style="height:6px;border-radius:3px;width:{bar_w:.1f}%;background:{sc_col}"></div>'
            f'  </div>'
            f'  <div style="font-size:9px;color:{age_col};margin-top:2px">{age_lbl}</div>'
            f'</div>'
            f'<div style="text-align:right;font-size:11px;color:#a78bfa;font-weight:700">{contrib:.1f}%</div>'
            f'<div style="font-size:10px;color:#64748b;white-space:nowrap;overflow:hidden;'
            f'text-overflow:ellipsis;padding-left:6px">{title_s}</div>'
            f'</div>'
        )

    header_row = (
        '<div style="display:grid;grid-template-columns:64px 10px 52px 10px 64px 1fr 44px 180px;'
        'align-items:center;gap:4px;margin-bottom:6px;padding:0 8px">'
        '<div style="text-align:right;font-size:9px;color:#475569">SCORE</div>'
        '<div></div>'
        '<div style="text-align:right;font-size:9px;color:#475569">DECAY</div>'
        '<div></div>'
        '<div style="text-align:right;font-size:9px;color:#475569">WEIGHTED</div>'
        '<div style="padding:0 6px;font-size:9px;color:#475569">CONTRIBUTION BAR</div>'
        '<div style="text-align:right;font-size:9px;color:#475569">SHARE</div>'
        '<div style="padding-left:6px;font-size:9px;color:#475569">TITLE</div>'
        '</div>'
    )

    footer = (
        f'<div style="margin-top:10px;padding-top:8px;border-top:1px solid #1e2535;'
        f'font-size:11px;color:#475569;line-height:1.8">'
        f'avg({n} articles) = <span style="color:#fcd34d;font-weight:700">{avg_w:.4f}</span>'
        f' × macro <span style="color:#60a5fa;font-weight:700">{macro:.4f}</span>'
        f' × ai_sector <span style="color:#a78bfa;font-weight:700">{ai_mult:.4f}</span>'
        f' = <span style="font-size:14px;font-weight:900;color:{score_color(sc)}">{fmt_score(sc)}</span>'
        f'</div>'
    )

    return (
        '<div style="margin-bottom:16px;background:#161b27;border:1px solid #1e2535;'
        'border-radius:10px;padding:14px 18px">'
        '<div style="font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;'
        'letter-spacing:.5px;margin-bottom:10px">📊 News Score — Contribution to Final Score</div>'
        + header_row
        + rows_html
        + footer
        + '</div>'
    )


def _news_contribution_panel(arts, sc, macro, ai_mult):
    """Render the News Score → Symbol Final Score contribution table."""
    if not arts:
        return ""
    scored = [a for a in arts if a["sentiment_score"] is not None]
    if not scored:
        return ""

    total_abs = max(sum(abs(float(a["weighted_sentiment"] or 0)) for a in scored), 1e-9)
    rows_html = ""
    for a in sorted(scored, key=lambda x: abs(float(x["weighted_sentiment"] or 0)), reverse=True):
        s  = float(a["sentiment_score"] or 0)
        w  = float(a["weighted_sentiment"] or 0)
        df = _decay_factor(a["published_at"]) if a["published_at"] else 1.0
        contrib = abs(w) / total_abs * 100
        sc_col  = score_color(s)
        df_col  = "#4ade80" if df > 0.5 else "#f59e0b" if df > 0.05 else "#f87171"
        w_col   = score_color(w)
        bar_w   = min(contrib, 100)
        title   = (a["title"] or "")[:55].replace("<", "&lt;").replace(">", "&gt;")
        rows_html += (
            f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:5px;padding:6px 8px;'
            f'background:#0f172a;border-radius:6px;border-left:3px solid {sc_col}">'
            f'<div style="flex-shrink:0;width:62px;text-align:right;font-size:13px;font-weight:900;color:{sc_col}">{fmt_score(s)}</div>'
            f'<div style="flex-shrink:0;width:20px;text-align:center;font-size:10px;color:#475569">×</div>'
            f'<div style="flex-shrink:0;width:50px;text-align:right;font-size:12px;color:{df_col}">{df:.4f}</div>'
            f'<div style="flex-shrink:0;width:20px;text-align:center;font-size:10px;color:#475569">=</div>'
            f'<div style="flex-shrink:0;width:62px;text-align:right;font-size:14px;font-weight:900;color:{w_col}">{fmt_score(w)}</div>'
            f'<div style="flex:1;margin-left:6px">'
            f'  <div style="background:#1e2535;border-radius:3px;height:5px;overflow:hidden">'
            f'    <div style="height:5px;border-radius:3px;width:{bar_w:.1f}%;background:{sc_col}"></div>'
            f'  </div>'
            f'</div>'
            f'<div style="flex-shrink:0;width:38px;text-align:right;font-size:11px;color:#a78bfa;font-weight:700">{contrib:.1f}%</div>'
            f'<div style="flex-shrink:0;width:190px;font-size:10px;color:#64748b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-left:6px">{title}</div>'
            f'</div>'
        )

    n = len(scored)
    avg_w = sum(float(a["weighted_sentiment"] or 0) for a in scored) / n
    return (
        f'<div style="margin-bottom:16px;background:#161b27;border:1px solid #1e2535;border-radius:10px;padding:14px 18px">'
        f'<div style="font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">'
        f'📊 News Score — Contribution to Final Score</div>'
        f'<div style="font-size:10px;color:#475569;margin-bottom:8px;display:flex;gap:16px;align-items:center">'
        f'<span>Article Score <span style="color:#94a3b8">×</span> Decay</span>'
        f'<span style="color:#475569">=</span>'
        f'<span style="color:#fcd34d;font-weight:700">Weighted Score</span>'
        f'<span style="color:#475569">→ bar = % of pool → feeds avg</span>'
        f'</div>'
        f'{rows_html}'
        f'<div style="margin-top:10px;padding-top:8px;border-top:1px solid #1e2535;font-size:11px;color:#475569">'
        f'avg({n} articles) = <span style="color:#fcd34d">{avg_w:.4f}</span>'
        f' × macro <span style="color:#60a5fa">{macro:.4f}</span>'
        f' × ai_sector <span style="color:#a78bfa">{ai_mult:.4f}</span>'
        f' = <span style="font-size:14px;font-weight:900;color:{score_color(sc)}">{fmt_score(sc)}</span>'
        f'</div>'
        f'</div>'
    )


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
    if s >= 0.75: return ("STRONG BUY",  "#4ade80")
    if s >= 0.60: return ("BUY",         "#86efac")
    if s >= 0.40: return ("NEUTRAL",     "#94a3b8")
    if s >= 0.25: return ("WEAK SELL",   "#fb923c")
    return ("SELL", "#f87171")


def fmt_score(s):
    return f"{float(s):+.4f}" if s is not None else "—"


BASE_STYLE = """
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<script src="https://unpkg.com/htmx.org@1.9.10"></script>
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
  <span id="scoring-ctrl-v"
        hx-get="/scoring/status"
        hx-trigger="load, every 5s"
        hx-swap="outerHTML"
        class="badge" style="color:#475569;font-size:11px">⏸ …</span>
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
        <span style="color:#60a5fa;font-weight:700">Step 1 — Anchor band selection (17 bands)</span><br>
        Each article is matched to a band based on event type and magnitude. The LLM scores on <b style="color:#f87171">−1.0</b> to <b style="color:#4ade80">+1.0</b>:
        <table style="margin-top:8px;border-collapse:collapse;width:100%">
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#4ade80;white-space:nowrap">+1.000</td><td style="padding:4px 8px">EPOCH_DEFINING — >50% fundamental revaluation (disease cure, 100%+ premium acquisition)</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#4ade80;white-space:nowrap">+0.995–0.999</td><td style="padding:4px 8px">TRANSFORMATIVE — revenue guidance >300%, world-class resource deposit, breakthrough proven</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#86efac;white-space:nowrap">+0.985–0.994</td><td style="padding:4px 8px">EXCEPTIONAL — earnings beat >100%, FDA final approval, first-in-class Phase 3 success</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#86efac;white-space:nowrap">+0.970–0.984</td><td style="padding:4px 8px">VERY_STRONG — revenue beat >50%, BLA/NDA submitted, FDA AdCom positive vote</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#d1fae5;white-space:nowrap">+0.940–0.969</td><td style="padding:4px 8px">STRONG — beat 20–50%, BLA/NDA accepted (admin step), Priority Review, IND cleared</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#d1fae5;white-space:nowrap">+0.900–0.939</td><td style="padding:4px 8px">CLEARLY_POSITIVE — beat 5–20% + raised guidance, analyst upgrade, margin improvement</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#fbbf24;white-space:nowrap">+0.800–0.899</td><td style="padding:4px 8px">POSITIVE — small beat, moderate contract, cost reduction program</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#94a3b8;white-space:nowrap">+0.600–0.799</td><td style="padding:4px 8px">SLIGHTLY_POSITIVE — non-exclusive partnership, product update, MOU signed</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#94a3b8;white-space:nowrap">+0.001–0.599</td><td style="padding:4px 8px">WEAK_POSITIVE — generic announcements, conference participation, minor improvements</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#94a3b8;white-space:nowrap">0.000</td><td style="padding:4px 8px">NEUTRAL — no material information, routine disclosures</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#fca5a5;white-space:nowrap">−0.001–−0.399</td><td style="padding:4px 8px">SLIGHTLY_NEGATIVE — minor delay, small contract loss, Hold initiation</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#fca5a5;white-space:nowrap">−0.400–−0.699</td><td style="padding:4px 8px">MILDLY_NEGATIVE — guidance cut <20%, analyst downgrade, minor regulatory setback</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#f87171;white-space:nowrap">−0.700–−0.899</td><td style="padding:4px 8px">NEGATIVE — earnings miss 5–20%, margin deterioration, write-down</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#f87171;white-space:nowrap">−0.900–−0.969</td><td style="padding:4px 8px">VERY_NEGATIVE — miss >20–50%, FDA CRL/IRL (de facto rejection), production shutdown</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#ef4444;white-space:nowrap">−0.970–−0.989</td><td style="padding:4px 8px">EXTREMELY_NEGATIVE — guidance cut >50%, loss of largest customer, flagship Phase 3 failure</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:4px 8px;color:#ef4444;white-space:nowrap">−0.990–−0.999</td><td style="padding:4px 8px">EXISTENTIAL_THREAT — acute liquidity crisis, DOJ/SEC criminal investigation</td></tr>
          <tr><td style="padding:4px 8px;color:#dc2626;white-space:nowrap">−1.000</td><td style="padding:4px 8px">CATASTROPHIC — bankruptcy, fraud proven, core product banned</td></tr>
        </table>
      </div>
      <div style="margin-bottom:12px">
        <span style="color:#60a5fa;font-weight:700">Step 2 — 10-factor calibration within band</span><br>
        F1 Surprise · F2 Revenue Impact · F3 Profit Impact · F4 Strategic Importance · F5 Competitive Moat · F6 Regulatory Impact · F7 LT Value Creation · F8 Management Credibility · F9 Recurrence Penalty · F10 Market Cap Scale<br>
        <span style="color:#64748b">Each factor adds or subtracts ±0.005 to ±0.020 to place the score precisely within the selected band.</span>
      </div>
      <div style="margin-bottom:12px">
        <span style="color:#60a5fa;font-weight:700">Step 3 — Outlook bonus (0.00–0.03)</span><br>
        A small forward-looking nudge for articles with specific named catalysts + dates. Max +0.03 — stays within one band step.<br>
        <code style="background:#0f1117;padding:2px 6px;border-radius:4px;color:#fcd34d">article_score = raw_score + outlook_bonus</code>
      </div>
      <div style="margin-bottom:12px">
        <span style="color:#60a5fa;font-weight:700">Step 4 — Time decay</span><br>
        Old news matters less. Each score is decayed by age (grace period: 6 months, then exponential):<br>
        <code style="background:#0f1117;padding:2px 6px;border-radius:4px;color:#fcd34d">weighted = article_score × e^(−λ × hours_after_grace)</code><br>
        <span style="color:#64748b">λ = 0.001/hr → half-life ~29 days after grace ends. Fresh articles stay at full weight.</span>
      </div>
      <div style="margin-bottom:12px">
        <span style="color:#60a5fa;font-weight:700">Step 5 — Symbol final score</span><br>
        <code style="background:#0f1117;padding:2px 6px;border-radius:4px;color:#fcd34d">final_score = weighted_mean(all decayed scores) × macro_multiplier × ai_sector_multiplier</code><br>
        <span style="color:#64748b">Macro and AI-sector multipliers adjust for industry tailwinds/headwinds. Default = 1.0.</span>
      </div>
      <div>
        <span style="color:#60a5fa;font-weight:700">Why high raw scores → low final score?</span><br>
        If most articles are old (weeks/months), time-decay brings weighted scores toward 0.00. Many neutral (0.00) articles also dilute the weighted mean.
        The final score reflects <em>current</em> sentiment momentum, not historical highs.
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
                       s.total_revenue, s.net_income, s.sec_score_modifier,
                       (SELECT COALESCE(MAX(macro_multiplier), 1.000)
                          FROM sectors_macro
                         WHERE industry_name ILIKE '%%' || s.industry || '%%')::float AS macro_multiplier,
                       (SELECT rationale
                          FROM sectors_macro
                         WHERE industry_name ILIKE '%%' || s.industry || '%%'
                         ORDER BY macro_multiplier DESC LIMIT 1) AS sector_rationale,
                       (SELECT sector_name
                          FROM sectors_macro
                         WHERE industry_name ILIKE '%%' || s.industry || '%%'
                         ORDER BY macro_multiplier DESC LIMIT 1) AS sector_name,
                       s.ai_sector_pick, s.ai_sector_multiplier
                FROM symbols s
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
                # Word-boundary regex match (PostgreSQL ~*) instead of ILIKE '%k%'.
                # The substring match was catching false positives like
                # 'Bodybuilding' matching keyword 'Building Products'. Anchoring
                # to word boundaries fixes that. Each keyword is regex-escaped.
                import re as _re
                _patterns = [r"\m" + _re.escape(k) + r"\M" for k in _mr_keywords]
                _conditions = " OR ".join(
                    ["(mra.title ~* %s OR mra.summary ~* %s)" for _ in _patterns]
                )
                _params = []
                for p in _patterns:
                    _params += [p, p]
                cur.execute(f"""
                    SELECT mra.title, mra.url, mra.published_at, mra.source_name, mra.summary
                    FROM market_research_articles mra
                    WHERE mra.llm_processed = TRUE AND ({_conditions})
                    ORDER BY mra.published_at DESC
                    LIMIT 8
                """, _params)
                mr_articles = cur.fetchall()
                # Fallback: if word-boundary match returned nothing AND we have
                # a sector_name, retry with sector ONLY as a looser ILIKE so the
                # panel isn't empty for symbols whose multi-word industries
                # never appear verbatim in MR feeds.
                if not mr_articles and _sector_name:
                    cur.execute("""
                        SELECT mra.title, mra.url, mra.published_at, mra.source_name, mra.summary
                        FROM market_research_articles mra
                        WHERE mra.llm_processed = TRUE
                          AND (mra.title ~* %s OR mra.summary ~* %s)
                        ORDER BY mra.published_at DESC
                        LIMIT 8
                    """, (r"\m" + _re.escape(_sector_name) + r"\M",
                          r"\m" + _re.escape(_sector_name) + r"\M"))
                    mr_articles = cur.fetchall()
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
                       sentiment_score, weighted_sentiment, outlook_bonus,
                       article_summary, score_rationale, forecast_until_earnings,
                       key_events, pre_summary_data, master_summary_snapshot,
                       stage2_prompt, company_connections,
                       full_text, summary
                FROM news_articles
                WHERE symbol_id = %s AND sentiment_score IS NOT NULL
                ORDER BY published_at DESC NULLS LAST
                LIMIT 50
            """, (sym_id,))
            articles = cur.fetchall()
    finally:
        conn.close()

    sc = sym["final_score"] or 0.0
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

    # Pre-compute total absolute weighted sum for contribution % calculation
    _total_abs_weighted = sum(abs(float(a["weighted_sentiment"] or 0)) for a in articles)

    # Articles HTML
    arts_html = ""
    for i, a in enumerate(articles):
        ascore    = a["sentiment_score"]
        abonus    = float(a["outlook_bonus"] or 0) if a.get("outlook_bonus") is not None else 0.0
        araw      = round(float(ascore) - abonus, 4) if ascore is not None else None
        aweighted = a.get("weighted_sentiment")
        pub_dt    = a["published_at"]

        # Real decay factor: e^(-λt) computed from published_at
        if pub_dt is not None:
            df = _decay_factor(pub_dt)
            now_utc = datetime.now(timezone.utc)
            if pub_dt.tzinfo is None:
                pub_dt_utc = pub_dt.replace(tzinfo=timezone.utc)
            else:
                pub_dt_utc = pub_dt
            age_days = (now_utc - pub_dt_utc).total_seconds() / 86400.0
            grace_days = _DECAY_GRACE_MONTHS * 30.44
            in_grace = age_days <= grace_days
        else:
            df = 1.0
            age_days = 0.0
            in_grace = True

        # Contribution % of this article to total weighted pool
        if _total_abs_weighted > 0 and aweighted is not None:
            contrib_pct = abs(float(aweighted)) / _total_abs_weighted * 100
        else:
            contrib_pct = 0.0

        acol = score_color(ascore)
        albl, alcol = score_label(ascore)
        pub = a["published_at"].strftime("%Y-%m-%d %H:%M") if a["published_at"] else "?"
        title = (a["title"] or "Untitled").replace("<","&lt;").replace(">","&gt;")
        src_link = f'<a href="{a["url"]}" target="_blank" style="color:#60a5fa">↗ Source</a>' if a.get("url") else ""

        # Score breakdown mini-table
        grace_label = (f'<span style="color:#4ade80;font-size:9px">IN GRACE — no decay yet</span>'
                       if in_grace else
                       f'<span style="color:#f59e0b;font-size:9px">{age_days:.0f}d old · {age_days - grace_days:.0f}d past grace</span>')
        w_col = score_color(aweighted)
        score_breakdown = f"""
        <div style="margin-top:8px;background:#0a1628;border:1px solid #1e2535;border-radius:8px;padding:10px 14px;font-size:12px">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
            <span style="font-size:10px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:.5px">📐 Score Breakdown</span>
            <span style="font-size:11px;color:#475569">LLM {fmt_score(araw)}
              {"<span style='color:#4ade80'> + bonus " + f"+{abonus:.4f}</span>" if abonus else ""}
              → stored {fmt_score(ascore)}
              × decay <span style="color:{'#4ade80' if df > 0.5 else '#f59e0b' if df > 0.05 else '#f87171'}">{df:.4f}</span>
              = <span style="font-weight:900;color:{w_col}">{fmt_score(aweighted)}</span>
            </span>
          </div>
          <div style="display:grid;grid-template-columns:1fr 2fr 1fr;gap:8px;align-items:center">
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;text-align:center">
              <div style="background:#0f172a;border:1px solid #1e2535;border-radius:6px;padding:6px 4px">
                <div style="font-size:9px;color:#475569;text-transform:uppercase;letter-spacing:.3px;margin-bottom:3px">LLM Raw</div>
                <div style="font-size:14px;font-weight:900;color:{score_color(araw)}">{fmt_score(araw)}</div>
              </div>
              <div style="background:#0f172a;border:1px solid #1e2535;border-radius:6px;padding:6px 4px">
                <div style="font-size:9px;color:#475569;text-transform:uppercase;letter-spacing:.3px;margin-bottom:3px">Bonus</div>
                <div style="font-size:14px;font-weight:900;color:{'#4ade80' if abonus else '#475569'}">{f'+{abonus:.4f}' if abonus else '—'}</div>
              </div>
              <div style="background:#0f172a;border:1px solid #1e2535;border-radius:6px;padding:6px 4px">
                <div style="font-size:9px;color:#475569;text-transform:uppercase;letter-spacing:.3px;margin-bottom:3px">Stored</div>
                <div style="font-size:14px;font-weight:900;color:{acol}">{fmt_score(ascore)}</div>
              </div>
              <div style="background:#0f172a;border:1px solid #1e2535;border-radius:6px;padding:6px 4px">
                <div style="font-size:9px;color:#475569;text-transform:uppercase;letter-spacing:.3px;margin-bottom:3px">Decay ×</div>
                <div style="font-size:14px;font-weight:900;color:{'#4ade80' if df>0.5 else '#f59e0b' if df>0.05 else '#f87171'}">{df:.4f}</div>
                <div style="font-size:8px;margin-top:1px">{grace_label}</div>
              </div>
            </div>
            <div style="background:#0d1f0d;border:2px solid {w_col};border-radius:8px;padding:8px 6px;text-align:center">
              <div style="font-size:8px;font-weight:700;color:{w_col};text-transform:uppercase;letter-spacing:.4px;margin-bottom:2px">↳ Contributes</div>
              <div style="font-size:20px;font-weight:900;color:{w_col};line-height:1">{fmt_score(aweighted)}</div>
              <div style="font-size:8px;color:#475569;margin-top:2px">stored × decay</div>
            </div>
            <div style="background:#0f172a;border:1px solid #a78bfa33;border-radius:8px;padding:10px 8px;text-align:center">
              <div style="font-size:9px;color:#a78bfa;text-transform:uppercase;letter-spacing:.3px;margin-bottom:4px">Share of Pool</div>
              <div style="font-size:24px;font-weight:900;color:#a78bfa">{contrib_pct:.1f}%</div>
              <div style="font-size:9px;color:#475569;margin-top:4px">of all weighted scores</div>
            </div>
          </div>
        </div>"""

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

        # Stage 1 INPUT — raw article text fed to the pre-summarizer
        s1_in_text = (a.get("full_text") or a.get("summary") or "").replace("<","&lt;")
        s1_in_title = (a.get("title") or "").replace("<","&lt;")
        s1_in_block = (f'<div style="font-size:11px;color:#475569;margin-bottom:4px"><b>Title:</b> {s1_in_title}</div>'
                       f'<div style="background:#060c18;border:1px solid #1e2535;border-radius:6px;padding:8px;'
                       f'font-size:11px;color:#cbd5e1;white-space:pre-wrap;max-height:200px;overflow-y:auto">'
                       f'{s1_in_text or "(no body — title-only article)"}</div>')

        # Stage 2 OUTPUT — what LLM returned (score + summary + rationale + forecast + events)
        s2_out = {
            "sentiment_score":      float(a["sentiment_score"]) if a.get("sentiment_score") is not None else None,
            "weighted_sentiment":   float(a["weighted_sentiment"]) if a.get("weighted_sentiment") is not None else None,
            "article_summary":      a.get("article_summary"),
            "score_rationale":      a.get("score_rationale"),
            "forecast_until_earnings": a.get("forecast_until_earnings"),
            "updated_master_summary":  a.get("master_summary_snapshot"),
            "key_events":           a.get("key_events"),
            "company_connections":  a.get("company_connections"),
        }
        try:
            s2_out_str = json.dumps(s2_out, indent=2, default=str).replace("<","&lt;")
        except Exception:
            s2_out_str = "(could not serialize)"

        def _stage_block(title_html, body_html):
            return (f'<div style="margin-top:10px"><div style="font-size:10px;font-weight:700;'
                    f'color:#94a3b8;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">{title_html}</div>'
                    f'{body_html}</div>')

        llm_input_html = f"""
        <details style="margin-top:10px">
          <summary style="cursor:pointer;font-size:10px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:.5px;user-select:none">🔬 LLM Pipeline I/O (click to expand)</summary>
          <div style="margin-top:8px">
            {_stage_block("☀ STAGE 1 — INPUT (raw article)", s1_in_block)}
            {_stage_block("☀ STAGE 1 — OUTPUT (extracted facts JSON)",
                '<div style="background:#060c18;border:1px solid #1e2535;border-radius:6px;padding:8px;font-size:11px;color:#94a3b8;white-space:pre-wrap;max-height:240px;overflow-y:auto;font-family:monospace">' + (pre_data_str or "(stage 1 not run / no data)") + '</div>')}
            {_stage_block("▶ STAGE 2 — INPUT (full prompt sent to scorer)",
                '<div style="background:#060c18;border:1px solid #1e2535;border-radius:6px;padding:8px;font-size:11px;color:#94a3b8;white-space:pre-wrap;max-height:280px;overflow-y:auto;font-family:monospace">' + (stage2_p or "(not saved — scored before v3.0.4)") + '</div>')}
            {_stage_block("▶ STAGE 2 — OUTPUT (score + summary + rationale + forecast)",
                '<div style="background:#060c18;border:1px solid #1e2535;border-radius:6px;padding:8px;font-size:11px;color:#cbd5e1;white-space:pre-wrap;max-height:280px;overflow-y:auto;font-family:monospace">' + s2_out_str + '</div>')}
            {_stage_block("📚 MASTER SUMMARY (rolling chain context after this article)",
                '<div style="background:#060c18;border:1px solid #1e2535;border-radius:6px;padding:8px;font-size:11px;color:#94a3b8;white-space:pre-wrap;max-height:180px;overflow-y:auto">' + (master_snap or "—") + '</div>')}
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
            <span style="color:{score_color(aweighted)}">⚖ weighted {fmt_score(aweighted)}</span>
            {src_link}
          </div>
          {score_breakdown}
          {('<div style="font-size:13px;color:#94a3b8;margin-top:8px;margin-bottom:6px">'+a['article_summary'].replace('<','&lt;')+'</div>') if a.get('article_summary') else ''}
          {('<details style="margin-bottom:6px"><summary style="cursor:pointer;font-size:10px;font-weight:700;color:#f59e0b;text-transform:uppercase;letter-spacing:.5px">Rationale</summary><div style="margin-top:6px;font-size:12px;color:#cbd5e1;line-height:1.6">' + a['score_rationale'].replace('<','&lt;') + '</div></details>') if a.get('score_rationale') else ''}
          {('<details style="margin-bottom:6px"><summary style="cursor:pointer;font-size:10px;font-weight:700;color:#a78bfa;text-transform:uppercase;letter-spacing:.5px">Forecast</summary><div style="margin-top:6px;font-size:12px;color:#cbd5e1;line-height:1.6">' + a['forecast_until_earnings'].replace('<','&lt;') + '</div></details>') if a.get('forecast_until_earnings') else ''}
          {ke_html}
          {a_cc_html}
          {llm_input_html}
        </div>"""

    master = (sym.get("symbol_master_summary") or "").replace("<","&lt;")
    forecast = (sym.get("symbol_forecast_narrative") or "").replace("<","&lt;")
    sector_rat = (sym.get("sector_rationale") or "").replace("<","&lt;")

    # ── SEC Filings tab ──────────────────────────────────────────────────────
    conn2 = get_conn()
    try:
        with conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur2:
            cur2.execute("""
                SELECT id, form_type, filing_tier, published_at,
                       sentiment_score, sec_source_weight,
                       title, article_summary, score_rationale, url
                FROM news_articles
                WHERE symbol_id = %s AND form_type IS NOT NULL
                ORDER BY published_at DESC
                LIMIT 200
            """, (sym["id"],))
            sec_filings = [dict(r) for r in cur2.fetchall()]
    finally:
        conn2.close()

    sym_sec_count = len(sec_filings)

    _TIER_COLOR = {1: "#f87171", 2: "#fb923c", 3: "#a78bfa"}
    _TIER_LABEL = {1: "Tier 1 — Core", 2: "Tier 2 — Capital", 3: "Tier 3 — Ownership"}
    _FORM_ICON  = {
        "10-K": "📋", "10-K/A": "📋", "10-Q": "📈", "10-Q/A": "📈",
        "8-K": "🔔", "8-K/A": "🔔", "S-3": "⚠️", "S-3/A": "⚠️",
        "424B1": "⚠️", "424B3": "⚠️", "424B4": "⚠️", "424B5": "⚠️",
        "NT 10-K": "🚨", "NT 10-Q": "🚨",
        "4": "👤", "SC 13D": "🎯", "SC 13D/A": "🎯",
        "SC 13G": "🏦", "SC 13G/A": "🏦",
    }

    if sec_filings:
        # Summary bar by form type
        from collections import Counter
        form_counts = Counter(f.get("form_type","?") for f in sec_filings)
        scored_sec  = sum(1 for f in sec_filings if f.get("sentiment_score") is not None)
        sec_mod_val  = float(sym.get("sec_score_modifier") or 0)
        sec_mod_col  = "#4ade80" if sec_mod_val >= 0 else "#f87171"
        summary_bar = (
            f'<div style="display:flex;gap:16px;flex-wrap:wrap;padding:14px 18px;background:#0f172a;'
            f'border:1px solid #1e2535;border-radius:10px;margin-bottom:16px;align-items:center">'
            f'<div style="font-size:12px;color:#64748b">Total: <span style="color:#e2e8f0;font-weight:700">{sym_sec_count}</span></div>'
            f'<div style="font-size:12px;color:#64748b">Scored: <span style="color:#4ade80;font-weight:700">{scored_sec}</span> '
            f'/ Pending: <span style="color:#f59e0b;font-weight:700">{sym_sec_count - scored_sec}</span></div>'
            + "".join(
                f'<div style="background:#1e293b;padding:3px 10px;border-radius:10px;font-size:11px;color:#94a3b8">'
                f'{_FORM_ICON.get(ft,"📄")} <b>{ft}</b> × {cnt}</div>'
                for ft, cnt in sorted(form_counts.items())
            )
            + f'<div style="margin-left:auto;font-size:11px;color:#475569">SEC modifier: '
            f'<span style="color:{sec_mod_col};font-weight:700">'
            f'{sec_mod_val:+.4f}</span></div>'
            + '</div>'
        )

        # Tier separator headers
        current_tier = None
        rows = ""
        for f in sec_filings:
            ft      = f.get("form_type") or "?"
            tier    = f.get("filing_tier") or 1
            sc_val  = f.get("sentiment_score")
            weight  = f.get("sec_source_weight") or 1.0
            pub     = f.get("published_at")
            pub_str = pub.strftime("%Y-%m-%d") if pub else "?"
            title_s = (f.get("title") or f"{ft} filing").replace("<","&lt;")[:100]
            summary = (f.get("article_summary") or "").replace("<","&lt;")
            rat     = (f.get("score_rationale") or "").replace("<","&lt;")
            icon    = _FORM_ICON.get(ft, "📄")
            t_col   = _TIER_COLOR.get(tier, "#64748b")
            url_link = f'<a href="{f["url"]}" target="_blank" style="color:#60a5fa;font-size:11px">↗ EDGAR</a>' if f.get("url") else ""

            # Tier separator
            if tier != current_tier:
                current_tier = tier
                t_lbl = _TIER_LABEL.get(tier, f"Tier {tier}")
                rows += (
                    f'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;'
                    f'color:{t_col};margin:20px 0 8px;padding:6px 10px;border-left:3px solid {t_col};'
                    f'background:{t_col}11">{t_lbl}</div>'
                )

            # Score badge
            if sc_val is not None:
                sc_f = float(sc_val)
                sc_col = score_color(sc_f)
                sc_badge = (
                    f'<div style="text-align:right;flex-shrink:0">'
                    f'<div style="font-size:18px;font-weight:900;color:{sc_col}">{fmt_score(sc_f)}</div>'
                    f'<div style="font-size:9px;color:#64748b">×{float(weight):.1f}w</div>'
                    f'</div>'
                )
            else:
                sc_badge = (
                    '<div style="text-align:right;flex-shrink:0">'
                    '<div style="font-size:11px;color:#f59e0b;font-weight:700">⏳ Pending</div>'
                    '<div style="font-size:9px;color:#475569">Not scored</div>'
                    '</div>'
                )

            # Form type badge
            ft_badge = (
                f'<span style="background:{t_col}22;color:{t_col};padding:2px 8px;'
                f'border-radius:6px;font-size:11px;font-weight:700;border:1px solid {t_col}44">'
                f'{icon} {ft}</span>'
            )

            # Collapsible body
            body = ""
            if summary:
                body += f'<div style="margin-top:10px;font-size:13px;color:#94a3b8;line-height:1.6">{summary}</div>'
            if rat:
                body += (
                    f'<details style="margin-top:8px"><summary style="cursor:pointer;font-size:10px;'
                    f'font-weight:700;color:#f59e0b;text-transform:uppercase;letter-spacing:.5px">Rationale</summary>'
                    f'<div style="margin-top:6px;font-size:12px;color:#cbd5e1;line-height:1.6">{rat}</div></details>'
                )
            if not summary and not rat:
                body = '<div style="margin-top:8px;font-size:12px;color:#475569">Scoring pending — run orchestrator.py to score this filing.</div>'

            rows += (
                f'<div style="background:#0f172a;border:1px solid #1e2535;border-radius:10px;'
                f'padding:14px;margin-bottom:8px;border-left:3px solid {t_col}">'
                f'<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px">'
                f'<div style="flex:1">'
                f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">'
                f'{ft_badge}'
                f'<span style="font-size:11px;color:#475569">{pub_str}</span>'
                f'{url_link}'
                f'</div>'
                f'<div style="font-size:13px;color:#e2e8f0;font-weight:600;line-height:1.4">{title_s}</div>'
                f'{body}'
                f'</div>'
                f'{sc_badge}'
                f'</div>'
                f'</div>'
            )

        sec_filings_html = summary_bar + rows
    else:
        sec_filings_html = (
            '<div style="text-align:center;padding:40px;color:#475569">'
            '<div style="font-size:32px;margin-bottom:12px">🏛</div>'
            '<div style="font-size:14px;font-weight:700;color:#64748b;margin-bottom:8px">No SEC Filings Ingested</div>'
            '<div style="font-size:12px;color:#475569">Run: <code style="background:#0f172a;padding:2px 8px;'
            'border-radius:4px;color:#fcd34d">python scripts\\edgar_backfill.py --symbol '
            + sym["symbol"] + '</code></div>'
            '</div>'
        )

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
  <div style="display:flex;align-items:center;gap:20px;margin-bottom:16px;padding:20px;background:#161b27;border:1px solid #1e2535;border-radius:12px">
    <div>
      <div style="font-size:48px;font-weight:900;color:{col}">{fmt_score(sc)}</div>
      <div style="font-size:14px;font-weight:700;color:{lcol}">{lbl}</div>
    </div>
    <div style="flex:1">
      <div style="font-size:12px;color:#475569">Updated: {upd}</div>
      <div style="font-size:12px;color:#475569">Articles scored: {len(articles)}</div>
      {(lambda: (
        (lambda macro, ai_mult, ai_pick, sector: (
          (lambda base: (
            f'<div style="font-size:12px;color:#94a3b8;margin-top:8px;line-height:1.8">'
            f'<span style="color:#64748b">Base avg (time-decayed, weighted): </span>'
            f'<span style="color:#fcd34d;font-weight:700">{fmt(base)}</span><br>'
            f'<span style="color:#64748b">Macro multiplier (</span>'
            f'<span style="color:#60a5fa">{sector or "—"}</span>'
            f'<span style="color:#64748b">): </span>'
            f'<span style="color:#34d399;font-weight:700">×{fmt(macro)}</span>'
            + (f'<span style="color:#64748b;margin-left:8px;font-size:11px">(+{fmt(round((macro-1)*100, 2))}% boost)</span>' if macro and macro > 1 else '')
            + (f'<span style="color:#f87171;margin-left:8px;font-size:11px">({fmt(round((macro-1)*100, 2))}% drag)</span>' if macro and macro < 1 else '')
            + '<br>'
            f'<span style="color:#64748b">AI sector multiplier (</span>'
            f'<span style="color:#60a5fa">{ai_pick or "—"}</span>'
            f'<span style="color:#64748b">): </span>'
            f'<span style="color:#34d399;font-weight:700">×{fmt(ai_mult)}</span>'
            + (f'<span style="color:#64748b;margin-left:8px;font-size:11px">(+{fmt(round((ai_mult-1)*100, 2))}% boost)</span>' if ai_mult and ai_mult > 1 else '')
            + (f'<span style="color:#f87171;margin-left:8px;font-size:11px">({fmt(round((ai_mult-1)*100, 2))}% drag)</span>' if ai_mult and ai_mult < 1 else '')
            + '<br>'
            f'<span style="color:#64748b">Final = base × macro × ai_sec: </span>'
            f'<span style="color:{col};font-weight:700">{fmt_score(sc)}</span>'
            f'</div>'
          ))(round(float(sc) / (macro * ai_mult), 6) if (macro and ai_mult and (macro * ai_mult) != 0) else float(sc))
        ))(
          float(sym.get("macro_multiplier") or 1.0),
          float(sym.get("ai_sector_multiplier") or 1.0),
          sym.get("ai_sector_pick"),
          sym.get("sector_name"),
        )
      ))() if (sym.get("macro_multiplier") or sym.get("ai_sector_multiplier")) else '<div style="font-size:12px;color:#475569;margin-top:4px">No multipliers</div>'}
    </div>
  </div>

  <!-- News Score Contribution to Final Score -->
  {_news_contrib_html(articles, sc, float(sym.get("macro_multiplier") or 1.0), float(sym.get("ai_sector_multiplier") or 1.0))}

  <!-- Score band legend -->
  <details style="margin-bottom:16px;background:#161b27;border:1px solid #1e2535;border-radius:8px;padding:10px 16px">
    <summary style="cursor:pointer;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.5px;user-select:none">
      📐 Scoring System — how article scores and the symbol score are calculated
    </summary>
    <div style="margin-top:12px;font-size:12px;color:#94a3b8;line-height:1.7">
      <div style="margin-bottom:10px">
        <span style="color:#60a5fa;font-weight:700">Article score pipeline</span><br>
        <code style="background:#0f1117;padding:2px 6px;border-radius:4px;color:#fcd34d;font-size:11px">
          LLM raw score + outlook_bonus (max 0.03) = final score → × time-decay = weighted score
        </code><br>
        <span style="color:#475569;font-size:11px">Scores decay exponentially after a 6-month grace period (λ=0.001/hr → half-life ~29 days after grace).</span>
      </div>
      <div style="margin-bottom:10px">
        <span style="color:#60a5fa;font-weight:700">Symbol final score</span><br>
        <code style="background:#0f1117;padding:2px 6px;border-radius:4px;color:#fcd34d;font-size:11px">
          avg(all weighted scores) × macro_multiplier × ai_sector_multiplier
        </code>
      </div>
      <div style="margin-bottom:10px">
        <span style="color:#60a5fa;font-weight:700">17-band scoring rubric (anchor bands)</span>
        <table style="margin-top:6px;border-collapse:collapse;width:100%;font-size:11px">
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#f8fafc;font-weight:800">1.000</td><td style="padding:3px 8px;color:#fbbf24;font-weight:700">EPOCH-DEFINING</td><td style="padding:3px 8px">Fundamental valuation revised &gt;50% — disease cure, 100%+ acquisition premium. &lt;1 in 1000 articles.</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#4ade80">0.995–0.999</td><td style="padding:3px 8px;color:#4ade80;font-weight:700">TRANSFORMATIVE</td><td style="padding:3px 8px">Revenue guidance &gt;300% raise, massive sovereign contract, world-class resource confirmed.</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#4ade80">0.985–0.994</td><td style="padding:3px 8px;color:#4ade80;font-weight:700">EXCEPTIONAL</td><td style="padding:3px 8px">FDA final approval, Phase 3 success (first-in-class), earnings beat &gt;100%. +20% to +100%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#86efac">0.970–0.984</td><td style="padding:3px 8px;color:#86efac;font-weight:700">VERY STRONG</td><td style="padding:3px 8px">Revenue beat &gt;50%, BLA/NDA submitted, FDA Advisory Committee positive. +10% to +50%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#86efac">0.940–0.969</td><td style="padding:3px 8px;color:#86efac;font-weight:700">STRONG</td><td style="padding:3px 8px">Beat 20–50%, BLA accepted (admin step), Priority Review granted, IND cleared. +5% to +30%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#d1fae5">0.900–0.939</td><td style="padding:3px 8px;color:#d1fae5;font-weight:700">CLEARLY POSITIVE</td><td style="padding:3px 8px">Beat 5–20% + raised guidance, analyst upgrade, IND cleared for secondary asset. +3% to +15%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#94a3b8">0.800–0.899</td><td style="padding:3px 8px;color:#94a3b8;font-weight:700">POSITIVE</td><td style="padding:3px 8px">Beat &lt;5%, moderate contract, cost reduction. +1% to +10%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#64748b">0.600–0.799</td><td style="padding:3px 8px;color:#64748b;font-weight:700">SLIGHTLY POSITIVE</td><td style="padding:3px 8px">Non-exclusive partnership, product update, MOU/LOI. 0% to +5%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#475569">0.001–0.599</td><td style="padding:3px 8px;color:#475569;font-weight:700">WEAK POSITIVE</td><td style="padding:3px 8px">Generic announcements, conference participation. &lt;1%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#94a3b8">0.000</td><td style="padding:3px 8px;color:#94a3b8;font-weight:700">NEUTRAL</td><td style="padding:3px 8px">No material information. Routine procedural disclosures.</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#475569">−0.001–−0.399</td><td style="padding:3px 8px;color:#fb923c;font-weight:700">SLIGHTLY NEGATIVE</td><td style="padding:3px 8px">Minor delay, small contract loss. 0% to −3%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#fb923c">−0.400–−0.699</td><td style="padding:3px 8px;color:#fb923c;font-weight:700">MILDLY NEGATIVE</td><td style="padding:3px 8px">Guidance cut &lt;20%, analyst downgrade. −3% to −15%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#fca5a5">−0.700–−0.899</td><td style="padding:3px 8px;color:#fca5a5;font-weight:700">NEGATIVE</td><td style="padding:3px 8px">Earnings miss 5–20%, write-down, FDA CRL/IRL (de facto rejection). −5% to −30%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#f87171">−0.900–−0.969</td><td style="padding:3px 8px;color:#f87171;font-weight:700">VERY NEGATIVE</td><td style="padding:3px 8px">Miss &gt;20–50%, flagship product failure, FDA final rejection. −15% to −50%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#ef4444">−0.970–−0.989</td><td style="padding:3px 8px;color:#ef4444;font-weight:700">EXTREMELY NEGATIVE</td><td style="padding:3px 8px">Revenue cut &gt;50%, loss of largest customer. −30% to −70%</td></tr>
          <tr style="border-bottom:1px solid #1e2535"><td style="padding:3px 8px;color:#dc2626">−0.990–−0.999</td><td style="padding:3px 8px;color:#dc2626;font-weight:700">EXISTENTIAL THREAT</td><td style="padding:3px 8px">Liquidity crisis, DOJ/SEC criminal investigation. −40% to −90%</td></tr>
          <tr><td style="padding:3px 8px;color:#991b1b">−1.000</td><td style="padding:3px 8px;color:#991b1b;font-weight:700">CATASTROPHIC</td><td style="padding:3px 8px">Bankruptcy, fraud proven, core product banned. −50% to −100%</td></tr>
        </table>
      </div>
      <div style="margin-top:8px;color:#475569;font-size:11px">
        Scores use 10-factor calibration: F1 Surprise · F2 Revenue Impact · F3 Profit Impact · F4 Strategic · F5 Moat · F6 Regulatory · F7 LT Value · F8 Management · F9 Duplicate Penalty · F10 Market Cap Scale
      </div>
    </div>
  </details>

  <!-- TradingView data -->
  <div class="section-title">📊 TradingView Screener Data</div>
  <div class="tv-grid">{tv_grid}</div>

  <!-- Forecast + Master Summary tabs -->
  {(lambda: (
    f'''<div class="section-title">🧠 Narrative</div>
    <div class="tabbed" style="background:#0f172a;border:1px solid #1e2535;border-radius:8px;overflow:hidden">
      <div style="display:flex;border-bottom:1px solid #1e2535;background:#161b27">
        <button class="tabbtn active" data-tab="t-forecast"
                style="flex:1;padding:10px 14px;background:none;border:0;color:#fcd34d;font-size:12px;font-weight:700;letter-spacing:.5px;text-transform:uppercase;cursor:pointer;border-bottom:2px solid #fcd34d">
          🔮 Forecast
        </button>
        <button class="tabbtn" data-tab="t-master"
                style="flex:1;padding:10px 14px;background:none;border:0;color:#64748b;font-size:12px;font-weight:700;letter-spacing:.5px;text-transform:uppercase;cursor:pointer;border-bottom:2px solid transparent">
          📋 Master Summary
        </button>
      </div>
      <div id="t-forecast" class="tabpane" style="padding:14px;font-size:13px;color:#cbd5e1;line-height:1.7;white-space:pre-wrap">{forecast or '<span style="color:#475569">No forecast yet.</span>'}</div>
      <div id="t-master"   class="tabpane" style="padding:14px;font-size:13px;color:#cbd5e1;line-height:1.7;white-space:pre-wrap;display:none">{master or '<span style="color:#475569">No master summary yet.</span>'}</div>
    </div>
    <script>
      (function() {{
        const btns = document.querySelectorAll('.tabbtn');
        btns.forEach(b => b.addEventListener('click', () => {{
          btns.forEach(x => {{
            x.classList.remove('active');
            x.style.color = '#64748b';
            x.style.borderBottomColor = 'transparent';
          }});
          b.classList.add('active');
          b.style.color = '#fcd34d';
          b.style.borderBottomColor = '#fcd34d';
          document.querySelectorAll('.tabpane').forEach(p => p.style.display = 'none');
          document.getElementById(b.dataset.tab).style.display = 'block';
        }}));
      }})();
    </script>'''
    if (forecast or master) else ''
  ))()}

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

  <!-- ══════ TABBED: News + SEC Filings ══════ -->
  <div style="margin-top:24px">
    <!-- Tab buttons -->
    <div style="display:flex;gap:4px;border-bottom:2px solid #1e2535;margin-bottom:0">
      <button id="tab-news-btn" onclick="switchDetailTab('news')"
        style="padding:10px 20px;background:#1d4ed8;border:1px solid #3b82f6;border-bottom:2px solid #1d4ed8;
               color:#fff;font-size:13px;font-weight:700;border-radius:8px 8px 0 0;cursor:pointer;margin-bottom:-2px">
        📰 News &amp; Scored Articles ({len(articles)})
      </button>
      <button id="tab-sec-btn" onclick="switchDetailTab('sec')"
        style="padding:10px 20px;background:#161b27;border:1px solid #1e2535;border-bottom:2px solid #1e2535;
               color:#c084fc;font-size:13px;font-weight:700;border-radius:8px 8px 0 0;cursor:pointer;margin-bottom:-2px">
        🏛 SEC Filings ({sym_sec_count})
      </button>
    </div>

    <!-- News tab pane -->
    <div id="tab-pane-news" style="background:#0d1117;border:1px solid #1e2535;border-top:none;border-radius:0 8px 8px 8px;padding:20px">
      {"".join(arts_html) if arts_html else '<div style="color:#475569;font-size:13px;padding:20px">No scored articles yet.</div>'}
    </div>

    <!-- SEC tab pane -->
    <div id="tab-pane-sec" style="display:none;background:#0d1117;border:1px solid #1e2535;border-top:none;border-radius:0 8px 8px 8px;padding:20px">
      <!-- Charts button -->
      <div style="margin-bottom:18px">
        <a href="/sec/{sym['symbol']}" target="_blank"
           style="display:inline-flex;align-items:center;gap:8px;
                  background:linear-gradient(135deg,#4c1d95,#6d28d9);
                  color:#e9d5ff;font-size:13px;font-weight:700;
                  padding:10px 20px;border-radius:8px;text-decoration:none;
                  border:1px solid #7c3aed;letter-spacing:.3px;
                  box-shadow:0 0 12px #7c3aed44;transition:opacity .15s"
           onmouseover="this.style.opacity='.8'" onmouseout="this.style.opacity='1'">
          📊 View SEC Charts — {sym['symbol']}
          <span style="font-size:10px;opacity:.7">↗ opens in new tab</span>
        </a>
        <span style="font-size:11px;color:#475569;margin-left:12px">
          18 charts · Revenue · Margins · Cash Flow · Balance Sheet · Insiders · Dilution
        </span>
      </div>
      {sec_filings_html}
    </div>
  </div>

  <script>
  function switchDetailTab(tab) {{
    const isNews = tab === 'news';
    document.getElementById('tab-pane-news').style.display = isNews ? 'block' : 'none';
    document.getElementById('tab-pane-sec').style.display  = isNews ? 'none'  : 'block';
    const nb = document.getElementById('tab-news-btn');
    const sb = document.getElementById('tab-sec-btn');
    nb.style.background = isNews ? '#1d4ed8' : '#161b27';
    nb.style.borderColor = isNews ? '#3b82f6' : '#1e2535';
    nb.style.borderBottomColor = isNews ? '#1d4ed8' : '#1e2535';
    nb.style.color = isNews ? '#fff' : '#60a5fa';
    sb.style.background = !isNews ? '#4c1d95' : '#161b27';
    sb.style.borderColor = !isNews ? '#7c3aed' : '#1e2535';
    sb.style.borderBottomColor = !isNews ? '#4c1d95' : '#1e2535';
    sb.style.color = !isNews ? '#e9d5ff' : '#c084fc';
  }}
  // Auto-open SEC tab if URL hash is #sec
  if (window.location.hash === '#sec') switchDetailTab('sec');
  </script>

</div>
</body>
</html>""")


# ── Scoring control (viewer: pause/resume only, no tier) ──────────────────────

def _get_scoring_control_v() -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT paused FROM scoring_control WHERE id = 1")
            row = cur.fetchone()
        return {"paused": bool(row[0]) if row else False}
    except Exception:
        return {"paused": False}
    finally:
        conn.close()


def _set_paused_v(paused: bool) -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scoring_control SET paused = %s, updated_at = NOW() WHERE id = 1",
                (paused,)
            )
        conn.commit()
    finally:
        conn.close()


@app.get("/scoring/status", response_class=HTMLResponse)
def viewer_scoring_status():
    """Small pause/resume widget for the viewer header (no tier control)."""
    paused = _get_scoring_control_v()["paused"]
    pause_btn = (
        '<button hx-post="/scoring/resume" hx-target="#scoring-ctrl-v" hx-swap="outerHTML" '
        'style="background:#14532d;color:#4ade80;border:1px solid #166534;padding:4px 12px;'
        'border-radius:6px;cursor:pointer;font-size:11px;font-weight:700">▶ Resume</button>'
        if paused else
        '<button hx-post="/scoring/pause" hx-target="#scoring-ctrl-v" hx-swap="outerHTML" '
        'style="background:#450a0a;color:#fca5a5;border:1px solid #991b1b;padding:4px 12px;'
        'border-radius:6px;cursor:pointer;font-size:11px;font-weight:700">⏸ Pause</button>'
    )
    status = (
        '<span style="color:#fbbf24;font-size:11px;font-weight:700">⏸ PAUSED</span>'
        if paused else
        '<span style="color:#4ade80;font-size:11px;font-weight:700">▶ RUNNING</span>'
    )
    return HTMLResponse(
        f'<span id="scoring-ctrl-v" '
        f'hx-get="/scoring/status" hx-trigger="every 5s" hx-swap="outerHTML" '
        f'class="badge" style="display:inline-flex;align-items:center;gap:6px">'
        f'{status}{pause_btn}</span>'
    )


@app.post("/scoring/pause", response_class=HTMLResponse)
def viewer_scoring_pause():
    _set_paused_v(True)
    return viewer_scoring_status()


@app.post("/scoring/resume", response_class=HTMLResponse)
def viewer_scoring_resume():
    _set_paused_v(False)
    return viewer_scoring_status()


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = 8000
    def _open():
        time.sleep(1.2)
        webbrowser.open(f"http://localhost:{port}")
    threading.Thread(target=_open, daemon=True).start()
    print(f"TradeIntel Viewer → http://localhost:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
