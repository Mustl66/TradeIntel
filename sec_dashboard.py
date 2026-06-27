"""
sec_dashboard.py — TradeIntel SEC Intelligence Dashboard
=========================================================
Standalone FastAPI route module. Mounted into viewer.py via:
    from sec_dashboard import sec_router
    app.include_router(sec_router)

Routes:
    GET /sec/{symbol}          → full SEC dashboard page
    GET /sec/{symbol}/data     → raw JSON data for chart rendering

All chart data is sourced from:
    1. news_articles (form_type IS NOT NULL) — extracted_facts JSONB
    2. sec_signals — deterministic rule-based signals
    3. symbols — current state + sec_score_modifier

Charts rendered with Chart.js 4.4 + chartjs-plugin-annotation 3.0 via CDN.
Dark theme: matches the existing viewer.py colour palette.
"""

import json
import math
import re
import logging
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse

from config import DB_CONFIG

logger = logging.getLogger(__name__)
sec_router = APIRouter()

# ── DB helper ──────────────────────────────────────────────────────────────────

def _get_conn():
    return psycopg2.connect(**DB_CONFIG)

# ── Colour palette (matches viewer.py) ────────────────────────────────────────
C_GREEN  = "#4ade80"
C_RED    = "#f87171"
C_BLUE   = "#38bdf8"
C_ORANGE = "#fb923c"
C_PURPLE = "#c084fc"
C_SLATE  = "#64748b"
C_YELLOW = "#facc15"
C_TEAL   = "#2dd4bf"
C_BG     = "#0f172a"
C_CARD   = "#1e2535"
C_GRID   = "rgba(255,255,255,0.05)"
C_TEXT   = "#94a3b8"

# ── Financial figure parser ────────────────────────────────────────────────────

def _parse_value(raw: str) -> Optional[float]:
    """Parse verbatim figure like '$1.23B', '€450M', '12.4%', '-3.2 bps' → float in millions."""
    if not raw:
        return None
    s = str(raw).strip()
    # Handle percentage
    if "%" in s:
        m = re.search(r"[-+]?[\d,]+\.?\d*", s.replace(",", ""))
        return float(m.group()) if m else None
    # Handle bps
    if "bps" in s.lower():
        m = re.search(r"[-+]?[\d,]+\.?\d*", s.replace(",", ""))
        return float(m.group()) / 100 if m else None
    # Strip currency symbols
    s = re.sub(r"[€£¥$]", "", s)
    # Extract numeric part
    m = re.search(r"([-+]?[\d,]+\.?\d*)", s.replace(",", ""))
    if not m:
        return None
    num = float(m.group(1).replace(",", ""))
    s_upper = s.upper()
    if "T" in s_upper:
        num *= 1_000_000
    elif "B" in s_upper:
        num *= 1_000
    # M → already in millions (our base unit)
    elif "K" in s_upper:
        num /= 1_000
    return num


def _extract_figures(extracted_facts: dict, metric_keys: list[str],
                     period_hint: str = "") -> list[dict]:
    """Pull specific metrics from extracted_facts.financial_figures array.

    Handles both:
      • Structured array: [{"metric": "revenue", "value": "$4.1M", ...}, ...]
      • Legacy flat dict:  {"revenue": "$4.1M", ...}
    Matching is fuzzy: any metric_key that appears as a substring of the
    stored metric name (or vice-versa) counts as a hit.
    """
    ef = extracted_facts or {}
    raw_figs = ef.get("financial_figures") or []

    # ── Normalise to list of dicts ────────────────────────────────────────────
    if isinstance(raw_figs, dict):
        # Legacy flat dict → convert to list
        items = []
        for k, v in raw_figs.items():
            if k.startswith("_"):
                continue
            items.append({"metric": k, "value": str(v), "period": period_hint})
        raw_figs = items
    elif isinstance(raw_figs, list):
        # Filter out the _INSTRUCTION / _REQUIRED_METRICS meta objects
        raw_figs = [
            fig for fig in raw_figs
            if isinstance(fig, dict) and not (fig.get("metric") or "").startswith("_")
            and not "_INSTRUCTION" in fig and not "_REQUIRED_METRICS" in fig
        ]
    else:
        raw_figs = []

    # ── Also scan top-level keys in extracted_facts for flat metric storage ───
    # Some LLMs output metrics directly at the top level
    for k, v in ef.items():
        if k.startswith("_") or k in ("financial_figures", "guidance_and_outlook",
           "contracts_and_orders", "mergers_and_acquisitions", "partnerships_and_collaborations",
           "management_and_board_changes", "legal_and_regulatory_events", "patents_and_ip",
           "clinical_and_regulatory_pipeline", "products_and_technology", "capital_structure_events",
           "key_quotes", "people_mentioned", "connected_companies_detail", "earnings_calendar",
           "industry_and_market", "article_metadata", "headline_event"):
            continue
        if isinstance(v, (str, int, float)) and not isinstance(v, bool):
            raw_figs.append({"metric": k, "value": str(v), "period": period_hint,
                             "_from_toplevel": True})

    results = []
    seen_metrics = set()
    for fig in raw_figs:
        if not isinstance(fig, dict):
            continue
        metric = (fig.get("metric") or "").lower().strip()
        if not metric or metric in seen_metrics:
            continue
        # Fuzzy match: key is substring of metric or metric is substring of key
        matched = False
        for k in metric_keys:
            kl = k.lower()
            if kl in metric or metric in kl:
                matched = True
                break
        if not matched:
            continue
        val = _parse_value(fig.get("value") or "")
        if val is not None:
            seen_metrics.add(metric)
            results.append({
                "metric":  metric,
                "value":   val,
                "raw":     fig.get("value"),
                "period":  fig.get("period") or period_hint,
                "yoy":     fig.get("yoy_change"),
                "qoq":     fig.get("qoq_change"),
                "context": fig.get("context"),
            })
    return results


# ── Data builder ───────────────────────────────────────────────────────────────

def _build_dashboard_data(symbol: str) -> dict:
    """Query DB and build all chart-ready data structures."""
    conn = _get_conn()
    try:
        # ── Symbol meta ───────────────────────────────────────────────────────
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, symbol, company_name, industry, market_cap_formatted,
                       final_score, sec_score_modifier, last_10k_filed, last_10q_filed,
                       symbol_master_summary, ai_sector_pick, ai_sector_multiplier,
                       score_updated_at
                FROM symbols WHERE symbol = %s AND status = TRUE LIMIT 1
            """, (symbol.upper(),))
            sym = cur.fetchone()
        if not sym:
            return {"error": f"Symbol '{symbol}' not found"}
        sym = dict(sym)
        sym_id = sym["id"]

        # ── SEC filings ───────────────────────────────────────────────────────
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, form_type, filing_tier, published_at,
                       sentiment_score, sec_source_weight,
                       title, article_summary, score_rationale,
                       extracted_facts, key_events, is_relevant
                FROM news_articles
                WHERE symbol_id = %s AND form_type IS NOT NULL
                ORDER BY published_at DESC
                LIMIT 200
            """, (sym_id,))
            filings_raw = [dict(r) for r in cur.fetchall()]

        # ── sec_signals ───────────────────────────────────────────────────────
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT signal_type, signal_value, signal_text,
                       score_modifier, filed_at, form_type, is_active
                FROM sec_signals
                WHERE symbol_id = %s
                ORDER BY filed_at DESC
                LIMIT 100
            """, (sym_id,))
            signals = [dict(r) for r in cur.fetchall()]

        # ── News articles (for score composition) ─────────────────────────────
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT sentiment_score, weighted_sentiment, published_at,
                       sec_source_weight, form_type
                FROM news_articles
                WHERE symbol_id = %s AND sentiment_score IS NOT NULL
                ORDER BY published_at DESC
                LIMIT 50
            """, (sym_id,))
            scored_articles = [dict(r) for r in cur.fetchall()]

    finally:
        conn.close()

    # Parse extracted_facts JSON strings
    for f in filings_raw:
        ef = f.get("extracted_facts")
        if isinstance(ef, str):
            try:
                f["extracted_facts"] = json.loads(ef)
            except Exception:
                f["extracted_facts"] = {}
        ke = f.get("key_events")
        if isinstance(ke, str):
            try:
                f["key_events"] = json.loads(ke)
            except Exception:
                f["key_events"] = {}

    # Separate by form family
    def _forms(form_prefix):
        return [f for f in filings_raw
                if (f.get("form_type") or "").startswith(form_prefix)]

    tenk_filings = sorted(_forms("10-K"), key=lambda x: x["published_at"])
    tenq_filings = sorted(_forms("10-Q"), key=lambda x: x["published_at"])
    eightk_filings = sorted([f for f in filings_raw if f.get("form_type") in ("8-K","8-K/A")],
                             key=lambda x: x["published_at"])
    form4_filings  = [f for f in filings_raw if f.get("form_type") == "4"]
    sc13d_filings  = [f for f in filings_raw if (f.get("form_type") or "").startswith("SC 13D")]
    sc13g_filings  = [f for f in filings_raw if (f.get("form_type") or "").startswith("SC 13G")]
    s3_filings     = [f for f in filings_raw if (f.get("form_type") or "").startswith(("S-3","424B"))]
    nt_filings     = [f for f in filings_raw if (f.get("form_type") or "").startswith("NT")]

    # ── Chart 1 & 2: 10-K revenue + margins ──────────────────────────────────
    tenk_labels, tenk_revenue, tenk_growth = [], [], []
    tenk_gross_margin, tenk_op_margin, tenk_net_margin = [], [], []

    for f in tenk_filings[-5:]:
        ef = f.get("extracted_facts") or {}
        pub_year = f["published_at"].year if f.get("published_at") else "?"
        tenk_labels.append(str(pub_year))
        rev  = _extract_figures(ef, ["revenue"], str(pub_year))
        gm   = _extract_figures(ef, ["gross_margin"])
        om   = _extract_figures(ef, ["operating_margin"])
        nm   = _extract_figures(ef, ["net_margin"])
        tenk_revenue.append(rev[0]["value"] if rev else None)
        tenk_gross_margin.append(gm[0]["value"] if gm else None)
        tenk_op_margin.append(om[0]["value"] if om else None)
        tenk_net_margin.append(nm[0]["value"] if nm else None)
        # YoY growth from metadata
        yoy = rev[0].get("yoy") if rev else None
        if yoy:
            m = re.search(r"[-+]?[\d.]+", str(yoy))
            tenk_growth.append(float(m.group()) if m else None)
        else:
            tenk_growth.append(None)

    # ── Chart 3: Cash Flow Quality ────────────────────────────────────────────
    tenk_opcf, tenk_fcf, tenk_ni = [], [], []
    for f in tenk_filings[-5:]:
        ef = f.get("extracted_facts") or {}
        opcf = _extract_figures(ef, ["operating_cash_flow"])
        fcf  = _extract_figures(ef, ["free_cash_flow"])
        ni   = _extract_figures(ef, ["net_income"])
        tenk_opcf.append(opcf[0]["value"] if opcf else None)
        tenk_fcf.append(fcf[0]["value"] if fcf else None)
        tenk_ni.append(ni[0]["value"] if ni else None)

    # ── Chart 4: Balance Sheet (Cash vs Debt) ─────────────────────────────────
    tenk_cash, tenk_debt = [], []
    for f in tenk_filings[-5:]:
        ef = f.get("extracted_facts") or {}
        cash = _extract_figures(ef, ["cash_and_equivalents"])
        debt = _extract_figures(ef, ["total_debt"])
        tenk_cash.append(cash[0]["value"] if cash else None)
        tenk_debt.append(debt[0]["value"] if debt else None)

    # ── Chart 5: Share Dilution ────────────────────────────────────────────────
    tenk_shares = []
    for f in tenk_filings[-5:]:
        ef = f.get("extracted_facts") or {}
        sh = _extract_figures(ef, ["shares_outstanding"])
        tenk_shares.append(sh[0]["value"] if sh else None)

    # ── Chart 6: R&D + SGA ────────────────────────────────────────────────────
    tenk_rd, tenk_sga = [], []
    for f in tenk_filings[-5:]:
        ef = f.get("extracted_facts") or {}
        rd  = _extract_figures(ef, ["rd_spend"])
        sga = _extract_figures(ef, ["sga_spend"])
        tenk_rd.append(rd[0]["value"] if rd else None)
        tenk_sga.append(sga[0]["value"] if sga else None)

    # ── Charts 7-10: 10-Q quarterly ───────────────────────────────────────────
    tenq_labels, tenq_revenue = [], []
    tenq_gross_m, tenq_op_m, tenq_net_m = [], [], []
    tenq_burn, tenq_cash = [], []

    for f in tenq_filings[-5:]:
        ef = f.get("extracted_facts") or {}
        pub = f["published_at"]
        qtr_label = f"Q{((pub.month-1)//3)+1} {pub.year}" if pub else "?"
        tenq_labels.append(qtr_label)
        rev = _extract_figures(ef, ["revenue"])
        gm  = _extract_figures(ef, ["gross_margin"])
        om  = _extract_figures(ef, ["operating_margin"])
        nm  = _extract_figures(ef, ["net_margin"])
        opcf = _extract_figures(ef, ["operating_cash_flow"])
        capex = _extract_figures(ef, ["capex"])
        cash = _extract_figures(ef, ["cash_and_equivalents"])
        tenq_revenue.append(rev[0]["value"] if rev else None)
        tenq_gross_m.append(gm[0]["value"] if gm else None)
        tenq_op_m.append(om[0]["value"] if om else None)
        tenq_net_m.append(nm[0]["value"] if nm else None)
        o = opcf[0]["value"] if opcf else None
        c = capex[0]["value"] if capex else None
        tenq_burn.append((o - c) if (o is not None and c is not None) else o)
        tenq_cash.append(cash[0]["value"] if cash else None)

    # ── Chart 11: 8-K Event Timeline ─────────────────────────────────────────
    eightk_events = []
    for f in eightk_filings:
        pub = f.get("published_at")
        sc  = float(f.get("sentiment_score") or 0)
        eightk_events.append({
            "date":    pub.isoformat() if pub else None,
            "score":   sc,
            "title":   (f.get("title") or "")[:80],
            "summary": (f.get("article_summary") or "")[:120],
            "form":    f.get("form_type"),
        })

    # ── Chart 13: Form 4 insider timeline ────────────────────────────────────
    insider_events = []
    for sig in signals:
        if sig.get("signal_type") in ("insider_buy", "insider_sell"):
            filed = sig.get("filed_at")
            insider_events.append({
                "date":   filed.isoformat() if filed else None,
                "type":   sig["signal_type"],
                "value":  float(sig.get("signal_value") or 0),
                "text":   sig.get("signal_text") or "",
                "modifier": float(sig.get("score_modifier") or 0),
            })

    # ── Chart 16: S-3/424B dilution history ──────────────────────────────────
    dilution_events = []
    for f in s3_filings:
        pub = f.get("published_at")
        sc  = float(f.get("sentiment_score") or 0)
        dilution_events.append({
            "date":  pub.isoformat() if pub else None,
            "score": sc,
            "form":  f.get("form_type"),
            "title": (f.get("title") or "")[:80],
        })

    # ── Chart 17: Score Composition ──────────────────────────────────────────
    news_scores = [float(a["weighted_sentiment"] or 0)
                   for a in scored_articles
                   if not a.get("form_type") and a.get("weighted_sentiment") is not None]
    sec_scores  = [(float(a["weighted_sentiment"] or 0), float(a.get("sec_source_weight") or 1.0), a.get("form_type"))
                   for a in scored_articles
                   if a.get("form_type") and a.get("weighted_sentiment") is not None]

    news_base = sum(news_scores) / max(len(news_scores), 1) if news_scores else 0
    tenk_contrib  = sum(s for s, w, ft in sec_scores if ft and ft.startswith("10-K")) / max(len([x for x in sec_scores if x[2] and x[2].startswith("10-K")]), 1) if sec_scores else 0
    tenq_contrib  = sum(s for s, w, ft in sec_scores if ft and ft.startswith("10-Q")) / max(len([x for x in sec_scores if x[2] and x[2].startswith("10-Q")]), 1) if sec_scores else 0
    eightk_contrib = sum(s for s, w, ft in sec_scores if ft in ("8-K","8-K/A")) / max(len([x for x in sec_scores if x[2] in ("8-K","8-K/A")]), 1) if sec_scores else 0
    sec_mod = float(sym.get("sec_score_modifier") or 0)
    final   = float(sym.get("final_score") or 0)

    # ── Chart 18: Filing Coverage Heatmap ────────────────────────────────────
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    coverage = {}
    for f in filings_raw:
        ft  = f.get("form_type") or "other"
        pub = f.get("published_at")
        if pub:
            age_days = (now - (pub if pub.tzinfo else pub.replace(tzinfo=timezone.utc))).days
            month_bucket = age_days // 30
            key = f"{ft}:{month_bucket}"
            coverage[key] = coverage.get(key, 0) + 1

    return {
        "symbol":       sym["symbol"],
        "company_name": sym.get("company_name") or sym["symbol"],
        "final_score":  final,
        "sec_modifier": sec_mod,
        "industry":     sym.get("industry") or "",
        "market_cap":   sym.get("market_cap_formatted") or "",
        "last_10k":     sym["last_10k_filed"].isoformat() if sym.get("last_10k_filed") else None,
        "last_10q":     sym["last_10q_filed"].isoformat() if sym.get("last_10q_filed") else None,
        "ai_sector":    sym.get("ai_sector_pick") or "",
        "score_updated": sym["score_updated_at"].isoformat() if sym.get("score_updated_at") else None,
        # 10-K charts
        "tenk_labels":       tenk_labels,
        "tenk_revenue":      tenk_revenue,
        "tenk_growth":       tenk_growth,
        "tenk_gross_margin": tenk_gross_margin,
        "tenk_op_margin":    tenk_op_margin,
        "tenk_net_margin":   tenk_net_margin,
        "tenk_opcf":         tenk_opcf,
        "tenk_fcf":          tenk_fcf,
        "tenk_ni":           tenk_ni,
        "tenk_cash":         tenk_cash,
        "tenk_debt":         tenk_debt,
        "tenk_shares":       tenk_shares,
        "tenk_rd":           tenk_rd,
        "tenk_sga":          tenk_sga,
        # 10-Q charts
        "tenq_labels":   tenq_labels,
        "tenq_revenue":  tenq_revenue,
        "tenq_gross_m":  tenq_gross_m,
        "tenq_op_m":     tenq_op_m,
        "tenq_net_m":    tenq_net_m,
        "tenq_burn":     tenq_burn,
        "tenq_cash":     tenq_cash,
        # 8-K events
        "eightk_events":  eightk_events,
        # Insiders
        "insider_events": insider_events,
        "signals":        [dict(s, filed_at=s["filed_at"].isoformat() if s.get("filed_at") else None)
                           for s in signals],
        # Capital raises
        "dilution_events": dilution_events,
        "nt_count":        len(nt_filings),
        # Score composition
        "score_composition": {
            "news_base":      round(news_base, 4),
            "tenk_contrib":   round(tenk_contrib, 4),
            "tenq_contrib":   round(tenq_contrib, 4),
            "eightk_contrib": round(eightk_contrib, 4),
            "sec_modifier":   round(sec_mod, 4),
            "final":          round(final, 4),
        },
        # Coverage heatmap
        "coverage": coverage,
        "filings_count": {
            "10k": len(tenk_filings), "10q": len(tenq_filings),
            "8k":  len(eightk_filings), "form4": len(form4_filings),
            "13d": len(sc13d_filings), "13g": len(sc13g_filings),
            "s3":  len(s3_filings), "nt": len(nt_filings),
        },
    }


# ── JSON data endpoint ─────────────────────────────────────────────────────────

@sec_router.get("/sec/{symbol}/data")
async def sec_data(symbol: str):
    try:
        data = _build_dashboard_data(symbol.upper())
        return JSONResponse(content=data)
    except Exception as e:
        logger.error(f"[sec_dashboard] data error {symbol}: {e}", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


# ── LLM I/O Debug Endpoint ────────────────────────────────────────────────────

@sec_router.get("/sec/{symbol}/llm-io", response_class=HTMLResponse)
async def sec_llm_io(symbol: str):
    """Show exactly what the LLM saw (stage2_prompt) and output (extracted_facts,
    score_rationale, article_summary) for every SEC filing of this symbol."""
    sym = symbol.upper()
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, form_type, filing_tier, published_at,
                       title, sentiment_score, article_summary,
                       score_rationale, stage2_prompt,
                       extracted_facts, key_events,
                       forecast_until_earnings
                FROM news_articles
                WHERE symbol_id = (SELECT id FROM symbols WHERE symbol=%s AND status=TRUE LIMIT 1)
                  AND form_type IS NOT NULL
                ORDER BY published_at DESC
                LIMIT 50
            """, (sym,))
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    import html as _html

    def _fmt(v):
        if v is None: return "<em style='color:#64748b'>NULL</em>"
        if isinstance(v, (dict, list)): v = json.dumps(v, indent=2)
        return "<pre style='white-space:pre-wrap;word-break:break-word;font-size:11px;color:#94a3b8;margin:0'>" + _html.escape(str(v)) + "</pre>"

    cards = ""
    for r in rows:
        sc = r.get("sentiment_score")
        sc_color = "#4ade80" if sc and float(sc) > 0 else ("#f87171" if sc and float(sc) < 0 else "#64748b")
        sc_str = f"{float(sc):.4f}" if sc is not None else "⏳ Not scored yet"
        pub = r["published_at"].strftime("%Y-%m-%d") if r.get("published_at") else "?"

        # Parse extracted_facts for financial_figures display
        ef_raw = r.get("extracted_facts") or {}
        if isinstance(ef_raw, str):
            try: ef_raw = json.loads(ef_raw)
            except: ef_raw = {}
        figs = ef_raw.get("financial_figures") or []
        figs_html = ""
        if isinstance(figs, list) and figs:
            figs_html = "<table style='width:100%;border-collapse:collapse;font-size:11px'>"
            figs_html += "<tr style='color:#38bdf8'><th style='text-align:left;padding:3px 8px'>metric</th><th style='text-align:left;padding:3px 8px'>value</th><th style='text-align:left;padding:3px 8px'>period</th><th style='text-align:left;padding:3px 8px'>yoy</th></tr>"
            for fig in figs:
                if not isinstance(fig, dict) or "_INSTRUCTION" in fig or "_REQUIRED_METRICS" in fig:
                    continue
                m = _html.escape(str(fig.get("metric") or ""))
                v = _html.escape(str(fig.get("value") or ""))
                p = _html.escape(str(fig.get("period") or ""))
                y = _html.escape(str(fig.get("yoy_change") or fig.get("yoy") or ""))
                figs_html += f"<tr style='border-top:1px solid #1e2535'><td style='padding:3px 8px;color:#c084fc'>{m}</td><td style='padding:3px 8px;color:#4ade80'>{v}</td><td style='padding:3px 8px'>{p}</td><td style='padding:3px 8px'>{y}</td></tr>"
            figs_html += "</table>"
        elif isinstance(figs, dict):
            figs_html = _fmt(figs)

        stage2_prompt_val = r.get("stage2_prompt") or ""
        prompt_html = _fmt(stage2_prompt_val) if stage2_prompt_val else "<em style='color:#f87171'>stage2_prompt not stored — run orchestrator to score this filing</em>"

        cards += f"""
<div style="background:#1e2535;border-radius:10px;margin-bottom:24px;overflow:hidden;border:1px solid #2d3748">
  <div style="background:#0f172a;padding:14px 18px;display:flex;align-items:center;gap:12px">
    <span style="background:#7c3aed;color:#fff;padding:3px 10px;border-radius:12px;font-size:12px;font-weight:700">{_html.escape(r.get("form_type") or "")}</span>
    <span style="color:#94a3b8;font-size:13px">{pub}</span>
    <span style="color:{sc_color};font-weight:700;font-size:14px">{sc_str}</span>
    <span style="color:#e2e8f0;font-size:13px;flex:1">{_html.escape((r.get("title") or "")[:100])}</span>
  </div>

  <!-- Summary row -->
  <div style="padding:12px 18px;border-bottom:1px solid #2d3748">
    <div style="color:#38bdf8;font-size:11px;font-weight:600;margin-bottom:4px">📝 ARTICLE SUMMARY (LLM output)</div>
    <div style="color:#e2e8f0;font-size:13px">{_html.escape(r.get("article_summary") or "—")}</div>
  </div>

  <!-- Score rationale -->
  <div style="padding:12px 18px;border-bottom:1px solid #2d3748">
    <div style="color:#38bdf8;font-size:11px;font-weight:600;margin-bottom:4px">🎯 SCORE RATIONALE (LLM output)</div>
    <div style="color:#e2e8f0;font-size:12px">{_html.escape(r.get("score_rationale") or "—")}</div>
  </div>

  <!-- Financial figures -->
  <div style="padding:12px 18px;border-bottom:1px solid #2d3748">
    <div style="color:#38bdf8;font-size:11px;font-weight:600;margin-bottom:6px">📊 EXTRACTED FINANCIAL FIGURES (feeds charts)</div>
    {figs_html if figs_html else "<em style='color:#f87171;font-size:12px'>⚠ No financial_figures extracted — charts will be empty. Check LLM output below.</em>"}
  </div>

  <!-- Full LLM prompt -->
  <details style="padding:0">
    <summary style="padding:10px 18px;cursor:pointer;color:#fb923c;font-size:12px;font-weight:600;background:#0f172a;list-style:none">
      ▶ 📥 WHAT THE LLM SAW (stage2_prompt — click to expand)
    </summary>
    <div style="padding:12px 18px;border-top:1px solid #2d3748">
      {prompt_html}
    </div>
  </details>

  <!-- Full extracted facts -->
  <details style="padding:0">
    <summary style="padding:10px 18px;cursor:pointer;color:#4ade80;font-size:12px;font-weight:600;background:#0f172a;list-style:none">
      ▶ 📤 FULL LLM OUTPUT — extracted_facts (click to expand)
    </summary>
    <div style="padding:12px 18px;border-top:1px solid #2d3748">
      {_fmt(ef_raw)}
    </div>
  </details>
</div>"""

    scored_count = sum(1 for r in rows if r.get("sentiment_score") is not None)
    pending_count = len(rows) - scored_count

    return HTMLResponse(f"""<!DOCTYPE html>
<html><head>
<title>LLM I/O Debug — {sym}</title>
<meta charset="utf-8">
<style>
  body{{background:#0f172a;color:#e2e8f0;font-family:'Segoe UI',sans-serif;margin:0;padding:20px}}
  a{{color:#38bdf8;text-decoration:none}}
  details>summary::marker{{display:none}}
  details>summary{{outline:none}}
</style>
</head><body>
<div style="max-width:1200px;margin:0 auto">
  <div style="display:flex;align-items:center;gap:16px;margin-bottom:24px">
    <a href="/sec/{sym}">← SEC Dashboard</a>
    <h1 style="margin:0;font-size:22px">🔬 LLM I/O Debug — {sym}</h1>
    <span style="background:#1e2535;padding:4px 12px;border-radius:8px;font-size:13px">
      {len(rows)} SEC filings · <span style="color:#4ade80">{scored_count} scored</span> · <span style="color:#f87171">{pending_count} pending</span>
    </span>
  </div>

  <div style="background:#1e2535;border-radius:10px;padding:16px;margin-bottom:24px;border-left:4px solid #38bdf8">
    <div style="font-size:13px;color:#94a3b8">
      <strong style="color:#e2e8f0">How to read this page:</strong><br>
      • <span style="color:#fb923c">📥 What the LLM saw</span> = the exact JSON payload sent to the LLM (symbol, master_summary, article text)<br>
      • <span style="color:#4ade80">📤 Full LLM output</span> = extracted_facts stored in DB — this is what feeds the charts<br>
      • <span style="color:#c084fc">📊 Financial figures</span> = parsed metrics that go into revenue/margin/cash flow charts<br>
      • If financial_figures is empty or missing → charts will be blank → check the LLM output for the correct keys
    </div>
  </div>

  {cards if cards else '<div style="color:#64748b;text-align:center;padding:60px">No SEC filings found for ' + sym + '</div>'}
</div>
</body></html>""")


# ── HTML Dashboard ─────────────────────────────────────────────────────────────

def _score_color(s: float) -> str:
    if s >= 0.7:   return C_GREEN
    if s >= 0.3:   return "#86efac"
    if s >= 0.05:  return C_TEAL
    if s >= -0.05: return C_SLATE
    if s >= -0.3:  return "#fca5a5"
    return C_RED


@sec_router.get("/sec/{symbol}", response_class=HTMLResponse)
async def sec_dashboard(symbol: str):
    sym = symbol.upper()
    try:
        data = _build_dashboard_data(sym)
    except Exception as e:
        return HTMLResponse(f"<h1>Error: {e}</h1>", status_code=500)

    if "error" in data:
        return HTMLResponse(f"<h1>{data['error']}</h1>", status_code=404)

    fs       = data["final_score"]
    fs_color = _score_color(fs)
    sec_mod  = data["sec_modifier"]
    sm_color = C_GREEN if sec_mod >= 0 else C_RED

    def _fmt_date(iso: Optional[str]) -> str:
        if not iso:
            return "N/A"
        try:
            return datetime.fromisoformat(iso).strftime("%b %d, %Y")
        except Exception:
            return iso[:10]

    signals_html = ""
    for sig in data["signals"][:8]:
        mod = float(sig.get("score_modifier") or 0)
        mc  = C_GREEN if mod >= 0 else C_RED
        signals_html += (
            f'<div style="display:flex;justify-content:space-between;align-items:center;'
            f'padding:6px 10px;margin-bottom:4px;background:#0f172a;border-radius:6px;'
            f'border-left:3px solid {mc}">'
            f'<span style="font-size:12px;color:#94a3b8">{sig.get("signal_text","")[:70]}</span>'
            f'<span style="font-size:13px;font-weight:700;color:{mc}">{mod:+.3f}</span>'
            f'</div>'
        )

    filing_counts = data["filings_count"]

    data_json = json.dumps(data, default=str)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{sym} — SEC Intelligence Dashboard | TradeIntel</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.0.1/dist/chartjs-plugin-annotation.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: {C_BG}; color: #e2e8f0; font-family: 'Inter', system-ui, sans-serif; min-height: 100vh; }}
  .header {{ background: #0a0f1e; border-bottom: 1px solid #1e2535; padding: 18px 32px; display: flex; align-items: center; gap: 24px; }}
  .back-btn {{ color: {C_BLUE}; text-decoration: none; font-size: 13px; padding: 6px 12px; border: 1px solid {C_BLUE}33; border-radius: 6px; }}
  .back-btn:hover {{ background: {C_BLUE}11; }}
  .symbol-title {{ font-size: 26px; font-weight: 900; color: #f1f5f9; }}
  .company-name {{ font-size: 13px; color: {C_SLATE}; margin-top: 2px; }}
  .score-badge {{ margin-left: auto; text-align: right; }}
  .score-val {{ font-size: 32px; font-weight: 900; color: {fs_color}; }}
  .score-label {{ font-size: 11px; color: {C_SLATE}; text-transform: uppercase; letter-spacing: 1px; }}
  .meta-bar {{ background: #0d1424; border-bottom: 1px solid #1e2535; padding: 10px 32px; display: flex; gap: 32px; flex-wrap: wrap; }}
  .meta-item {{ font-size: 12px; color: {C_SLATE}; }}
  .meta-item span {{ color: #cbd5e1; font-weight: 600; }}
  .tabs {{ display: flex; gap: 4px; padding: 16px 32px 0; border-bottom: 1px solid #1e2535; background: #0d1424; }}
  .tab {{ padding: 8px 18px; font-size: 13px; color: {C_SLATE}; cursor: pointer; border-radius: 6px 6px 0 0; border: 1px solid transparent; border-bottom: none; transition: all 0.15s; }}
  .tab:hover {{ color: #e2e8f0; background: #1e2535; }}
  .tab.active {{ color: {C_BLUE}; background: {C_BG}; border-color: #1e2535; border-bottom-color: {C_BG}; font-weight: 600; }}
  .tab-pane {{ display: none; padding: 24px 32px; }}
  .tab-pane.active {{ display: block; }}
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }}
  .chart-grid-3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin-bottom: 20px; }}
  .chart-card {{ background: {C_CARD}; border: 1px solid #1e3a5f22; border-radius: 12px; padding: 18px; }}
  .chart-card.wide {{ grid-column: span 2; }}
  .chart-card.full {{ grid-column: span 3; }}
  .chart-title {{ font-size: 12px; font-weight: 700; color: {C_SLATE}; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 14px; }}
  canvas {{ max-height: 220px; }}
  canvas.tall {{ max-height: 280px; }}
  canvas.timeline {{ max-height: 180px; }}
  .signal-panel {{ background: {C_CARD}; border-radius: 12px; padding: 18px; margin-bottom: 20px; }}
  .signal-title {{ font-size: 12px; font-weight: 700; color: {C_SLATE}; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 12px; }}
  .health-pill {{ display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 11px; font-weight: 700; }}
  .summary-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }}
  .stat-card {{ background: {C_CARD}; border-radius: 10px; padding: 14px; text-align: center; }}
  .stat-val {{ font-size: 22px; font-weight: 900; }}
  .stat-label {{ font-size: 11px; color: {C_SLATE}; margin-top: 4px; }}
  .waterfall-bar {{ display: flex; align-items: center; margin-bottom: 8px; gap: 10px; }}
  .wf-label {{ width: 140px; font-size: 12px; color: {C_SLATE}; text-align: right; flex-shrink: 0; }}
  .wf-bar-wrap {{ flex: 1; height: 22px; background: #0f172a; border-radius: 4px; overflow: hidden; }}
  .wf-bar {{ height: 100%; border-radius: 4px; transition: width 0.6s; }}
  .wf-val {{ width: 60px; font-size: 13px; font-weight: 700; text-align: left; flex-shrink: 0; }}
  .heatmap-row {{ display: flex; align-items: center; margin-bottom: 4px; gap: 4px; }}
  .heatmap-label {{ width: 80px; font-size: 11px; color: {C_SLATE}; text-align: right; flex-shrink: 0; }}
  .heatmap-cell {{ width: 18px; height: 18px; border-radius: 3px; flex-shrink: 0; }}
  .event-log {{ font-size: 12px; }}
  .event-row {{ display: grid; grid-template-columns: 90px 60px 1fr 60px; gap: 8px; padding: 6px 0; border-bottom: 1px solid #1e2535; align-items: center; }}
  .event-row:last-child {{ border-bottom: none; }}
  .no-data {{ color: {C_SLATE}; font-size: 13px; text-align: center; padding: 40px; }}
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <a href="/" class="back-btn">← Leaderboard</a>
  <div>
    <div class="symbol-title">{sym}</div>
    <div class="company-name">{data.get("company_name",sym)} &nbsp;·&nbsp; {data.get("industry","")}</div>
  </div>
  <div style="display:flex;gap:10px;align-items:center">
    <a href="/sec/{sym}/llm-io" target="_blank"
       style="display:inline-flex;align-items:center;gap:6px;background:#0c4a6e;color:#7dd3fc;
              font-size:12px;font-weight:700;padding:7px 14px;border-radius:7px;text-decoration:none;
              border:1px solid #0369a1">
      🔬 LLM I/O Debug
    </a>
  </div>
  <div class="score-badge">
    <div class="score-val">{fs:+.4f}</div>
    <div class="score-label">Final Score</div>
    <div style="font-size:12px;color:{sm_color};margin-top:4px">SEC modifier: {sec_mod:+.4f}</div>
  </div>
</div>

<!-- Meta bar -->
<div class="meta-bar">
  <div class="meta-item">Market Cap: <span>{data.get("market_cap","N/A")}</span></div>
  <div class="meta-item">Sector: <span>{data.get("ai_sector","—")}</span></div>
  <div class="meta-item">Last 10-K: <span>{_fmt_date(data.get("last_10k"))}</span></div>
  <div class="meta-item">Last 10-Q: <span>{_fmt_date(data.get("last_10q"))}</span></div>
  <div class="meta-item">Score updated: <span>{_fmt_date(data.get("score_updated"))}</span></div>
  <div class="meta-item">10-K filings: <span>{filing_counts["10k"]}</span> &nbsp;|
    10-Q: <span>{filing_counts["10q"]}</span> &nbsp;|
    8-K: <span>{filing_counts["8k"]}</span> &nbsp;|
    Form4: <span>{filing_counts["form4"]}</span> &nbsp;|
    13D: <span>{filing_counts["13d"]}</span> &nbsp;|
    13G: <span>{filing_counts["13g"]}</span>
  </div>
</div>

<!-- Tabs -->
<div class="tabs">
  <div class="tab active" onclick="switchTab('t10k',this)">📋 10-K Annual</div>
  <div class="tab" onclick="switchTab('t10q',this)">📋 10-Q Quarterly</div>
  <div class="tab" onclick="switchTab('t8k',this)">📋 8-K Events</div>
  <div class="tab" onclick="switchTab('tins',this)">👤 Insiders</div>
  <div class="tab" onclick="switchTab('tcap',this)">⚠️ Capital Raises</div>
  <div class="tab" onclick="switchTab('tscore',this)">🎯 Score Composition</div>
</div>

<!-- ═══════════ TAB: 10-K Annual ═══════════ -->
<div id="t10k" class="tab-pane active">
  <div class="chart-grid">
    <div class="chart-card">
      <div class="chart-title">Revenue & YoY Growth (5-Year)</div>
      <canvas id="c1"></canvas>
    </div>
    <div class="chart-card">
      <div class="chart-title">Margin Waterfall — Gross / Operating / Net %</div>
      <canvas id="c2"></canvas>
    </div>
    <div class="chart-card">
      <div class="chart-title">Cash Flow Quality — OpCF vs FCF vs Net Income ($M)</div>
      <canvas id="c3"></canvas>
    </div>
    <div class="chart-card">
      <div class="chart-title">Balance Sheet — Cash vs Total Debt ($M)</div>
      <canvas id="c4"></canvas>
    </div>
    <div class="chart-card">
      <div class="chart-title">Shares Outstanding ($M) — Dilution Trend</div>
      <canvas id="c5"></canvas>
    </div>
    <div class="chart-card">
      <div class="chart-title">R&D + SGA as % of Revenue</div>
      <canvas id="c6"></canvas>
    </div>
  </div>
</div>

<!-- ═══════════ TAB: 10-Q Quarterly ═══════════ -->
<div id="t10q" class="tab-pane">
  <div class="chart-grid">
    <div class="chart-card">
      <div class="chart-title">Quarterly Revenue ($M) — Last 5 Quarters</div>
      <canvas id="c7"></canvas>
    </div>
    <div class="chart-card">
      <div class="chart-title">Quarterly Margin Trend %</div>
      <canvas id="c8"></canvas>
    </div>
    <div class="chart-card">
      <div class="chart-title">Cash Position ($M) — Liquidity Runway</div>
      <canvas id="c9"></canvas>
    </div>
    <div class="chart-card">
      <div class="chart-title">Net Cash Burn per Quarter ($M)</div>
      <canvas id="c10"></canvas>
    </div>
  </div>
</div>

<!-- ═══════════ TAB: 8-K Events ═══════════ -->
<div id="t8k" class="tab-pane">
  <div class="chart-grid">
    <div class="chart-card wide">
      <div class="chart-title">8-K Event Sentiment Timeline — Last 24 Months</div>
      <canvas id="c11" class="tall"></canvas>
    </div>
  </div>
  <div class="chart-grid">
    <div class="chart-card">
      <div class="chart-title">8-K Sentiment Distribution</div>
      <canvas id="c12"></canvas>
    </div>
    <div class="chart-card">
      <div class="chart-title">Recent 8-K Events</div>
      <div class="event-log" id="eightk-log"></div>
    </div>
  </div>
</div>

<!-- ═══════════ TAB: Insiders ═══════════ -->
<div id="tins" class="tab-pane">
  <div class="chart-grid">
    <div class="chart-card wide">
      <div class="chart-title">Insider Transaction Timeline — Last 12 Months</div>
      <canvas id="c13" class="tall"></canvas>
    </div>
  </div>
  <div class="signal-panel">
    <div class="signal-title">Active SEC Signals</div>
    {signals_html if signals_html else '<div class="no-data">No active signals</div>'}
  </div>
</div>

<!-- ═══════════ TAB: Capital Raises ═══════════ -->
<div id="tcap" class="tab-pane">
  <div class="chart-grid">
    <div class="chart-card wide">
      <div class="chart-title">Dilution Events — S-3 / 424B Sentiment (Last 36 Months)</div>
      <canvas id="c16" class="tall"></canvas>
    </div>
  </div>
  <div class="signal-panel">
    <div class="signal-title">NT Late Filing History ({len(data.get('signals',[]))} signals tracked)</div>
    <div id="nt-panel"></div>
  </div>
</div>

<!-- ═══════════ TAB: Score Composition ═══════════ -->
<div id="tscore" class="tab-pane">
  <div class="chart-grid">
    <div class="chart-card">
      <div class="chart-title">Score Waterfall — How final_score is Built</div>
      <div id="waterfall"></div>
    </div>
    <div class="chart-card">
      <div class="chart-title">Filing Coverage Heatmap — Last 36 Months</div>
      <div id="heatmap"></div>
    </div>
  </div>
</div>

<script>
// ── Data injected from server ─────────────────────────────────────────────────
const D = {data_json};

// ── Tab switching ─────────────────────────────────────────────────────────────
function switchTab(id, el) {{
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  el.classList.add('active');
}}

// ── Chart defaults ────────────────────────────────────────────────────────────
Chart.defaults.color = '{C_TEXT}';
Chart.defaults.borderColor = '{C_GRID}';
Chart.defaults.font.family = "'Inter', system-ui, sans-serif";
Chart.defaults.font.size = 11;

const NULL_SKIP = {{ spanGaps: true }};

function mkChart(id, config) {{
  const ctx = document.getElementById(id);
  if (!ctx) return null;
  // Check if all data arrays are empty / all-null
  const datasets = (config.data || {{}}).datasets || [];
  const hasData = datasets.some(ds => (ds.data || []).some(v => v !== null && v !== undefined));
  if (!hasData) {{
    const wrap = ctx.parentElement;
    ctx.style.display = 'none';
    const msg = document.createElement('div');
    msg.style.cssText = 'padding:40px;text-align:center;color:#475569;font-size:12px';
    // Check if filings exist but facts not extracted (different from "no filings at all")
    const hasFilings = D.filings_count && (D.filings_count['10k'] > 0 || D.filings_count['10q'] > 0);
    const hasScored = D.score_composition && (D.score_composition.tenk_contrib !== 0 || D.score_composition.tenq_contrib !== 0);
    if (hasFilings && hasScored) {{
      msg.innerHTML = '<div style="font-size:24px;margin-bottom:8px">⚠️</div><div style="color:#fcd34d">Filings scored but no financial figures extracted</div><div style="margin-top:8px">Check <a href="/sec/' + D.symbol + '/llm-io" target="_blank" style="color:#38bdf8">🔬 LLM I/O Debug</a> to see what the LLM output</div>';
    }} else if (hasFilings) {{
      msg.innerHTML = '<div style="font-size:24px;margin-bottom:8px">⏳</div>SEC filings ingested but not yet scored<br>Run <code style="color:#fcd34d">python orchestrator.py</code> to score them';
    }} else {{
      msg.innerHTML = '<div style="font-size:24px;margin-bottom:8px">📭</div>No SEC filings ingested yet<br>Run <code style="color:#fcd34d">python scripts/edgar_backfill.py --symbol ' + D.symbol + '</code>';
    }}
    wrap.appendChild(msg);
    return null;
  }}
  return new Chart(ctx, config);
}}

function barColors(data, pos='{C_GREEN}', neg='{C_RED}', neutral='{C_SLATE}') {{
  return (data || []).map(v => v === null ? '#1e2535' : v > 0 ? pos : v < 0 ? neg : neutral);
}}

// ── C1: Revenue + Growth ──────────────────────────────────────────────────────
mkChart('c1', {{
  type: 'bar',
  data: {{
    labels: D.tenk_labels,
    datasets: [
      {{ label: 'Revenue ($M)', data: D.tenk_revenue, backgroundColor: '{C_BLUE}88',
         borderColor: '{C_BLUE}', borderWidth: 1, yAxisID: 'y' }},
      {{ label: 'YoY Growth %', data: D.tenk_growth, type: 'line',
         borderColor: '{C_GREEN}', backgroundColor: 'transparent',
         pointBackgroundColor: '{C_GREEN}', tension: 0.3, yAxisID: 'y2' }},
    ]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => '$'+v+'M' }} }},
               y2: {{ position: 'right', grid: {{ display: false }}, ticks: {{ callback: v => v+'%' }} }} }} }}
}});

// ── C2: Margin Waterfall ──────────────────────────────────────────────────────
mkChart('c2', {{
  type: 'line', ...NULL_SKIP,
  data: {{
    labels: D.tenk_labels,
    datasets: [
      {{ label: 'Gross %', data: D.tenk_gross_margin, borderColor: '{C_GREEN}', fill: false, tension: 0.3 }},
      {{ label: 'Operating %', data: D.tenk_op_margin, borderColor: '{C_BLUE}', fill: false, tension: 0.3 }},
      {{ label: 'Net %', data: D.tenk_net_margin, borderColor: '{C_ORANGE}', fill: false, tension: 0.3 }},
    ]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }},
    annotation: {{ annotations: {{ zero: {{ type:'line', yMin:0, yMax:0, borderColor:'{C_RED}55', borderWidth:1, borderDash:[4,4] }} }} }} }},
    scales: {{ y: {{ grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => v+'%' }} }} }} }}
}});

// ── C3: Cash Flow Quality ─────────────────────────────────────────────────────
mkChart('c3', {{
  type: 'bar',
  data: {{
    labels: D.tenk_labels,
    datasets: [
      {{ label: 'OpCF', data: D.tenk_opcf, backgroundColor: '{C_TEAL}88', borderColor: '{C_TEAL}', borderWidth: 1 }},
      {{ label: 'FCF',  data: D.tenk_fcf,  backgroundColor: '{C_GREEN}88', borderColor: '{C_GREEN}', borderWidth: 1 }},
      {{ label: 'Net Income', data: D.tenk_ni, backgroundColor: '{C_BLUE}55', borderColor: '{C_BLUE}', borderWidth: 1 }},
    ]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => '$'+v+'M' }} }} }} }}
}});

// ── C4: Balance Sheet ─────────────────────────────────────────────────────────
mkChart('c4', {{
  type: 'bar',
  data: {{
    labels: D.tenk_labels,
    datasets: [
      {{ label: 'Cash ($M)', data: D.tenk_cash, backgroundColor: '{C_GREEN}88', borderColor: '{C_GREEN}', borderWidth: 1 }},
      {{ label: 'Total Debt ($M)', data: D.tenk_debt, backgroundColor: '{C_RED}88', borderColor: '{C_RED}', borderWidth: 1 }},
    ]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => '$'+v+'M' }} }} }} }}
}});

// ── C5: Shares Outstanding ────────────────────────────────────────────────────
mkChart('c5', {{
  type: 'bar',
  data: {{
    labels: D.tenk_labels,
    datasets: [{{ label: 'Shares (M)', data: D.tenk_shares, backgroundColor: D.tenk_shares.map((v,i,a) => {{
      if (i===0 || !a[i-1] || v===null) return '{C_BLUE}88';
      return v > a[i-1] ? '{C_ORANGE}88' : '{C_GREEN}88';
    }}), borderWidth: 1 }}]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ grid: {{ color: '{C_GRID}' }} }} }} }}
}});

// ── C6: R&D + SGA ────────────────────────────────────────────────────────────
mkChart('c6', {{
  type: 'bar',
  data: {{
    labels: D.tenk_labels,
    datasets: [
      {{ label: 'R&D ($M)', data: D.tenk_rd, backgroundColor: '{C_BLUE}88', borderColor: '{C_BLUE}', borderWidth: 1, stack: 'a' }},
      {{ label: 'SG&A ($M)', data: D.tenk_sga, backgroundColor: '{C_PURPLE}88', borderColor: '{C_PURPLE}', borderWidth: 1, stack: 'a' }},
    ]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ stacked: true, grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => '$'+v+'M' }} }} }} }}
}});

// ── C7: Quarterly Revenue ─────────────────────────────────────────────────────
mkChart('c7', {{
  type: 'bar',
  data: {{
    labels: D.tenq_labels,
    datasets: [{{ label: 'Revenue ($M)', data: D.tenq_revenue,
      backgroundColor: D.tenq_revenue.map((v,i,a) => i===0||!a[i-1]||v===null?'{C_BLUE}88':v>a[i-1]?'{C_GREEN}88':'{C_ORANGE}88'),
      borderWidth: 1 }}]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => '$'+v+'M' }} }} }} }}
}});

// ── C8: Quarterly Margins ─────────────────────────────────────────────────────
mkChart('c8', {{
  type: 'line', ...NULL_SKIP,
  data: {{
    labels: D.tenq_labels,
    datasets: [
      {{ label: 'Gross %',  data: D.tenq_gross_m, borderColor: '{C_GREEN}', fill: false, tension: 0.3 }},
      {{ label: 'Op %',     data: D.tenq_op_m,    borderColor: '{C_BLUE}',  fill: false, tension: 0.3 }},
      {{ label: 'Net %',    data: D.tenq_net_m,   borderColor: '{C_ORANGE}', fill: false, tension: 0.3 }},
    ]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => v+'%' }} }} }} }}
}});

// ── C9: Cash Position / Liquidity ────────────────────────────────────────────
mkChart('c9', {{
  type: 'bar',
  data: {{
    labels: D.tenq_labels,
    datasets: [{{ label: 'Cash ($M)', data: D.tenq_cash,
      backgroundColor: D.tenq_cash.map(v => !v ? '{C_SLATE}55' : v > 50 ? '{C_GREEN}88' : v > 10 ? '{C_YELLOW}88' : '{C_RED}88'),
      borderWidth: 1 }}]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => '$'+v+'M' }} }} }} }}
}});

// ── C10: Cash Burn ────────────────────────────────────────────────────────────
mkChart('c10', {{
  type: 'bar',
  data: {{
    labels: D.tenq_labels,
    datasets: [{{ label: 'Net CF ($M)', data: D.tenq_burn,
      backgroundColor: D.tenq_burn.map(v => !v?'{C_SLATE}55':v>=0?'{C_GREEN}88':'{C_RED}88'),
      borderWidth: 1 }}]
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }},
    annotation: {{ annotations: {{ zero: {{ type:'line', yMin:0, yMax:0, borderColor:'{C_SLATE}88', borderWidth:1 }} }} }} }},
    scales: {{ y: {{ grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => '$'+v+'M' }} }} }} }}
}});

// ── C11: 8-K Event Timeline (scatter) ────────────────────────────────────────
(function() {{
  const events = D.eightk_events || [];
  if (!events.length) {{
    const wrap = document.getElementById('c11')?.parentElement;
    if (wrap) {{ document.getElementById('c11').style.display='none'; wrap.innerHTML += '<div style="padding:40px;text-align:center;color:#475569;font-size:12px"><div style="font-size:24px;margin-bottom:8px">📭</div>No 8-K events scored yet — run <code style="color:#fcd34d">orchestrator.py</code></div>'; }}
    return;
  }}
  const pts = events.map(e => ({{
    x: new Date(e.date).getTime(),
    y: e.score,
    label: e.title,
    summary: e.summary,
  }}));
  mkChart('c11', {{
    type: 'scatter',
    data: {{ datasets: [{{ label: '8-K Events', data: pts,
      backgroundColor: pts.map(p => p.y >= 0.3 ? '{C_GREEN}cc' : p.y <= -0.3 ? '{C_RED}cc' : '{C_SLATE}cc'),
      pointRadius: pts.map(p => Math.max(4, Math.min(14, Math.abs(p.y)*14))),
      pointHoverRadius: 16,
    }}] }},
    options: {{ responsive: true,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{ callbacks: {{ label: ctx => [ctx.raw.label, 'Score: '+ctx.raw.y.toFixed(3), ctx.raw.summary||''].filter(Boolean) }} }}
      }},
      scales: {{
        x: {{ type: 'linear', grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => new Date(v).toLocaleDateString('en-US',{{month:'short',year:'2-digit'}}) }} }},
        y: {{ min: -1, max: 1, grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => v.toFixed(1) }} }}
      }}
    }}
  }});
}})();

// ── C12: 8-K Sentiment Histogram ─────────────────────────────────────────────
(function() {{
  const events = D.eightk_events || [];
  const buckets = {{}};
  for (let b = -10; b <= 10; b++) {{ buckets[b/10] = 0; }}
  events.forEach(e => {{
    const b = Math.round(e.score * 10) / 10;
    const k = Math.max(-1, Math.min(1, b));
    buckets[Math.round(k*10)/10] = (buckets[Math.round(k*10)/10]||0) + 1;
  }});
  const labels = Object.keys(buckets).map(Number).sort((a,b)=>a-b);
  const vals   = labels.map(l => buckets[l]);
  mkChart('c12', {{
    type: 'bar',
    data: {{ labels: labels.map(l=>l.toFixed(1)), datasets: [{{ label: 'Count', data: vals,
      backgroundColor: labels.map(l => l >= 0.3 ? '{C_GREEN}aa' : l <= -0.3 ? '{C_RED}aa' : '{C_SLATE}aa'),
      borderWidth: 0 }}] }},
    options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
      scales: {{ x: {{ grid: {{ color: '{C_GRID}' }} }}, y: {{ grid: {{ color: '{C_GRID}' }}, ticks: {{ stepSize: 1 }} }} }} }}
  }});
}})();

// ── Event Log (8-K) ───────────────────────────────────────────────────────────
(function() {{
  const el = document.getElementById('eightk-log');
  const events = (D.eightk_events || []).slice().reverse().slice(0, 12);
  if (!events.length) {{ el.innerHTML = '<div class="no-data">No 8-K events scored yet</div>'; return; }}
  const hdr = '<div class="event-row" style="color:{C_SLATE};font-size:11px;text-transform:uppercase"><span>Date</span><span>Score</span><span>Title</span><span>Form</span></div>';
  el.innerHTML = hdr + events.map(e => {{
    const sc = e.score || 0;
    const col = sc >= 0.3 ? '{C_GREEN}' : sc <= -0.3 ? '{C_RED}' : '{C_SLATE}';
    const dt  = e.date ? new Date(e.date).toLocaleDateString('en-US',{{month:'short',day:'numeric',year:'2-digit'}}) : '?';
    return `<div class="event-row">
      <span style="color:{C_SLATE}">${{dt}}</span>
      <span style="color:${{col}};font-weight:700">${{sc.toFixed(3)}}</span>
      <span style="color:#cbd5e1">${{(e.title||'').slice(0,60)}}</span>
      <span style="color:{C_SLATE}">${{e.form||''}}</span>
    </div>`;
  }}).join('');
}})();

// ── C13: Insider Timeline ─────────────────────────────────────────────────────
(function() {{
  const events = D.insider_events || [];
  if (!events.length) {{
    const wrap = document.getElementById('c13')?.parentElement;
    if (wrap) {{ document.getElementById('c13').style.display='none'; wrap.innerHTML += '<div style="padding:40px;text-align:center;color:#475569;font-size:12px"><div style="font-size:24px;margin-bottom:8px">👤</div>No insider transactions on record</div>'; }}
    return;
  }}
  const buys  = events.filter(e => e.type === 'insider_buy').map(e => ({{ x: new Date(e.date).getTime(), y: e.value/1000, r: Math.max(4, Math.min(16, e.value/100000)), label: e.text }}));
  const sells = events.filter(e => e.type === 'insider_sell').map(e => ({{ x: new Date(e.date).getTime(), y: -e.value/1000, r: Math.max(4, Math.min(16, e.value/100000)), label: e.text }}));
  mkChart('c13', {{
    type: 'bubble',
    data: {{ datasets: [
      {{ label: 'Buy', data: buys,  backgroundColor: '{C_GREEN}99', borderColor: '{C_GREEN}' }},
      {{ label: 'Sell', data: sells, backgroundColor: '{C_RED}99',  borderColor: '{C_RED}' }},
    ] }},
    options: {{ responsive: true,
      plugins: {{ legend: {{ position: 'bottom' }}, tooltip: {{ callbacks: {{ label: ctx => ctx.raw.label }} }} }},
      scales: {{
        x: {{ type: 'linear', grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => new Date(v).toLocaleDateString('en-US',{{month:'short',year:'2-digit'}}) }} }},
        y: {{ grid: {{ color: '{C_GRID}' }}, ticks: {{ callback: v => '$'+v+'k' }} }}
      }}
    }}
  }});
}})();

// ── C16: Dilution Events ──────────────────────────────────────────────────────
(function() {{
  const events = D.dilution_events || [];
  if (!events.length) {{
    const wrap = document.getElementById('c16')?.parentElement;
    if (wrap) {{ document.getElementById('c16').style.display='none'; wrap.innerHTML += '<div style="padding:40px;text-align:center;color:#475569;font-size:12px"><div style="font-size:24px;margin-bottom:8px">⚠️</div>No S-3 / 424B capital raise events found</div>'; }}
    return;
  }}
  mkChart('c16', {{
    type: 'bar',
    data: {{ labels: events.map(e => e.date ? new Date(e.date).toLocaleDateString('en-US',{{month:'short',year:'2-digit'}}) : '?'),
      datasets: [{{ label: 'Dilution Score', data: events.map(e=>e.score||0),
        backgroundColor: events.map(e => (e.score||0) < -0.6 ? '{C_RED}cc' : (e.score||0) < -0.3 ? '{C_ORANGE}cc' : '{C_YELLOW}cc'),
        borderWidth: 0 }}]
    }},
    options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
      scales: {{ y: {{ min: -1, max: 0.1, grid: {{ color: '{C_GRID}' }} }} }} }}
  }});
}})();

// ── NT Panel ─────────────────────────────────────────────────────────────────
(function() {{
  const el = document.getElementById('nt-panel');
  const nt = (D.signals||[]).filter(s => s.signal_type && s.signal_type.includes('nt_delay'));
  if (!nt.length) {{ el.innerHTML = '<div class="no-data">No NT late filings on record</div>'; return; }}
  el.innerHTML = nt.slice(0,6).map(s => `
    <div style="padding:8px 10px;margin-bottom:4px;background:#0f172a;border-radius:6px;border-left:3px solid {C_RED}">
      <span style="font-size:12px;color:#94a3b8">${{s.signal_text||''}}</span>
      <span style="float:right;font-size:12px;font-weight:700;color:{C_RED}">${{(s.score_modifier||0).toFixed(3)}}</span>
    </div>`).join('');
}})();

// ── Waterfall ─────────────────────────────────────────────────────────────────
(function() {{
  const sc = D.score_composition || {{}};
  const el = document.getElementById('waterfall');
  const items = [
    ['News baseline',   sc.news_base     || 0, '{C_BLUE}'],
    ['+ 10-K (×2.5w)',  sc.tenk_contrib  || 0, '{C_GREEN}'],
    ['+ 10-Q (×2.0w)',  sc.tenq_contrib  || 0, '{C_TEAL}'],
    ['+ 8-K (×1.5w)',   sc.eightk_contrib|| 0, '{C_BLUE}'],
    ['+ SEC modifier',  sc.sec_modifier  || 0, sc.sec_modifier >= 0 ? '{C_GREEN}' : '{C_RED}'],
    ['= Final Score',   sc.final         || 0, Math.abs(sc.final||0) > 0.5 ? '{C_GREEN}' : '{C_ORANGE}'],
  ];
  const maxAbs = Math.max(...items.map(i => Math.abs(i[1])), 0.01);
  el.innerHTML = items.map(([label, val, col]) => {{
    const pct = Math.abs(val) / maxAbs * 100;
    const dir = val >= 0 ? 'right' : 'left';
    return `<div class="waterfall-bar">
      <div class="wf-label">${{label}}</div>
      <div class="wf-bar-wrap"><div class="wf-bar" style="width:${{pct.toFixed(1)}}%;background:${{col}};"></div></div>
      <div class="wf-val" style="color:${{col}}">${{val >= 0 ? '+' : ''}}${{val.toFixed(4)}}</div>
    </div>`;
  }}).join('');
}})();

// ── Heatmap ───────────────────────────────────────────────────────────────────
(function() {{
  const el = document.getElementById('heatmap');
  const cov = D.coverage || {{}};
  const rows = [
    ['10-K',  '#10-K:'],
    ['10-Q',  '#10-Q:'],
    ['8-K',   '#8-K:'],
    ['S-3',   '#S-3:'],
    ['Form4', '#4:'],
    ['13D',   '#SC 13D:'],
    ['13G',   '#SC 13G:'],
  ];
  const months = Array.from({{length:36}},(_,i)=>i);
  el.innerHTML = rows.map(([label, prefix]) => {{
    const cells = months.map(m => {{
      const key = Object.keys(cov).find(k => k.startsWith(prefix.slice(1)) && k.endsWith(':'+m));
      const cnt = key ? cov[key] : 0;
      const bg  = cnt === 0 ? '#1e2535' : cnt === 1 ? '#1e3a5f' : cnt < 4 ? '#2563eb88' : '{C_GREEN}';
      return `<div class="heatmap-cell" title="${{label}} ${{36-m}}mo ago: ${{cnt}} filing(s)" style="background:${{bg}}"></div>`;
    }}).join('');
    return `<div class="heatmap-row"><div class="heatmap-label">${{label}}</div>${{cells}}</div>`;
  }}).join('');
}})();
</script>
</body>
</html>"""

    return HTMLResponse(html)
