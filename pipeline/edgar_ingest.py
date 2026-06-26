"""
pipeline/edgar_ingest.py — SEC EDGAR Multi-Form Filing Ingestion
=================================================================
Fetches SEC filings across three tiers for all active symbols:

  Tier 1 — Core Financials (ALL symbols):
    10-K  (5-year window, 5 filings)   — annual report
    10-Q  (5-quarter window, 5 filings) — quarterly report
    8-K   (24-month window, 40 filings) — current report (RSS-less symbols)

  Tier 2 — Capital & Dilution (ALL symbols):
    S-3 / S-3/A  (36-month, 10 filings) — shelf registration
    424B1/3/4/5  (36-month, 10 filings) — prospectus supplements
    NT 10-K / NT 10-Q (24-month, 6)     — late-filing notices

  Tier 3 — Ownership & Insiders (ALL symbols):
    Form 4       (12-month, 50 filings) — insider transactions
    SC 13D/A     (36-month, 10 filings) — activist investor
    SC 13G/A     (24-month, 10 filings) — institutional holdings

Smart section extraction (Option A):
  Each form type has a dedicated extractor that pulls only the
  signal-rich sections, keeping LLM context tight and accurate.

Rate limit: SEC requests max 10 req/sec. We stay at ~5 req/sec (0.2s delay).
User-Agent MUST be "name email" format — SEC blocks generic agents.

Run modes:
    edgar_ingest.run()                          # incremental (new filings only)
    edgar_ingest.run(forms_only=["10-K","10-Q"]) # specific forms
    edgar_ingest.run(tier_filter=[1])           # specific tier
"""

import hashlib
import json
import logging
import re
import time
import random
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
import sys

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from dateutil import parser as dateutil_parser
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

sys.path.insert(0, ".")
from db.connection import get_connection

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
EDGAR_USER_AGENT = "TradeIntel research@tradeintel.com"
EDGAR_HEADERS    = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
EDGAR_DELAY_MIN  = 0.15
EDGAR_DELAY_MAX  = 0.35
EDGAR_TIMEOUT    = 25
BULK_MAP_CACHE   = Path("scratch/edgar_cik_map.json")
BULK_MAP_TTL_H   = 24

# ── Filing Registry ────────────────────────────────────────────────────────────
# all_symbols=True  → fetch for every active symbol
# all_symbols=False → only symbols without an RSS/atom feed (8-K fallback)
FILING_REGISTRY = {
    # ── Tier 1: Core Financials ───────────────────────────────────────────────
    "10-K":     {"tier": 1, "weight": 2.5, "max_per_run": 5,  "all_symbols": True,
                 "lookback_months": 60,   "desc": "Annual report"},
    "10-K/A":   {"tier": 1, "weight": 2.5, "max_per_run": 5,  "all_symbols": True,
                 "lookback_months": 60,   "desc": "Annual report amendment"},
    "10-Q":     {"tier": 1, "weight": 2.0, "max_per_run": 5,  "all_symbols": True,
                 "lookback_months": 15,   "desc": "Quarterly report"},
    "10-Q/A":   {"tier": 1, "weight": 2.0, "max_per_run": 5,  "all_symbols": True,
                 "lookback_months": 15,   "desc": "Quarterly report amendment"},
    "8-K":      {"tier": 1, "weight": 1.5, "max_per_run": 40, "all_symbols": False,
                 "lookback_months": 24,   "desc": "Current report"},
    "8-K/A":    {"tier": 1, "weight": 1.5, "max_per_run": 10, "all_symbols": False,
                 "lookback_months": 24,   "desc": "Current report amendment"},
    # ── Tier 2: Capital & Dilution ────────────────────────────────────────────
    "S-3":      {"tier": 2, "weight": 1.8, "max_per_run": 10, "all_symbols": True,
                 "lookback_months": 36,   "desc": "Shelf registration"},
    "S-3/A":    {"tier": 2, "weight": 1.8, "max_per_run": 10, "all_symbols": True,
                 "lookback_months": 36,   "desc": "Shelf registration amendment"},
    "424B1":    {"tier": 2, "weight": 1.8, "max_per_run": 10, "all_symbols": True,
                 "lookback_months": 36,   "desc": "Prospectus supplement"},
    "424B3":    {"tier": 2, "weight": 1.8, "max_per_run": 10, "all_symbols": True,
                 "lookback_months": 36,   "desc": "Prospectus supplement"},
    "424B4":    {"tier": 2, "weight": 1.8, "max_per_run": 10, "all_symbols": True,
                 "lookback_months": 36,   "desc": "Prospectus supplement"},
    "424B5":    {"tier": 2, "weight": 1.8, "max_per_run": 10, "all_symbols": True,
                 "lookback_months": 36,   "desc": "Prospectus supplement"},
    "NT 10-K":  {"tier": 2, "weight": 1.6, "max_per_run": 6,  "all_symbols": True,
                 "lookback_months": 24,   "desc": "Late annual report notice"},
    "NT 10-Q":  {"tier": 2, "weight": 1.6, "max_per_run": 6,  "all_symbols": True,
                 "lookback_months": 24,   "desc": "Late quarterly report notice"},
    # ── Tier 3: Ownership & Insiders ─────────────────────────────────────────
    "4":        {"tier": 3, "weight": 1.2, "max_per_run": 50, "all_symbols": True,
                 "lookback_months": 12,   "desc": "Insider transaction"},
    "SC 13D":   {"tier": 3, "weight": 1.5, "max_per_run": 10, "all_symbols": True,
                 "lookback_months": 36,   "desc": "Activist investor (>5%)"},
    "SC 13D/A": {"tier": 3, "weight": 1.5, "max_per_run": 10, "all_symbols": True,
                 "lookback_months": 36,   "desc": "Activist investor amendment"},
    "SC 13G":   {"tier": 3, "weight": 1.3, "max_per_run": 10, "all_symbols": True,
                 "lookback_months": 24,   "desc": "Institutional holdings (>5%)"},
    "SC 13G/A": {"tier": 3, "weight": 1.3, "max_per_run": 10, "all_symbols": True,
                 "lookback_months": 24,   "desc": "Institutional holdings amendment"},
}

# ── Section budget per form (chars) ───────────────────────────────────────────
_SECTION_BUDGET = {
    "10-K":   58_000,
    "10-Q":   45_000,
    "8-K":    30_000,
    "S-3":    33_000,
    "424B":   33_000,   # prefix match for 424B1/3/4/5
    "NT":     8_000,    # prefix match for NT 10-K/Q
    "4":      4_000,
    "SC 13D": 20_000,
    "SC 13G": 12_000,
}

def _budget_for(form_type: str) -> int:
    for key, budget in _SECTION_BUDGET.items():
        if form_type.startswith(key):
            return budget
    return 30_000


# ═══════════════════════════════════════════════════════════════════════════════
# CIK Map
# ═══════════════════════════════════════════════════════════════════════════════

def _load_cik_map() -> dict[str, str]:
    """Load ticker→CIK mapping. Cache locally for 24h."""
    if BULK_MAP_CACHE.exists():
        age = time.time() - BULK_MAP_CACHE.stat().st_mtime
        if age < BULK_MAP_TTL_H * 3600:
            with open(BULK_MAP_CACHE) as f:
                return json.load(f)
    logger.info("[edgar] Downloading SEC bulk ticker→CIK map...")
    r = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=EDGAR_HEADERS, timeout=30,
    )
    r.raise_for_status()
    raw     = r.json()
    mapping = {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in raw.values()}
    BULK_MAP_CACHE.parent.mkdir(exist_ok=True)
    with open(BULK_MAP_CACHE, "w") as f:
        json.dump(mapping, f)
    logger.info(f"[edgar] CIK map loaded: {len(mapping)} companies")
    return mapping


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _article_hash(url: str, title: str, published_at: datetime) -> str:
    raw = f"{url}|{title}|{published_at.isoformat()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _parse_date(raw) -> Optional[datetime]:
    if not raw:
        return None
    try:
        if isinstance(raw, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", raw.strip()):
            return datetime.strptime(raw.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        dt = dateutil_parser.parse(str(raw))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _sec_get(url: str) -> Optional[requests.Response]:
    """Rate-limited GET with retry."""
    time.sleep(random.uniform(EDGAR_DELAY_MIN, EDGAR_DELAY_MAX))
    for attempt in range(3):
        try:
            r = requests.get(url, headers=EDGAR_HEADERS, timeout=EDGAR_TIMEOUT)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                time.sleep(2 ** attempt)
        except Exception as e:
            logger.debug(f"[edgar] GET {url} attempt {attempt+1}: {e}")
            time.sleep(1)
    return None


def _clean_text(text: str) -> str:
    """Collapse whitespace and strip page noise."""
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{4,}", " ", text)
    return text.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# Smart Section Extractors (Option A)
# ═══════════════════════════════════════════════════════════════════════════════

def _find_section(soup: BeautifulSoup, item_patterns: list[str],
                  next_item_patterns: list[str], budget: int) -> str:
    """
    Generic section finder. Scans the document for the first heading
    matching any pattern in item_patterns, then extracts text up to the
    next heading matching next_item_patterns (or budget chars).
    """
    text = soup.get_text(separator="\n", strip=True)
    lines = text.split("\n")

    start_idx = None
    for i, line in enumerate(lines):
        ln = line.strip().upper()
        for pat in item_patterns:
            if re.search(pat, ln):
                start_idx = i
                break
        if start_idx is not None:
            break

    if start_idx is None:
        return ""

    end_idx = len(lines)
    for i in range(start_idx + 1, len(lines)):
        ln = lines[i].strip().upper()
        for pat in next_item_patterns:
            if re.search(pat, ln):
                end_idx = i
                break
        if end_idx < len(lines):
            break

    snippet = "\n".join(lines[start_idx:end_idx])
    return _clean_text(snippet[:budget])


def _extract_10k_sections(soup: BeautifulSoup) -> str:
    """
    10-K Smart Extractor — 5-year trend signals.
    Extracts (in order): Risk Factors, MD&A, Financial Statements.
    Total budget: ~58,000 chars.
    """
    parts = []

    # ── Item 1A: Risk Factors (8,000 chars) ──────────────────────────────────
    rf = _find_section(soup,
        item_patterns=[r"ITEM\s+1A[\.\s]", r"RISK\s+FACTORS"],
        next_item_patterns=[r"ITEM\s+1B[\.\s]", r"ITEM\s+2[\.\s]", r"UNRESOLVED"],
        budget=8_000)
    if rf:
        parts.append(f"=== RISK FACTORS (Item 1A) ===\n{rf}")

    # ── Item 7: MD&A (25,000 chars) ──────────────────────────────────────────
    mda = _find_section(soup,
        item_patterns=[r"ITEM\s+7[\.\s]", r"MANAGEMENT.{0,20}DISCUSSION"],
        next_item_patterns=[r"ITEM\s+7A[\.\s]", r"ITEM\s+8[\.\s]", r"QUANTITATIVE"],
        budget=25_000)
    if mda:
        parts.append(f"=== MANAGEMENT DISCUSSION & ANALYSIS (Item 7) ===\n{mda}")

    # ── Item 7A: Market Risk (3,000 chars) ───────────────────────────────────
    mr = _find_section(soup,
        item_patterns=[r"ITEM\s+7A[\.\s]", r"QUANTITATIVE.*QUALITATIVE"],
        next_item_patterns=[r"ITEM\s+8[\.\s]", r"FINANCIAL\s+STATEMENTS"],
        budget=3_000)
    if mr:
        parts.append(f"=== MARKET RISK (Item 7A) ===\n{mr}")

    # ── Financial Statements — extract key tables as text (20,000 chars) ─────
    # Income Statement
    fs_inc = _find_section(soup,
        item_patterns=[r"CONSOLIDATED\s+STATEMENTS?\s+OF\s+(OPERATIONS|INCOME|EARNINGS)",
                       r"STATEMENTS?\s+OF\s+COMPREHENSIVE"],
        next_item_patterns=[r"CONSOLIDATED\s+BALANCE", r"STATEMENTS?\s+OF\s+FINANCIAL"],
        budget=6_000)
    if fs_inc:
        parts.append(f"=== INCOME STATEMENT ===\n{fs_inc}")

    # Balance Sheet
    fs_bs = _find_section(soup,
        item_patterns=[r"CONSOLIDATED\s+BALANCE\s+SHEET",
                       r"STATEMENTS?\s+OF\s+FINANCIAL\s+POSITION"],
        next_item_patterns=[r"CONSOLIDATED\s+STATEMENTS?\s+OF\s+(CASH|EQUITY)",
                            r"STATEMENTS?\s+OF\s+STOCKHOLDERS"],
        budget=6_000)
    if fs_bs:
        parts.append(f"=== BALANCE SHEET ===\n{fs_bs}")

    # Cash Flow
    fs_cf = _find_section(soup,
        item_patterns=[r"CONSOLIDATED\s+STATEMENTS?\s+OF\s+CASH",
                       r"CASH\s+FLOWS?\s+FROM\s+OPERATING"],
        next_item_patterns=[r"NOTES\s+TO\s+(CONSOLIDATED\s+)?FINANCIAL",
                            r"ITEM\s+9[\.\s]"],
        budget=6_000)
    if fs_cf:
        parts.append(f"=== CASH FLOW STATEMENT ===\n{fs_cf}")

    # Going concern / debt notes (5,000 chars)
    gc = _find_section(soup,
        item_patterns=[r"GOING\s+CONCERN", r"SUBSTANTIAL\s+DOUBT",
                       r"NOTE\s+\d+.*DEBT", r"LONG.TERM\s+DEBT"],
        next_item_patterns=[r"NOTE\s+\d+\.", r"ITEM\s+\d+[\.\s]"],
        budget=5_000)
    if gc:
        parts.append(f"=== GOING CONCERN / DEBT NOTES ===\n{gc}")

    result = "\n\n".join(parts)
    return result[:58_000] if result else ""


def _extract_10q_sections(soup: BeautifulSoup) -> str:
    """
    10-Q Smart Extractor — 5-quarter sequential trend.
    Budget: ~45,000 chars.
    """
    parts = []

    # Financial Statements (Part I, Item 1) — 20,000 chars
    fs = _find_section(soup,
        item_patterns=[r"ITEM\s+1[\.\s]", r"FINANCIAL\s+STATEMENTS"],
        next_item_patterns=[r"ITEM\s+2[\.\s]", r"MANAGEMENT.{0,20}DISCUSSION"],
        budget=20_000)
    if fs:
        parts.append(f"=== FINANCIAL STATEMENTS (Part I, Item 1) ===\n{fs}")

    # MD&A (Part I, Item 2) — 15,000 chars
    mda = _find_section(soup,
        item_patterns=[r"ITEM\s+2[\.\s]", r"MANAGEMENT.{0,20}DISCUSSION"],
        next_item_patterns=[r"ITEM\s+3[\.\s]", r"QUANTITATIVE"],
        budget=15_000)
    if mda:
        parts.append(f"=== MD&A (Part I, Item 2) ===\n{mda}")

    # Market Risk (Part I, Item 3) — 3,000 chars
    mr = _find_section(soup,
        item_patterns=[r"ITEM\s+3[\.\s]", r"QUANTITATIVE.*QUALITATIVE"],
        next_item_patterns=[r"ITEM\s+4[\.\s]", r"CONTROLS"],
        budget=3_000)
    if mr:
        parts.append(f"=== MARKET RISK (Part I, Item 3) ===\n{mr}")

    # Risk Factor updates (Part II, Item 1A) — 5,000 chars
    rf = _find_section(soup,
        item_patterns=[r"PART\s+II.*ITEM\s+1A", r"RISK\s+FACTORS"],
        next_item_patterns=[r"ITEM\s+2[\.\s]", r"UNREGISTERED"],
        budget=5_000)
    if rf:
        parts.append(f"=== RISK FACTOR UPDATES (Part II, Item 1A) ===\n{rf}")

    # Unregistered Sales / Dilution (Part II, Item 2) — 2,000 chars
    us = _find_section(soup,
        item_patterns=[r"ITEM\s+2[\.\s].*UNREGISTERED", r"UNREGISTERED\s+SALES"],
        next_item_patterns=[r"ITEM\s+3[\.\s]", r"ITEM\s+4[\.\s]", r"DEFAULTS"],
        budget=2_000)
    if us:
        parts.append(f"=== UNREGISTERED SALES / DILUTION (Part II, Item 2) ===\n{us}")

    result = "\n\n".join(parts)
    return result[:45_000] if result else ""


def _extract_8k_sections(soup: BeautifulSoup, full_text_raw: str) -> str:
    """
    8-K Smart Extractor — current report event extraction.
    If short (<30,000 chars), return full text.
    Otherwise extract key item sections.
    Budget: ~30,000 chars.
    """
    if len(full_text_raw) <= 30_000:
        return _clean_text(full_text_raw)

    parts = []
    # Map each Item number to a label for context
    item_map = {
        "1.01": "Entry into Material Agreement",
        "1.02": "Termination of Material Agreement",
        "1.03": "Bankruptcy/Receivership",
        "2.01": "Completion of Acquisition",
        "2.02": "Results of Operations",
        "2.03": "Creation of Direct Financial Obligation",
        "2.05": "Departure of Directors / Officers",
        "2.06": "Material Impairment",
        "3.01": "Delisting",
        "4.01": "Auditor Change",
        "4.02": "Non-Reliance on Financial Statements",
        "5.01": "Changes in Control",
        "5.02": "Director/Officer Changes",
        "5.03": "Amendments to Articles",
        "7.01": "Regulation FD Disclosure",
        "8.01": "Other Events",
        "9.01": "Financial Statements and Exhibits",
    }
    for item_num, label in item_map.items():
        section = _find_section(soup,
            item_patterns=[rf"ITEM\s+{re.escape(item_num)}"],
            next_item_patterns=[r"ITEM\s+\d+\.\d+", r"SIGNATURES", r"EXHIBIT"],
            budget=5_000)
        if section:
            parts.append(f"=== Item {item_num}: {label} ===\n{section}")

    result = "\n\n".join(parts) if parts else _clean_text(full_text_raw[:30_000])
    return result[:30_000]


def _extract_s3_424b_sections(soup: BeautifulSoup) -> str:
    """
    S-3 / 424B Smart Extractor — dilution & capital structure.
    Budget: ~33,000 chars.
    """
    parts = []

    summary = _find_section(soup,
        item_patterns=[r"PROSPECTUS\s+SUMMARY", r"SUMMARY\s+OF\s+OFFERING",
                       r"SUMMARY\s+OF\s+THE\s+OFFERING"],
        next_item_patterns=[r"RISK\s+FACTORS", r"USE\s+OF\s+PROCEEDS"],
        budget=8_000)
    if summary:
        parts.append(f"=== PROSPECTUS SUMMARY ===\n{summary}")

    offering = _find_section(soup,
        item_patterns=[r"THE\s+OFFERING", r"OFFERING\s+OVERVIEW"],
        next_item_patterns=[r"RISK\s+FACTORS", r"USE\s+OF\s+PROCEEDS", r"PLAN\s+OF\s+DISTRIBUTION"],
        budget=6_000)
    if offering:
        parts.append(f"=== THE OFFERING ===\n{offering}")

    proceeds = _find_section(soup,
        item_patterns=[r"USE\s+OF\s+PROCEEDS"],
        next_item_patterns=[r"RISK\s+FACTORS", r"DILUTION", r"DIVIDEND"],
        budget=4_000)
    if proceeds:
        parts.append(f"=== USE OF PROCEEDS ===\n{proceeds}")

    dilution = _find_section(soup,
        item_patterns=[r"DILUTION"],
        next_item_patterns=[r"PLAN\s+OF\s+DISTRIBUTION", r"SELLING\s+STOCKHOLDER",
                            r"DESCRIPTION\s+OF\s+(CAPITAL\s+STOCK|SECURITIES)"],
        budget=5_000)
    if dilution:
        parts.append(f"=== DILUTION ===\n{dilution}")

    rf = _find_section(soup,
        item_patterns=[r"RISK\s+FACTORS"],
        next_item_patterns=[r"USE\s+OF\s+PROCEEDS", r"DILUTION",
                            r"DESCRIPTION\s+OF\s+(CAPITAL|SECURITIES)"],
        budget=10_000)
    if rf:
        parts.append(f"=== RISK FACTORS ===\n{rf}")

    result = "\n\n".join(parts)
    return result[:33_000] if result else ""


def _extract_nt_sections(soup: BeautifulSoup) -> str:
    """NT 10-K / NT 10-Q — short filing, return full text up to budget."""
    text = soup.get_text(separator="\n", strip=True)
    return _clean_text(text[:8_000])


def _extract_13d_sections(soup: BeautifulSoup) -> str:
    """SC 13D — Activist investor. Focus on Item 4 (purpose) and Item 5 (ownership)."""
    parts = []

    item3 = _find_section(soup,
        item_patterns=[r"ITEM\s+3[\.\s]", r"SOURCE\s+(AND\s+AMOUNT\s+OF\s+)?FUNDS"],
        next_item_patterns=[r"ITEM\s+4[\.\s]"],
        budget=3_000)
    if item3:
        parts.append(f"=== Item 3: Source of Funds ===\n{item3}")

    item4 = _find_section(soup,
        item_patterns=[r"ITEM\s+4[\.\s]", r"PURPOSE\s+OF\s+TRANSACTION"],
        next_item_patterns=[r"ITEM\s+5[\.\s]"],
        budget=8_000)
    if item4:
        parts.append(f"=== Item 4: Purpose of Transaction ===\n{item4}")

    item5 = _find_section(soup,
        item_patterns=[r"ITEM\s+5[\.\s]", r"INTEREST\s+IN\s+SECURITIES"],
        next_item_patterns=[r"ITEM\s+6[\.\s]"],
        budget=4_000)
    if item5:
        parts.append(f"=== Item 5: Interest in Securities ===\n{item5}")

    item6 = _find_section(soup,
        item_patterns=[r"ITEM\s+6[\.\s]", r"CONTRACTS.*ARRANGEMENTS"],
        next_item_patterns=[r"ITEM\s+7[\.\s]", r"SIGNATURES"],
        budget=5_000)
    if item6:
        parts.append(f"=== Item 6: Contracts/Arrangements ===\n{item6}")

    result = "\n\n".join(parts)
    return result[:20_000] if result else ""


def _extract_13g_sections(soup: BeautifulSoup) -> str:
    """SC 13G — Institutional holder. Concise — item 5 (%) is what matters."""
    parts = []
    for item_num, label, budget in [
        ("5", "Interest in Securities (Ownership %)", 5_000),
        ("4", "Ownership / Citizenship", 3_000),
        ("6", "Ownership of More than 5%", 3_000),
        ("7", "Identification of Subsidiaries", 2_000),
    ]:
        sec = _find_section(soup,
            item_patterns=[rf"ITEM\s+{item_num}[\.\s]"],
            next_item_patterns=[rf"ITEM\s+{int(item_num)+1}[\.\s]", r"SIGNATURES"],
            budget=budget)
        if sec:
            parts.append(f"=== Item {item_num}: {label} ===\n{sec}")

    result = "\n\n".join(parts)
    return result[:12_000] if result else ""


# ═══════════════════════════════════════════════════════════════════════════════
# Form 4 XML Parser (machine-readable, no HTML parsing)
# ═══════════════════════════════════════════════════════════════════════════════

_TRANSACTION_CODES = {
    "P": "PURCHASED (open market buy)",
    "S": "SOLD (open market sale)",
    "A": "AWARD (grant/award from company)",
    "D": "RETURNED to issuer",
    "F": "PAYMENT of tax (withheld for taxes)",
    "G": "GIFT",
    "I": "DISCRETIONARY transaction (by broker)",
    "J": "OTHER acquisition or disposition",
    "M": "OPTION EXERCISE (exercise of derivative)",
    "C": "CONVERSION of derivative security",
    "W": "ACQUIRED by will/inheritance",
    "X": "EXERCISE of in-the-money derivative",
    "Z": "DEPOSIT/WITHDRAWAL into/from voting trust",
}

_OWNERSHIP_CODES = {
    "D": "Direct",
    "I": "Indirect (through entity/trust)",
}

def _parse_form4_xml(url: str) -> str:
    """
    Fetch and parse Form 4 XML. Returns a structured human-readable text block
    that the LLM can score without seeing raw XML.
    """
    r = _sec_get(url)
    if not r:
        return ""
    try:
        root = ET.fromstring(r.content)
        ns   = {"": ""}  # Form 4 has no namespace

        def _find(el, *tags):
            for tag in tags:
                found = el.find(tag)
                if found is not None and found.text:
                    return found.text.strip()
            return None

        # ── Issuer ────────────────────────────────────────────────────────────
        issuer     = root.find(".//issuer")
        issuer_name = _find(issuer, "issuerName") if issuer is not None else "Unknown"
        issuer_tick = _find(issuer, "issuerTradingSymbol") if issuer is not None else ""

        # ── Reporting owner ───────────────────────────────────────────────────
        owner       = root.find(".//reportingOwner")
        owner_name  = ""
        owner_title = ""
        if owner is not None:
            oid  = owner.find("reportingOwnerId")
            orel = owner.find("reportingOwnerRelationship")
            if oid is not None:
                owner_name = _find(oid, "rptOwnerName") or ""
            if orel is not None:
                parts_title = []
                if _find(orel, "isDirector") == "1":
                    parts_title.append("Director")
                if _find(orel, "isOfficer") == "1":
                    title = _find(orel, "officerTitle") or "Officer"
                    parts_title.append(title)
                if _find(orel, "isTenPercentOwner") == "1":
                    parts_title.append("10% Owner")
                owner_title = ", ".join(parts_title) or "Insider"

        # ── Period of report ──────────────────────────────────────────────────
        period = _find(root, "periodOfReport") or ""

        lines = [
            f"SEC FORM 4 — INSIDER TRANSACTION REPORT",
            f"Company: {issuer_name} ({issuer_tick})",
            f"Insider: {owner_name} ({owner_title})",
            f"Report Period: {period}",
            "",
        ]

        total_buy_value  = 0.0
        total_sell_value = 0.0
        tx_count         = 0

        # ── Non-derivative transactions (open market buys/sells) ──────────────
        for tx in root.findall(".//nonDerivativeTransaction"):
            code_el  = tx.find(".//transactionCode")
            code     = code_el.text.strip().upper() if code_el is not None and code_el.text else "?"
            shares_el = tx.find(".//transactionShares/value")
            price_el  = tx.find(".//transactionPricePerShare/value")
            owned_el  = tx.find(".//sharesOwnedFollowingTransaction/value")
            date_el   = tx.find("transactionDate/value")
            direct_el = tx.find(".//directOrIndirectOwnership/value")

            shares = float(shares_el.text) if shares_el is not None and shares_el.text else 0.0
            price  = float(price_el.text)  if price_el  is not None and price_el.text  else 0.0
            owned  = float(owned_el.text)  if owned_el  is not None and owned_el.text  else 0.0
            date   = date_el.text.strip()  if date_el   is not None and date_el.text   else period
            direct = _OWNERSHIP_CODES.get((direct_el.text or "D").strip().upper(), "Direct")

            code_label = _TRANSACTION_CODES.get(code, f"CODE={code}")
            value = shares * price
            if code == "P":
                total_buy_value  += value
            elif code == "S":
                total_sell_value += value

            lines.append(
                f"  TRANSACTION [{code}] {code_label}"
                f" | Date: {date}"
                f" | Shares: {shares:,.0f}"
                f" | Price: ${price:,.2f}"
                f" | Value: ${value:,.0f}"
                f" | Post-tx Ownership: {owned:,.0f} shares ({direct})"
            )
            tx_count += 1

        # ── Derivative transactions (options, warrants, RSUs) ─────────────────
        for tx in root.findall(".//derivativeTransaction"):
            code_el   = tx.find(".//transactionCode")
            code      = code_el.text.strip().upper() if code_el is not None and code_el.text else "?"
            shares_el = tx.find(".//transactionShares/value")
            price_el  = tx.find(".//exercisePrice/value")
            date_el   = tx.find("transactionDate/value")
            sec_el    = tx.find(".//securityTitle/value")

            shares = float(shares_el.text) if shares_el is not None and shares_el.text else 0.0
            price  = float(price_el.text)  if price_el  is not None and price_el.text  else 0.0
            date   = date_el.text.strip()  if date_el   is not None and date_el.text   else period
            sec    = sec_el.text.strip()   if sec_el    is not None and sec_el.text    else "Derivative"
            code_label = _TRANSACTION_CODES.get(code, f"CODE={code}")

            lines.append(
                f"  DERIVATIVE [{code}] {code_label}"
                f" | Security: {sec}"
                f" | Date: {date}"
                f" | Shares: {shares:,.0f}"
                f" | Exercise Price: ${price:,.2f}"
            )
            tx_count += 1

        if tx_count == 0:
            return ""

        lines.append("")
        if total_buy_value > 0:
            lines.append(f"SUMMARY: Net open-market BUY value = ${total_buy_value:,.0f}")
        if total_sell_value > 0:
            lines.append(f"SUMMARY: Net open-market SELL value = ${total_sell_value:,.0f}")

        return "\n".join(lines)

    except Exception as e:
        logger.debug(f"[edgar] Form4 XML parse error {url}: {e}")
        return ""


# ═══════════════════════════════════════════════════════════════════════════════
# Universal Filing Text Extractor (dispatches to form-specific extractors)
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_filing_text(url: str, form_type: str) -> str:
    """
    Fetch filing document and extract signal-rich section text.
    Form 4 takes the XML path; all others use BeautifulSoup HTML parsing.
    """
    # Form 4 — always XML, special parser
    if form_type == "4":
        return _parse_form4_xml(url)

    r = _sec_get(url)
    if not r or r.status_code != 200:
        return ""

    content_type = r.headers.get("content-type", "")
    if "html" in content_type or url.lower().endswith((".htm", ".html")):
        soup = BeautifulSoup(r.content, "lxml")
        for tag in soup(["script", "style", "nav", "footer", "head"]):
            tag.decompose()
        full_text_raw = soup.get_text(separator="\n", strip=True)
    else:
        full_text_raw = r.text
        soup          = BeautifulSoup(r.content, "lxml")
        for tag in soup(["script", "style", "nav", "footer", "head"]):
            tag.decompose()

    ft = form_type.upper()
    if ft in ("10-K", "10-K/A"):
        extracted = _extract_10k_sections(soup)
    elif ft in ("10-Q", "10-Q/A"):
        extracted = _extract_10q_sections(soup)
    elif ft in ("8-K", "8-K/A"):
        extracted = _extract_8k_sections(soup, full_text_raw)
    elif ft in ("S-3", "S-3/A") or ft.startswith("424B"):
        extracted = _extract_s3_424b_sections(soup)
    elif ft.startswith("NT"):
        extracted = _extract_nt_sections(soup)
    elif ft in ("SC 13D", "SC 13D/A"):
        extracted = _extract_13d_sections(soup)
    elif ft in ("SC 13G", "SC 13G/A"):
        extracted = _extract_13g_sections(soup)
    else:
        extracted = _clean_text(full_text_raw[:_budget_for(form_type)])

    # Fallback: if extractor found nothing, use raw text up to budget
    if not extracted and full_text_raw:
        budget = _budget_for(form_type)
        extracted = _clean_text(full_text_raw[:budget])

    return extracted


# ═══════════════════════════════════════════════════════════════════════════════
# Title Extractor
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_title_from_text(text: str, fallback: str, form_type: str) -> str:
    """Extract a meaningful title from filing text, with form-type aware fallbacks."""
    # Form 4 — build title from structured text
    if form_type == "4":
        for line in text.split("\n"):
            if "INSIDER:" in line.upper() or "PURCHASED" in line.upper() or "SOLD" in line.upper():
                # Build a compact title
                company_line = next((l for l in text.split("\n") if "Company:" in l), "")
                insider_line = next((l for l in text.split("\n") if "Insider:" in l), "")
                tx_line      = next((l for l in text.split("\n") if "TRANSACTION" in l), "")
                company = company_line.replace("Company:", "").strip()[:40]
                insider = insider_line.replace("Insider:", "").strip()[:30]
                if "PURCHASED" in tx_line.upper():
                    return f"{company} — Insider Buy by {insider}"
                elif "SOLD" in tx_line.upper():
                    return f"{company} — Insider Sale by {insider}"
                return fallback
        return fallback

    # Generic title extraction
    skip_exact = re.compile(
        r"^(false|true|\d{10}|\d{4}-\d{2}-\d{2}|UNITED STATES|SECURITIES AND EXCHANGE"
        r"|FORM \d|CURRENT REPORT|PURSUANT TO SECTION|Washington, D\.C\.|Date of Report"
        r"|Registrant.?s? telephone|Commission File|IRS Employer|State or other"
        r"|Amendment Flag|Document Type|Entity|Fiscal Year"
        r"|Item \d+\.|Exhibit \d+|N/A$"
        r"|=== .* ===)",
        re.I,
    )
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    for line in lines[5:]:
        if len(line) < 20 or len(line) > 250:
            continue
        if skip_exact.match(line):
            continue
        if not re.search(r"[a-z]", line):
            continue
        return line[:200]
    return fallback


# ═══════════════════════════════════════════════════════════════════════════════
# Deterministic SEC Signal Extractor
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_sec_signals(conn, symbol_id: int, ticker: str,
                         filing: dict, article_id: Optional[int]):
    """
    Extract deterministic rule-based signals from a filing and write to sec_signals.
    No LLM. Pure rule logic. Runs after article insertion.
    """
    form_type   = filing["form_type"]
    filed_at    = filing["published_at"]
    source_url  = filing["url"]

    signals = []

    # ── NT 10-K / NT 10-Q → late filing red flag ─────────────────────────────
    if form_type in ("NT 10-K",):
        signals.append({
            "signal_type":    "nt_delay_10k",
            "signal_value":   None,
            "signal_text":    f"{ticker} failed to file 10-K on time (NT 10-K filed {filed_at.date()})",
            "score_modifier": -0.05,
        })
    elif form_type in ("NT 10-Q",):
        signals.append({
            "signal_type":    "nt_delay_10q",
            "signal_value":   None,
            "signal_text":    f"{ticker} failed to file 10-Q on time (NT 10-Q filed {filed_at.date()})",
            "score_modifier": -0.03,
        })

    # ── Form 4 — insider buy/sell ─────────────────────────────────────────────
    elif form_type == "4":
        full_text = filing.get("full_text", "")
        buy_match  = re.search(r"Net open-market BUY value\s*=\s*\$([\d,]+)", full_text)
        sell_match = re.search(r"Net open-market SELL value\s*=\s*\$([\d,]+)", full_text)
        if buy_match:
            val = float(buy_match.group(1).replace(",", ""))
            mod = min(0.03, max(0.01, val / 1_000_000 * 0.01))  # scale: $1M → +0.01
            signals.append({
                "signal_type":    "insider_buy",
                "signal_value":   val,
                "signal_text":    f"{ticker} insider open-market purchase ${val:,.0f}",
                "score_modifier": round(mod, 4),
            })
        if sell_match:
            val = float(sell_match.group(1).replace(",", ""))
            signals.append({
                "signal_type":    "insider_sell",
                "signal_value":   val,
                "signal_text":    f"{ticker} insider open-market sale ${val:,.0f}",
                "score_modifier": -0.01,
            })

    # ── SC 13D → activist entry ───────────────────────────────────────────────
    elif form_type == "SC 13D":
        signals.append({
            "signal_type":    "activist_entry",
            "signal_value":   None,
            "signal_text":    f"Activist investor filed SC 13D for {ticker}",
            "score_modifier": +0.05,
        })
    elif form_type == "SC 13D/A":
        signals.append({
            "signal_type":    "activist_increase",
            "signal_value":   None,
            "signal_text":    f"Activist investor amended SC 13D for {ticker}",
            "score_modifier": +0.02,
        })

    # ── SC 13G → institutional add ────────────────────────────────────────────
    elif form_type == "SC 13G":
        signals.append({
            "signal_type":    "institutional_new",
            "signal_value":   None,
            "signal_text":    f"New institutional 13G filing for {ticker} (>5% stake)",
            "score_modifier": +0.02,
        })
    elif form_type == "SC 13G/A":
        signals.append({
            "signal_type":    "institutional_update",
            "signal_value":   None,
            "signal_text":    f"Institutional investor updated 13G for {ticker}",
            "score_modifier": +0.01,
        })

    # ── S-3 / 424B → active dilution shelf ───────────────────────────────────
    elif form_type in ("S-3",):
        signals.append({
            "signal_type":    "shelf_registration",
            "signal_value":   None,
            "signal_text":    f"{ticker} filed S-3 shelf registration — dilution risk",
            "score_modifier": -0.02,
        })
    elif form_type.startswith("424B"):
        signals.append({
            "signal_type":    "offering_prospectus",
            "signal_value":   None,
            "signal_text":    f"{ticker} filed {form_type} prospectus — active capital raise",
            "score_modifier": -0.04,
        })

    if not signals:
        return

    with conn.cursor() as cur:
        for sig in signals:
            cur.execute("""
                INSERT INTO sec_signals
                    (symbol_id, form_type, filed_at, signal_type, signal_value,
                     signal_text, score_modifier, source_url, article_id, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                ON CONFLICT DO NOTHING
            """, (
                symbol_id, form_type, filed_at,
                sig["signal_type"], sig.get("signal_value"),
                sig["signal_text"], sig["score_modifier"],
                source_url, article_id,
            ))
    conn.commit()


def _recompute_sec_modifier(conn, symbol_id: int) -> float:
    """
    Sum all active sec_signals for this symbol (last 180 days).
    Also applies absence-based penalties (stale_10k) that have no filing event.
    Write result to symbols.sec_score_modifier. Returns the value.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(SUM(score_modifier), 0.0)
            FROM sec_signals
            WHERE symbol_id = %s
              AND is_active  = TRUE
              AND filed_at  > NOW() - INTERVAL '180 days'
        """, (symbol_id,))
        row = cur.fetchone()
    total = float(row[0]) if row else 0.0

    # ── Absence-based penalty: stale_10k ─────────────────────────────────────
    # If no 10-K has been ingested in the last 18 months → -0.06
    # (possible delisting risk, shell company, or non-filer)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT last_10k_filed FROM symbols WHERE id = %s
        """, (symbol_id,))
        sym_row = cur.fetchone()
    if sym_row:
        last_10k = sym_row[0]
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=18 * 30.44)
        if last_10k is None or (last_10k.replace(tzinfo=timezone.utc)
                                 if last_10k.tzinfo is None else last_10k) < cutoff:
            total -= 0.06  # stale_10k penalty

    total = round(max(-0.20, min(0.20, total)), 4)  # cap at ±0.20
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE symbols
               SET sec_score_modifier      = %s,
                   sec_modifier_updated_at = NOW()
             WHERE id = %s
        """, (total, symbol_id))
    conn.commit()
    return total


# ═══════════════════════════════════════════════════════════════════════════════
# DB Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _load_all_symbols(conn, limit: int = 0) -> list[tuple]:
    """Return all active NASDAQ symbols."""
    q = "SELECT id, symbol FROM symbols WHERE exchange = 'NASDAQ' AND status = TRUE ORDER BY symbol"
    if limit:
        q += f" LIMIT {limit}"
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchall()


def _load_symbols_without_rss(conn, limit: int = 0) -> list[tuple]:
    """Return symbols with no working RSS/atom feed (for 8-K fallback coverage)."""
    q = """
        SELECT s.id, s.symbol
        FROM symbols s
        WHERE s.exchange = 'NASDAQ'
          AND NOT EXISTS (
              SELECT 1 FROM rss_feeds r
              WHERE r.symbol_id = s.id
                AND r.feed_type IN ('rss', 'atom')
                AND r.is_active = true
          )
        ORDER BY s.symbol
    """
    if limit:
        q += f" LIMIT {limit}"
    with conn.cursor() as cur:
        cur.execute(q)
        return cur.fetchall()


def _get_seen_hashes(conn) -> set:
    with conn.cursor() as cur:
        cur.execute("SELECT article_hash FROM news_articles WHERE form_type IS NOT NULL")
        return {row[0] for row in cur.fetchall()}


def _ensure_edgar_feed_row(conn, symbol_id: int, ticker: str) -> int:
    """Create or return the synthetic rss_feeds row for EDGAR source."""
    feed_url = f"https://data.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={ticker}&type=all"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM rss_feeds WHERE symbol_id = %s AND source = 'edgar_8k'",
            (symbol_id,),
        )
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("""
            INSERT INTO rss_feeds (symbol_id, feed_url, feed_type, source, is_active, discovered_at)
            VALUES (%s, %s, 'api', 'edgar_8k', true, NOW())
            ON CONFLICT (feed_url) DO UPDATE SET is_active = true
            RETURNING id
        """, (symbol_id, feed_url))
        conn.commit()
        return cur.fetchone()[0]


def _insert_article(conn, feed_id: int, symbol_id: int, a: dict) -> Optional[int]:
    """Insert one filing article. Returns new article id or None if duplicate."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO news_articles
                (symbol_id, feed_id, url, title, full_text, published_at,
                 inserted_at, article_hash, source_name,
                 form_type, filing_tier, sec_source_weight)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, 'edgar_sec',
                    %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
        """, (
            symbol_id, feed_id,
            a["url"], a["title"], a["full_text"], a["published_at"],
            a["article_hash"],
            a["form_type"], a["filing_tier"], a["sec_source_weight"],
        ))
        row = cur.fetchone()
    conn.commit()
    return row[0] if row else None


def _update_symbol_sec_dates(conn, symbol_id: int, form_type: str, filed_at: datetime):
    """Track the most recent 10-K and 10-Q dates on the symbols row."""
    if form_type in ("10-K", "10-K/A"):
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE symbols SET last_10k_filed = %s
                WHERE id = %s AND (last_10k_filed IS NULL OR last_10k_filed < %s)
            """, (filed_at, symbol_id, filed_at))
        conn.commit()
    elif form_type in ("10-Q", "10-Q/A"):
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE symbols SET last_10q_filed = %s
                WHERE id = %s AND (last_10q_filed IS NULL OR last_10q_filed < %s)
            """, (filed_at, symbol_id, filed_at))
        conn.commit()


# ═══════════════════════════════════════════════════════════════════════════════
# Per-Symbol Filing Fetcher
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_symbol_filings(ticker: str, cik: str, seen_hashes: set,
                           forms_to_fetch: dict) -> list[dict]:
    """
    Fetch filings for one ticker from EDGAR submissions API.
    forms_to_fetch: {form_type: registry_entry}
    Returns list of filing dicts ready for DB insertion.
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    r   = _sec_get(url)
    if not r:
        return []

    try:
        d = r.json()
    except Exception:
        return []

    recent = d.get("filings", {}).get("recent", {})
    forms  = recent.get("form", [])
    dates  = recent.get("filingDate", [])
    accs   = recent.get("accessionNumber", [])
    docs   = recent.get("primaryDocument", [])

    now = datetime.now(timezone.utc)

    # Build per-form lookback cutoffs and counters
    cutoffs  = {}
    counters = {}
    for ft, cfg in forms_to_fetch.items():
        cutoffs[ft]  = now - timedelta(days=cfg["lookback_months"] * 30.44)
        counters[ft] = 0

    articles = []
    cik_int  = int(cik)

    for i in range(len(forms)):
        form = forms[i]
        if form not in forms_to_fetch:
            continue

        cfg        = forms_to_fetch[form]
        filing_date = _parse_date(dates[i] if i < len(dates) else None)
        if not filing_date:
            continue

        # Lookback cutoff
        if filing_date < cutoffs[form]:
            continue

        # Per-form max cap
        if counters[form] >= cfg["max_per_run"]:
            continue

        acc_clean = (accs[i] if i < len(accs) else "").replace("-", "")
        doc       = docs[i] if i < len(docs) else ""
        if not doc or not acc_clean:
            continue

        doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/{doc}"

        # Quick dedup — check hash with stub title before fetching
        stub_title = f"{ticker} {form} {dates[i]}"
        stub_hash  = _article_hash(doc_url, stub_title, filing_date)
        if stub_hash in seen_hashes:
            counters[form] += 1
            continue

        # Fetch + extract
        full_text = _extract_filing_text(doc_url, form)
        if not full_text:
            counters[form] += 1
            continue

        title     = _extract_title_from_text(full_text, stub_title, form)
        art_hash  = _article_hash(doc_url, title, filing_date)
        if art_hash in seen_hashes:
            counters[form] += 1
            continue

        articles.append({
            "url":             doc_url,
            "title":           title,
            "published_at":    filing_date,
            "full_text":       full_text,
            "article_hash":    art_hash,
            "form_type":       form,
            "filing_tier":     cfg["tier"],
            "sec_source_weight": cfg["weight"],
        })
        seen_hashes.add(art_hash)
        seen_hashes.add(stub_hash)
        counters[form] += 1

    return articles


# ═══════════════════════════════════════════════════════════════════════════════
# Main Entry Point
# ═══════════════════════════════════════════════════════════════════════════════

def run(
    limit: int = 0,
    tier_filter: list[int] | None = None,
    forms_only: list[str] | None = None,
) -> dict:
    """
    Main EDGAR ingestion entry point.

    Args:
        limit:        Process only the first N symbols (0 = all).
        tier_filter:  If set, only process forms in these tiers (e.g. [1,2]).
        forms_only:   If set, only process these specific form types.

    Returns:
        Summary dict with inserted counts per form.
    """
    conn = get_connection()
    try:
        cik_map     = _load_cik_map()
        seen_hashes = _get_seen_hashes(conn)
        logger.info(f"[edgar] {len(seen_hashes)} SEC articles already in DB")

        # ── Build the active form registry for this run ────────────────────────
        active_registry: dict[str, dict] = {}
        for ft, cfg in FILING_REGISTRY.items():
            if tier_filter and cfg["tier"] not in tier_filter:
                continue
            if forms_only and ft not in forms_only:
                continue
            active_registry[ft] = cfg

        if not active_registry:
            logger.info("[edgar] No forms active for this run configuration")
            return {}

        # ── Determine symbol sets ──────────────────────────────────────────────
        # Forms with all_symbols=True → every active symbol
        # Forms with all_symbols=False → only symbols without RSS (8-K fallback)
        all_sym_forms   = {ft: cfg for ft, cfg in active_registry.items() if cfg["all_symbols"]}
        rss_only_forms  = {ft: cfg for ft, cfg in active_registry.items() if not cfg["all_symbols"]}

        all_symbols     = _load_all_symbols(conn, limit) if all_sym_forms else []
        no_rss_symbols  = _load_symbols_without_rss(conn, limit) if rss_only_forms else []

        # Merge: every symbol needs all_sym_forms; no_rss symbols also get rss_only_forms
        symbol_form_map: dict[tuple, dict] = {}
        for sid, sym in all_symbols:
            symbol_form_map[(sid, sym)] = dict(all_sym_forms)
        for sid, sym in no_rss_symbols:
            key = (sid, sym)
            if key not in symbol_form_map:
                symbol_form_map[key] = {}
            symbol_form_map[key].update(rss_only_forms)

        logger.info(
            f"[edgar] {len(symbol_form_map)} symbols to process | "
            f"forms active: {sorted(active_registry.keys())}"
        )

        totals: dict[str, int] = {ft: 0 for ft in active_registry}
        sym_skipped  = 0
        sym_processed = 0

        for (symbol_id, ticker), forms_to_fetch in symbol_form_map.items():
            if ticker not in cik_map:
                sym_skipped += 1
                continue

            cik      = cik_map[ticker]
            filings  = _fetch_symbol_filings(ticker, cik, seen_hashes, forms_to_fetch)

            if not filings:
                sym_skipped += 1
                if sym_processed % 100 == 0:
                    logger.info(f"[edgar] {sym_processed}/{len(symbol_form_map)} symbols done")
                continue

            feed_id = _ensure_edgar_feed_row(conn, symbol_id, ticker)

            for filing in filings:
                art_id = _insert_article(conn, feed_id, symbol_id, filing)
                ft     = filing["form_type"]
                if art_id:
                    totals[ft] = totals.get(ft, 0) + 1
                    # Update symbol SEC date trackers
                    _update_symbol_sec_dates(conn, symbol_id, ft, filing["published_at"])
                    # Extract deterministic signals
                    try:
                        _extract_sec_signals(conn, symbol_id, ticker, filing, art_id)
                    except Exception as e:
                        logger.debug(f"[edgar] signal extract failed {ticker}: {e}")

            # Recompute combined sec_modifier for this symbol
            try:
                mod = _recompute_sec_modifier(conn, symbol_id)
                logger.debug(f"[edgar] {ticker} sec_modifier={mod:+.4f}")
            except Exception as e:
                logger.debug(f"[edgar] sec_modifier recompute failed {ticker}: {e}")

            sym_processed += 1
            if sym_processed % 50 == 0:
                logger.info(
                    f"[edgar] Progress: {sym_processed}/{len(symbol_form_map)} | "
                    + " | ".join(f"{ft}={c}" for ft, c in totals.items() if c > 0)
                )

        summary = {
            "symbols_processed": sym_processed,
            "symbols_no_cik":    sym_skipped,
            "articles_inserted": totals,
            "total_inserted":    sum(totals.values()),
        }
        logger.info(
            f"[edgar] Done — processed={sym_processed} no_cik={sym_skipped} "
            + " | ".join(f"{ft}={c}" for ft, c in totals.items() if c > 0)
        )
        return summary

    finally:
        conn.close()


def run_single(symbol: str, symbol_id: int,
               forms_only: list[str] | None = None) -> int:
    """Run EDGAR pipeline for one specific symbol. Returns total articles inserted."""
    conn = get_connection()
    try:
        cik_map = _load_cik_map()
        if symbol not in cik_map:
            logger.warning(f"[edgar] {symbol} not found in SEC CIK map")
            return 0
        cik          = cik_map[symbol]
        seen_hashes  = _get_seen_hashes(conn)
        active_forms = {
            ft: cfg for ft, cfg in FILING_REGISTRY.items()
            if not forms_only or ft in forms_only
        }
        filings = _fetch_symbol_filings(symbol, cik, seen_hashes, active_forms)
        if not filings:
            logger.info(f"[edgar] {symbol} — no new filings")
            return 0
        feed_id  = _ensure_edgar_feed_row(conn, symbol_id, symbol)
        inserted = 0
        for filing in filings:
            art_id = _insert_article(conn, feed_id, symbol_id, filing)
            if art_id:
                inserted += 1
                _update_symbol_sec_dates(conn, symbol_id, filing["form_type"], filing["published_at"])
                _extract_sec_signals(conn, symbol_id, symbol, filing, art_id)
        _recompute_sec_modifier(conn, symbol_id)
        logger.info(f"[edgar] {symbol} → {inserted} filings inserted")
        return inserted
    finally:
        conn.close()
