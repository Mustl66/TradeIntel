"""
Test SEC EDGAR as a source for press releases (8-K filings).
EDGAR is free, machine-readable, no JS, covers ALL listed companies.
"""
import requests, json, sys

# First find GYRO's CIK number
search_url = "https://efts.sec.gov/LATEST/search-index?q=%22GYRO%22&dateRange=custom&startdt=2024-01-01&enddt=2026-05-22&forms=8-K"
headers = {"User-Agent": "TradeIntel research@tradeintel.com"}

# Use company search
r = requests.get("https://efts.sec.gov/LATEST/search-index?q=%22Gyrodyne%22&forms=8-K&dateRange=custom&startdt=2023-01-01&enddt=2026-05-22", headers=headers, timeout=15)
print(f"EFTS status: {r.status_code}")
if r.ok:
    d = r.json()
    print(f"Total hits: {d.get('total', {}).get('value', 0)}")
    hits = d.get('hits', {}).get('hits', [])
    for h in hits[:5]:
        src = h.get('_source', {})
        print(f"  {src.get('file_date')} | {src.get('display_names')} | {src.get('form_type')} | {src.get('period_of_report')}")
        print(f"    {src.get('file_num')} | acc: {src.get('_id')}")

# Also try the ticker-based lookup
print("\n--- Ticker search ---")
r2 = requests.get("https://efts.sec.gov/LATEST/search-index?q=%22GYRO%22&forms=8-K&dateRange=custom&startdt=2024-01-01&enddt=2026-05-22", headers=headers, timeout=15)
print(f"Status: {r2.status_code}")
if r2.ok:
    d2 = r2.json()
    hits2 = d2.get('hits', {}).get('hits', [])
    print(f"Hits: {len(hits2)}")
    for h in hits2[:5]:
        src = h.get('_source', {})
        print(f"  {src.get('file_date')} | {src.get('entity_name')} | {src.get('form_type')}")
