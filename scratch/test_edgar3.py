"""
Test EDGAR bulk ticker->CIK mapping and multi-ticker batch pipeline.
"""
import requests, json

headers = {"User-Agent": "TradeIntel research@tradeintel.com"}

# SEC provides a bulk JSON mapping: ticker -> CIK
r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers, timeout=30)
print(f"Bulk map status: {r.status_code}, size: {len(r.text)} bytes")
data = r.json()
print(f"Total companies in SEC mapping: {len(data)}")

# Build ticker -> CIK dict
ticker_to_cik = {v['ticker'].upper(): str(v['cik_str']).zfill(10) for v in data.values()}

# Test a few tickers
test_tickers = ['GYRO', 'AAPL', 'ABNB', 'MSFT', 'ABTC']
for t in test_tickers:
    cik = ticker_to_cik.get(t)
    print(f"  {t} -> CIK: {cik}")
