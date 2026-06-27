import sys, json, requests
sys.path.insert(0, '.')
from pipeline.edgar_ingest import _load_cik_map, _fetch_symbol_filings, FILING_REGISTRY

cik_map = _load_cik_map()
cik = cik_map.get('BMRA')
print('CIK:', cik)

r = requests.get(
    f'https://data.sec.gov/submissions/CIK{cik}.json',
    headers={'User-Agent': 'TradeIntel research@tradeintel.com', 'Accept-Encoding': 'gzip, deflate'},
    timeout=30
)
d = r.json()
recent = d.get('filings', {}).get('recent', {})
forms = recent.get('form', [])
dates = recent.get('filingDate', [])
print('Company:', d.get('name'))
print('Total filings in recent bucket:', len(forms))

from collections import Counter
print()
print('All form types found:')
for ft, cnt in Counter(forms).most_common():
    print(f'  {ft}: {cnt}')

print()
print('Last 10 filings:')
for i in range(min(10, len(forms))):
    print(f'  {dates[i]}  {forms[i]}')
