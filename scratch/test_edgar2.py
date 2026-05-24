"""
Test SEC EDGAR submissions API - direct CIK lookup for 8-K filings.
CIK for GYRO (Gyrodyne) = 0001589061
"""
import requests, json

headers = {"User-Agent": "TradeIntel research@tradeintel.com"}

# Direct submissions API - most reliable
cik = "0001589061"
url = f"https://data.sec.gov/submissions/CIK{cik}.json"
r = requests.get(url, headers=headers, timeout=15)
print(f"Status: {r.status_code}")

d = r.json()
print(f"Company: {d['name']}")
print(f"Ticker: {d.get('tickers', [])}")

recent = d['filings']['recent']
forms = recent['form']
dates = recent['filingDate']
accs = recent['accessionNumber']
docs = recent['primaryDocument']
descriptions = recent.get('primaryDocDescription', [''] * len(forms))

# Filter 8-K only
filings_8k = [(forms[i], dates[i], accs[i], docs[i]) 
              for i in range(len(forms)) 
              if forms[i] in ('8-K', '8-K/A')]

print(f"\n8-K filings: {len(filings_8k)}")
for form, date, acc, doc in filings_8k[:10]:
    acc_clean = acc.replace('-', '')
    # Document URL
    doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/{doc}"
    print(f"  {date} | {form} | {doc_url}")

# Test fetching one 8-K
if filings_8k:
    form, date, acc, doc = filings_8k[0]
    acc_clean = acc.replace('-', '')
    doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/{doc}"
    print(f"\nFetching most recent 8-K: {doc_url}")
    r2 = requests.get(doc_url, headers=headers, timeout=15)
    print(f"Status: {r2.status_code}, Size: {len(r2.text)} chars")
    # Extract text content
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(r2.text, 'html.parser')
    text = soup.get_text(separator='\n', strip=True)
    print(f"Text length: {len(text)}")
    print(f"Preview:\n{text[:800]}")
