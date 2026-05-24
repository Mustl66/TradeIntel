import requests, re, json

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}

r = requests.get('https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases', headers=headers, timeout=15)
text = r.text

# Extract drupalSettings JSON (block 15 from previous run)
m = re.search(r'drupalSettings\s*=\s*(\{.+\})\s*;?\s*</script>', text, re.DOTALL)
if not m:
    # try alternate pattern
    blocks = re.findall(r'<script[^>]*>(\{[^<]*drupalSettings[^<]*\})</script>', text, re.DOTALL)
    print("No drupalSettings direct match")
    # find the big block
    blocks2 = re.findall(r'<script[^>]*>(.*?)</script>', text, re.DOTALL)
    for b in blocks2:
        if 'drupalSettings' in b and len(b) > 5000:
            # try to parse what's after drupalSettings =
            m2 = re.search(r'"jupiter22[^}]{0,50}config[^}]{0,200}', b)
            if m2:
                print("JUPITER CONFIG:", m2.group()[:500])
            # look for endpoints
            endpoints = re.findall(r'"[^"]*endpoint[^"]*"\s*:\s*"([^"]+)"', b, re.IGNORECASE)
            print("Endpoints in drupalSettings:", endpoints[:20])
            # look for article-list specific config
            al = re.findall(r'article.list[^"]{0,200}', b, re.IGNORECASE)
            print("Article list refs:", al[:5])
            # look for nsdq api
            apis = re.findall(r'nsdq[^"\'<]{0,100}', b)
            print("nsdq refs (first 10):", apis[:10])
            break
else:
    try:
        ds = json.loads(m.group(1))
        print(json.dumps(ds, indent=2)[:5000])
    except Exception as e:
        print("Parse error:", e)
        print(m.group(1)[:3000])
