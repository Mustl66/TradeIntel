"""
Live test: Nasdaq API handler for GYRO and BNBX.
"""
import sys, os, logging
sys.path.insert(0, r'C:\Users\Mustafa\PycharmProjects\TradeIntel')
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

from pipeline.html_ingest import _handle_nasdaq_page

# Test with fake feed_id/symbol_id — just checking article extraction
for sym, url in [
    ('GYRO', 'https://www.nasdaq.com/market-activity/stocks/gyro/press-releases'),
    ('BNBX', 'https://www.nasdaq.com/market-activity/stocks/bnbx/press-releases'),
    ('BLDP', 'https://www.nasdaq.com/market-activity/stocks/bldp/press-releases'),
]:
    arts = _handle_nasdaq_page(url, sym, feed_id=999, symbol_id=999)
    print(f"\n{sym}: {len(arts)} articles")
    for a in arts[:3]:
        print(f"  [{a['published_at']}] {a['title'][:70]}")
        print(f"   full_text len={len(a['full_text'])} chars")
