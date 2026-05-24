import sys, logging
sys.path.insert(0, r'C:\Users\Mustafa\PycharmProjects\TradeIntel')
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s - %(message)s")

from pipeline.html_ingest import (
    _handle_prnewswire,
    _handle_globenewswire_listing,
    _handle_nasdaq_page,
)

# Fake IDs for testing
FEED_ID = 999
SYM_ID = 999

print("\n=== TEST 1: PRNewswire ===")
arts = _handle_prnewswire(
    "https://www.prnewswire.com/news/above-food-ingredients-inc./",
    "ABVF", FEED_ID, SYM_ID
)
print(f"Articles found: {len(arts)}")
for a in arts[:3]:
    print(f"  [{a['published_at'].date()}] {a['title'][:70]}")

print("\n=== TEST 2: GlobeNewswire listing ===")
arts2 = _handle_globenewswire_listing(
    "https://www.globenewswire.com/en/search/organization/Alliance%20Entertainment",
    "AENT", FEED_ID, SYM_ID
)
print(f"Articles found: {len(arts2)}")
for a in arts2[:3]:
    print(f"  [{a['published_at'].date()}] {a['title'][:70]}")

print("\n=== TEST 3: Nasdaq page (fallback) ===")
arts3 = _handle_nasdaq_page(
    "https://www.nasdaq.com/market-activity/stocks/aapl/press-releases",
    "AAPL", FEED_ID, SYM_ID
)
print(f"Articles found: {len(arts3)}")
for a in arts3[:3]:
    print(f"  [{a['published_at'].date()}] {a['title'][:70]}")
