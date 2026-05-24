"""
TDD tests for:
  1. Admin symbol search — also matches feed URLs (source keywords like globenewswire, nasdaq)
  2. News tab keyword search — filter articles by keyword in title/full_text
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

# ── Helpers ───────────────────────────────────────────────────────────────────

def make_symbol_row(id, symbol, company_name, active_feeds=1, total_feeds=1):
    return {
        "id": id,
        "symbol": symbol,
        "company_name": company_name,
        "active_feeds": active_feeds,
        "total_feeds": total_feeds,
    }

def make_article_row(id, title, url="http://x.com", published_at=None,
                     inserted_at=None, full_text="", feed_url="http://feed.com"):
    from datetime import datetime
    return {
        "id": id,
        "title": title,
        "url": url,
        "published_at": published_at or datetime(2024, 1, 1, 12, 0),
        "inserted_at": inserted_at or datetime(2024, 1, 1, 12, 0),
        "full_text": full_text,
        "feed_url": feed_url,
    }

# ── Feature 1: Symbol search also matches feed URLs ───────────────────────────

class TestSymbolSearchByFeedUrl:
    """
    GET /symbols?q=<term>
    Should return symbols whose rss_feeds.feed_url ILIKE %term%
    in addition to symbol/company_name matches.
    """

    def setup_method(self):
        from admin import app
        self.client = TestClient(app)

    def _mock_conn(self, rows):
        """Patch get_conn so cursor returns given rows."""
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.__enter__ = lambda s: s
        mock_cur.__exit__  = MagicMock(return_value=False)
        mock_cur.fetchall.return_value = rows
        mock_conn.cursor.return_value = mock_cur
        return mock_conn

    def test_search_by_symbol_still_works(self):
        """Baseline — ticker search still returns matching symbol."""
        rows = [make_symbol_row(1, "AAPL", "Apple Inc")]
        with patch("admin.get_conn", return_value=self._mock_conn(rows)):
            resp = self.client.get("/symbols?q=AAPL")
        assert resp.status_code == 200
        assert "AAPL" in resp.text

    def test_search_by_company_name_still_works(self):
        rows = [make_symbol_row(2, "GOOG", "Alphabet Inc")]
        with patch("admin.get_conn", return_value=self._mock_conn(rows)):
            resp = self.client.get("/symbols?q=Alphabet")
        assert resp.status_code == 200
        assert "GOOG" in resp.text

    def test_search_globenewswire_returns_symbols_with_that_feed(self):
        """Searching 'globenewswire' should surface symbols that have a
        GlobeNewswire feed_url, even if ticker/name don't contain the word."""
        rows = [make_symbol_row(3, "XYZ", "Some Corp")]
        with patch("admin.get_conn", return_value=self._mock_conn(rows)):
            resp = self.client.get("/symbols?q=globenewswire")
        assert resp.status_code == 200
        assert "XYZ" in resp.text

    def test_search_nasdaq_returns_symbols_with_nasdaq_feed(self):
        rows = [make_symbol_row(4, "QQQQ", "Nasdaq ETF")]
        with patch("admin.get_conn", return_value=self._mock_conn(rows)):
            resp = self.client.get("/symbols?q=nasdaq")
        assert resp.status_code == 200
        assert "QQQQ" in resp.text

    def test_empty_search_returns_all(self):
        rows = [make_symbol_row(1, "AAPL", "Apple"), make_symbol_row(2, "MSFT", "Microsoft")]
        with patch("admin.get_conn", return_value=self._mock_conn(rows)):
            resp = self.client.get("/symbols")
        assert resp.status_code == 200
        assert "AAPL" in resp.text
        assert "MSFT" in resp.text

    def test_no_results_shows_empty_state(self):
        with patch("admin.get_conn", return_value=self._mock_conn([])):
            resp = self.client.get("/symbols?q=zzznomatch")
        assert resp.status_code == 200
        assert "No symbols found" in resp.text

    def test_sql_includes_feed_url_join_when_query_present(self):
        """
        The SQL executed for a non-empty q MUST reference rss_feeds.feed_url
        so that URL-based searches are possible.
        """
        captured_sql = []

        def fake_execute(sql, params=None):
            captured_sql.append(sql)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.__enter__ = lambda s: s
        mock_cur.__exit__  = MagicMock(return_value=False)
        mock_cur.fetchall.return_value = []
        mock_cur.execute.side_effect = fake_execute
        mock_conn.cursor.return_value = mock_cur

        with patch("admin.get_conn", return_value=mock_conn):
            self.client.get("/symbols?q=globenewswire")

        assert captured_sql, "No SQL was executed"
        sql = captured_sql[0].lower()
        assert "feed_url" in sql, f"SQL must filter on feed_url. Got:\n{sql}"


# ── Feature 2: News keyword search ───────────────────────────────────────────

class TestNewsKeywordSearch:
    """
    GET /symbol/{sym_id}/news?q=<keyword>
    Returns articles whose title OR full_text ILIKE %keyword%.
    """

    def setup_method(self):
        from admin import app
        self.client = TestClient(app)

    def _mock_conn_news(self, total, articles):
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.__enter__ = lambda s: s
        mock_cur.__exit__  = MagicMock(return_value=False)
        # fetchone for COUNT, fetchall for articles
        mock_cur.fetchone.return_value = {"total": total}
        mock_cur.fetchall.return_value = articles
        mock_conn.cursor.return_value = mock_cur
        return mock_conn

    def test_no_keyword_returns_all_articles(self):
        arts = [make_article_row(1, "FDA approves new drug"), make_article_row(2, "Q3 earnings")]
        with patch("admin.get_conn", return_value=self._mock_conn_news(2, arts)):
            resp = self.client.get("/symbol/1/news")
        assert resp.status_code == 200
        assert "FDA approves" in resp.text
        assert "Q3 earnings" in resp.text

    def test_keyword_fda_filters_articles(self):
        arts = [make_article_row(1, "FDA approves new drug")]
        with patch("admin.get_conn", return_value=self._mock_conn_news(1, arts)):
            resp = self.client.get("/symbol/1/news?q=FDA")
        assert resp.status_code == 200
        assert "FDA approves" in resp.text

    def test_keyword_no_match_shows_empty_state(self):
        with patch("admin.get_conn", return_value=self._mock_conn_news(0, [])):
            resp = self.client.get("/symbol/1/news?q=zzznomatch")
        assert resp.status_code == 200
        assert "No news" in resp.text or "empty" in resp.text.lower()

    def test_keyword_search_box_rendered_in_news_tab(self):
        """News tab HTML must include a keyword search input."""
        arts = [make_article_row(1, "Patent granted for new tech")]
        with patch("admin.get_conn", return_value=self._mock_conn_news(1, arts)):
            resp = self.client.get("/symbol/1/news?q=patent")
        assert resp.status_code == 200
        # The response fragment should contain a search input
        assert 'input' in resp.text.lower() or 'search' in resp.text.lower(), \
            "News tab should render a keyword search box"

    def test_keyword_in_full_text_matches(self):
        """Match on full_text even if title doesn't contain keyword."""
        arts = [make_article_row(1, "Company Update", full_text="groundbreaking discovery announced")]
        with patch("admin.get_conn", return_value=self._mock_conn_news(1, arts)):
            resp = self.client.get("/symbol/1/news?q=groundbreaking")
        assert resp.status_code == 200
        assert "Company Update" in resp.text

    def test_sql_filters_on_keyword_when_q_given(self):
        """SQL must apply WHERE title ILIKE or full_text ILIKE when q is set."""
        captured_sql = []

        def fake_execute(sql, params=None):
            captured_sql.append(sql)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.__enter__ = lambda s: s
        mock_cur.__exit__  = MagicMock(return_value=False)
        mock_cur.fetchone.return_value = {"total": 0}
        mock_cur.fetchall.return_value = []
        mock_cur.execute.side_effect = fake_execute
        mock_conn.cursor.return_value = mock_cur

        with patch("admin.get_conn", return_value=mock_conn):
            self.client.get("/symbol/1/news?q=FDA")

        assert captured_sql, "No SQL was executed"
        all_sql = " ".join(captured_sql).lower()
        assert "ilike" in all_sql or "like" in all_sql, \
            f"SQL must use ILIKE/LIKE for keyword. Got:\n{all_sql}"

    def test_pagination_preserves_keyword(self):
        """Pagination buttons must carry the keyword query param."""
        arts = [make_article_row(i, f"FDA news {i}") for i in range(20)]
        with patch("admin.get_conn", return_value=self._mock_conn_news(40, arts)):
            resp = self.client.get("/symbol/1/news?q=FDA&page=1")
        assert resp.status_code == 200
        # Next button URL should include q=FDA
        assert "q=FDA" in resp.text or "q=fda" in resp.text.lower(), \
            "Pagination URLs must preserve the keyword filter"
