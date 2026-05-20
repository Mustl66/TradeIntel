import sys
import logging
import requests
from typing import List

# Logging auf stderr konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# ─── Bibliotheken laden ──────────────────────────────────────────────
try:
    import pandas as pd
    from tradingview_screener import Query, col
except ImportError as e:
    print(f"System-Fehler beim Importieren: {e}", file=sys.stderr)
    print(
        "Fehler: 'pandas' oder 'tradingview_screener' ist nicht installiert.\n"
        "Bitte ausführen: pip install pandas tradingview-screener rookiepy",
        file=sys.stderr
    )
    sys.exit(1)

# ─── Cookie-Initialisierung (optional) ────────────────────────────────
TV_COOKIES = None


def load_session_cookies():
    """Versucht TradingView-Cookies aus dem Browser zu laden."""
    global TV_COOKIES
    try:
        import rookiepy
    except ImportError:
        logger.info("rookiepy nicht installiert. Verwende öffentliche Daten.")
        return

    domains = ["tradingview.com", ".tradingview.com"]
    browsers = [
        (rookiepy.firefox, "Firefox"),
        (rookiepy.chrome, "Chrome"),
        (rookiepy.edge, "Edge")
    ]

    for browser_fn, name in browsers:
        try:
            cookies = browser_fn(domains)
            TV_COOKIES = rookiepy.to_cookiejar(cookies)
            logger.info(f"TradingView Cookies erfolgreich aus {name} geladen.")
            return
        except Exception:
            continue
    logger.info("Keine aktive TradingView-Browsersitzung gefunden. Verwende zeitverzögerte öffentliche Daten.")


# ─── Lokaler Symbol-Fetcher (Umgeht v3-Importfehler) ──────────────────
def local_get_all_symbols(market: str = "america") -> List[str]:
    """Ruft die vollständige Ticker-Liste direkt von der TradingView API ab."""
    try:
        url = f"https://scanner.tradingview.com/{market}/scan"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json().get('data', [])
        # Liefert Symbole im Format 'EXCHANGE:TICKER' (z.B. 'NASDAQ:AAPL')
        return [item['s'] for item in data if 's' in item]
    except Exception as e:
        logger.error(f"Fehler beim direkten Abruf der Ticker-Liste: {e}")
        return []


# ─── Core-Logik ─────────────────────────────────────────────────────
def fetch_all_exchange_stocks(exchanges: List[str]):
    """Holt alle Aktien über die lokale Symbolliste und fragt Details in Chunks ab."""
    load_session_cookies()

    # 1. Alle US-Symbole abrufen (z.B. 'NASDAQ:AAPL', 'NYSE:T')
    logger.info("Rufe die vollständige Ticker-Liste von TradingView ab...")
    all_symbols = local_get_all_symbols("america")

    if not all_symbols:
        logger.error("Keine Symbole vom Server erhalten.")
        return

    logger.info(f"{len(all_symbols)} US-Gesamtsymbole geladen.")

    for exchange in exchanges:
        exchange_upper = exchange.upper()
        output_filename = f"{exchange_upper.lower()}_symbols.txt"

        logger.info(f"Filtere Symbole für Börse: {exchange_upper}")

        # Lokales Filtern nach Börse
        selected_tickers = [s for s in all_symbols if s.startswith(f"{exchange_upper}:")]

        if not selected_tickers:
            print(f"❌ Keine Symbole für {exchange_upper} in der Gesamtliste gefunden.", file=sys.stdout)
            continue

        logger.info(f"{len(selected_tickers)} Symbole für {exchange_upper} gefunden. Starte Detail-Abruf...")

        symbols_found = []
        chunk_size = 500

        # 2. Details (Name, Beschreibung) in Chunks von 500 abrufen
        for i in range(0, len(selected_tickers), chunk_size):
            chunk = selected_tickers[i:i + chunk_size]
            current_page = (i // chunk_size) + 1
            total_pages = (len(selected_tickers) + chunk_size - 1) // chunk_size

            logger.info(f"Frage Details ab: Batch {current_page}/{total_pages}...")

            try:
                # Aufbau der Abfrage
                q = Query().select('name', 'description', 'exchange').limit(chunk_size)

                # Extrem robuster Fallback-Mechanismus für set_tickers()
                try:
                    q = q.set_tickers(*chunk)
                except Exception:
                    try:
                        q = q.set_tickers(chunk)
                    except Exception:
                        # Letzter Fallback über die "isin"-Filterung
                        symbols_only = [t.split(':')[1] for t in chunk]
                        q = q.where(col('name').isin(symbols_only))

                res = q.get_scanner_data(cookies=TV_COOKIES)
                if res is None:
                    continue

                total, df = res
                if df is None or df.empty:
                    continue

                for _, row in df.iterrows():
                    sym_exchange = row.get('exchange', exchange_upper)
                    sym_ticker = row.get('name', 'N/A')
                    sym_name = row.get('description', 'N/A')
                    symbols_found.append(f"{sym_exchange} - {sym_ticker} - {sym_name}")

            except Exception as e:
                logger.error(f"Fehler bei Batch {current_page} für {exchange_upper}: {e}")
                continue

        # 3. Ergebnisse speichern
        if symbols_found:
            try:
                # Alphabetisch sortieren
                symbols_found.sort()

                with open(output_filename, "w", encoding="utf-8") as f:
                    for line in symbols_found:
                        f.write(line + "\n")

                print(f"\n✅ {exchange_upper} erfolgreich abgeschlossen!", file=sys.stdout)
                print(f"📊 Gesamtanzahl gefundener Symbole: {len(symbols_found)}", file=sys.stdout)
                print(f"📁 Datei gespeichert unter: {output_filename}", file=sys.stdout)
                print("-" * 50, file=sys.stdout)
            except Exception as e:
                logger.error(f"Fehler beim Schreiben der Datei {output_filename}: {e}")
        else:
            print(f"❌ Keine Details für {exchange_upper} gefunden.", file=sys.stdout)


# ─── Startblock ──────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1:
        requested_exchanges = [arg.strip().upper() for arg in sys.argv[1:]]
    else:
        logger.info("Keine Börse angegeben. Verwende standardmäßig NASDAQ.")
        requested_exchanges = ["NASDAQ"]

    fetch_all_exchange_stocks(requested_exchanges)