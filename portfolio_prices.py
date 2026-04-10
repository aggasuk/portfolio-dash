"""
Portfolio Bloomberg Enrichment
==============================
Fetches live prices from Bloomberg for all portfolio tickers.
Discovers tickers dynamically from:
  1. IBKR price server (localhost:8090) if running
  2. Manual trades in GitHub (aggasuk/trade-book/portfolio_trades.json)
  3. ticker_map.json as fallback

Writes portfolio_prices.json and appends daily NAV snapshot to nav_history.json.

Usage:
    python portfolio_prices.py

Runs manually via Claude Code or GitHub Actions (self-hosted runner + Bloomberg).
"""

import json
import os
import sys
import base64
from datetime import datetime

try:
    from xbbg import blp
except ImportError:
    print("ERROR: xbbg not available. Need Bloomberg Terminal + blpapi.")
    sys.exit(1)

# Use requests if available, fallback to urllib
try:
    import requests
    def http_get(url, headers=None):
        r = requests.get(url, headers=headers or {}, timeout=10)
        if r.status_code == 200:
            return r.json()
        return None
except ImportError:
    import urllib.request, ssl
    def http_get(url, headers=None):
        req = urllib.request.Request(url, headers=headers or {})
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=10) as r:
                return json.loads(r.read())
        except Exception:
            return None

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PRICES_PATH = os.path.join(SCRIPT_DIR, "portfolio_prices.json")
NAV_PATH = os.path.join(SCRIPT_DIR, "nav_history.json")
TICKER_MAP_PATH = os.path.join(SCRIPT_DIR, "ticker_map.json")

GH_TOKEN = os.environ.get("GITHUB_TOKEN", os.environ.get("GH_TOKEN", ""))
TRADES_API = "https://api.github.com/repos/aggasuk/trade-book/contents/portfolio_trades.json"
IBKR_URL = "http://localhost:8090/portfolio"


def discover_tickers():
    """Discover all tickers from IBKR + manual trades + ticker_map."""
    tickers = {}  # ticker -> {"bbg": "...", "ccy": "...", "source": "..."}

    # 1. IBKR positions (if server running)
    print("  Checking IBKR server...")
    ibkr_data = http_get(IBKR_URL)
    if ibkr_data and ibkr_data.get("positions"):
        for p in ibkr_data["positions"]:
            tk = p.get("ticker", "")
            if not tk:
                continue
            # Try to find BBG ticker from ticker_map, otherwise construct it
            ccy = p.get("currency", "USD")
            country = p.get("country", "US")
            exchange = p.get("exchange", "")

            # Guess BBG ticker from IBKR data
            if country == "JP" or "TSE" in exchange or "TSEJ" in exchange:
                bbg = f"{tk} JT Equity"
            elif country == "GB" or "LSE" in exchange:
                bbg = f"{tk} LN Equity"
            elif country == "DE" or "IBIS" in exchange:
                bbg = f"{tk} GR Equity"
            elif country == "CH" or "EBS" in exchange:
                bbg = f"{tk} SW Equity"
            elif country == "TW" or "TWSE" in exchange:
                bbg = f"{tk} TT Equity"
            else:
                bbg = f"{tk} US Equity"

            tickers[tk] = {"bbg": bbg, "ccy": ccy, "source": "ibkr",
                           "name": p.get("name", tk), "qty": p.get("qty", 0),
                           "mkt_value": p.get("mkt_value", 0)}
        print(f"    {len(ibkr_data['positions'])} IBKR positions found")
    else:
        print("    IBKR server not available")

    # 2. Manual trades from GitHub
    print("  Checking manual trades...")
    headers = {"Authorization": f"Bearer {GH_TOKEN}"} if GH_TOKEN else {}
    trades_data = http_get(TRADES_API, headers)
    if trades_data and "content" in trades_data:
        trades = json.loads(base64.b64decode(trades_data["content"]).decode("utf-8"))
        for t in trades:
            tk = t.get("ticker", "")
            if tk and tk not in tickers:
                tickers[tk] = {
                    "bbg": t.get("bbg_ticker", f"{tk} US Equity"),
                    "ccy": t.get("currency", "USD"),
                    "source": "manual",
                    "name": t.get("name", tk),
                }
        print(f"    {len(trades)} manual trades checked")
    else:
        print("    No manual trades found")

    # 3. Ticker map as fallback
    if os.path.exists(TICKER_MAP_PATH):
        with open(TICKER_MAP_PATH) as f:
            tmap = json.load(f)
        for tk, meta in tmap.items():
            if tk not in tickers:
                tickers[tk] = {
                    "bbg": meta.get("bbg", f"{tk} US Equity"),
                    "ccy": meta.get("ccy", "USD"),
                    "source": "ticker_map",
                    "name": meta.get("name", tk),
                }

    return tickers


def fetch_prices(tickers):
    """Fetch reference data from Bloomberg for all tickers."""
    if not tickers:
        return {}

    fields = [
        "PX_LAST", "PREV_CLOSE_VALUE_REALTIME", "CHG_PCT_1D",
        "CHG_PCT_YTD", "NAME", "CRNCY", "GICS_SECTOR_NAME",
        "COUNTRY_ISO"
    ]

    bbg_to_short = {v["bbg"]: k for k, v in tickers.items()}
    bbg_list = list(bbg_to_short.keys())

    print(f"  Fetching {len(bbg_list)} tickers from Bloomberg...")
    try:
        ref = blp.bdp(bbg_list, fields)
    except Exception as e:
        print(f"  ERROR: Bloomberg BDP failed: {e}")
        return {}

    results = {}
    for bbg_ticker, short_ticker in bbg_to_short.items():
        try:
            row = ref.loc[bbg_ticker]
            ccy = str(row.get("crncy", "USD") or "USD")

            px_last = float(row.get("px_last", 0) or 0)
            prev_close = float(row.get("prev_close_value_realtime", 0) or 0)

            # Handle GBp (pence) → GBP
            if ccy == "GBp":
                ccy = "GBP"
                px_last /= 100.0
                prev_close /= 100.0

            results[short_ticker] = {
                "bbg_ticker": bbg_ticker,
                "px_last": px_last,
                "prev_close": prev_close,
                "chg_pct_1d": float(row.get("chg_pct_1d", 0) or 0),
                "chg_pct_ytd": float(row.get("chg_pct_ytd", 0) or 0),
                "name": str(row.get("name", short_ticker) or short_ticker),
                "currency": ccy,
                "sector": str(row.get("gics_sector_name", "nan") or "nan"),
                "country": str(row.get("country_iso", "--") or "--"),
            }
            print(f"    {short_ticker:8s}: {px_last:>10.2f} {ccy}  ({results[short_ticker]['name'][:30]})")
        except Exception as e:
            print(f"    {short_ticker:8s}: FAILED ({e})")

    return results


def fetch_fx_rates(currencies):
    """Fetch FX rates to USD from Bloomberg."""
    pairs = []
    for ccy in currencies:
        if ccy == "USD":
            continue
        pairs.append(f"{ccy}USD Curncy")

    if not pairs:
        return {}

    print(f"  Fetching FX rates: {', '.join(pairs)}")
    try:
        ref = blp.bdp(pairs, ["PX_LAST"])
    except Exception as e:
        print(f"  ERROR: FX fetch failed: {e}")
        return {}

    rates = {}
    for pair in pairs:
        try:
            rate = float(ref.loc[pair]["px_last"])
            ccy = pair.replace("USD Curncy", "")
            rates[ccy + "USD"] = rate
            print(f"    {ccy}USD: {rate}")
        except Exception as e:
            print(f"    {pair}: FAILED ({e})")

    return rates


def compute_nav(tickers_info, prices, fx_rates, ibkr_data):
    """Compute total portfolio NAV."""
    nav = 0
    cost = 0
    daily_pnl = 0
    positions = {}

    # IBKR positions (use IBKR's own values, converted to USD)
    if ibkr_data and ibkr_data.get("positions"):
        usd_rate = ibkr_data.get("fx_rates", {}).get("USD", 1.27)
        for p in ibkr_data["positions"]:
            tk = p.get("ticker", "")
            base_val = p.get("base_mkt_value", 0)
            mkt_val_usd = base_val / usd_rate if usd_rate else 0
            base_cost = p.get("base_avg_cost", 0) * p.get("qty", 0)
            cost_usd = base_cost / usd_rate if usd_rate else 0
            unreal_usd = p.get("base_unrealised_pnl", 0) / usd_rate if usd_rate else 0

            nav += mkt_val_usd
            cost += cost_usd
            daily_pnl += unreal_usd  # approximate
            if tk:
                positions[tk] = {"qty": p.get("qty", 0), "mkt_val_usd": round(mkt_val_usd, 2)}

    # Manual positions (from prices)
    for tk, info in tickers_info.items():
        if info["source"] != "manual":
            continue
        if tk in positions:
            continue  # already from IBKR
        p = prices.get(tk, {})
        last = p.get("px_last", 0)
        if last <= 0:
            continue
        ccy = p.get("currency", "USD")
        fx = 1.0
        if ccy != "USD":
            fx = fx_rates.get(ccy + "USD", 1.0)
        # Would need qty from trades — skip for now unless we have it
        qty = info.get("qty", 0)
        if qty > 0:
            mkt_val = qty * last * fx
            nav += mkt_val
            positions[tk] = {"qty": qty, "mkt_val_usd": round(mkt_val, 2)}

    return round(nav, 2), round(cost, 2), round(daily_pnl, 2), positions


def append_nav_snapshot(nav, cost, daily_pnl, positions):
    """Append today's NAV snapshot to nav_history.json."""
    history = {"snapshots": []}
    if os.path.exists(NAV_PATH):
        with open(NAV_PATH) as f:
            history = json.load(f)

    today = datetime.now().strftime("%Y-%m-%d")

    # Fetch SPX close
    spx = 0
    try:
        ref = blp.bdp("SPX Index", "PX_LAST")
        spx = float(ref.iloc[0, 0])
    except Exception:
        pass

    # Remove existing snapshot for today (replace)
    history["snapshots"] = [s for s in history["snapshots"] if s.get("date") != today]

    history["snapshots"].append({
        "date": today,
        "nav": nav,
        "cost": cost,
        "daily_pnl": daily_pnl,
        "positions": positions,
        "spx_close": spx,
    })

    history["snapshots"].sort(key=lambda s: s["date"])

    with open(NAV_PATH, "w") as f:
        json.dump(history, f, indent=2)
    print(f"  NAV snapshot: ${nav:,.2f} (SPX: {spx:.2f})")


def main():
    print("\nPortfolio Bloomberg Enrichment")
    print("=" * 50)

    # Discover tickers dynamically
    print("\n  Discovering tickers...")
    tickers = discover_tickers()
    if not tickers:
        print("  No tickers found, exiting.")
        return
    print(f"\n  {len(tickers)} unique tickers: {', '.join(sorted(tickers.keys()))}")

    # Fetch Bloomberg prices
    prices = fetch_prices(tickers)

    # Determine currencies
    currencies = set()
    for p in prices.values():
        ccy = p.get("currency", "USD")
        if ccy != "USD":
            currencies.add(ccy)
    for info in tickers.values():
        ccy = info.get("ccy", "USD")
        if ccy != "USD":
            currencies.add(ccy)

    fx_rates = fetch_fx_rates(currencies)

    # Write portfolio_prices.json
    output = {
        "last_updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "prices": prices,
        "fx_rates": fx_rates,
    }
    with open(PRICES_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Saved {PRICES_PATH}")

    # Compute and append NAV
    ibkr_data = http_get(IBKR_URL)
    nav, cost, daily_pnl, positions = compute_nav(tickers, prices, fx_rates, ibkr_data)
    append_nav_snapshot(nav, cost, daily_pnl, positions)

    print("\n  Done.")


if __name__ == "__main__":
    main()
