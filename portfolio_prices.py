"""
Portfolio Bloomberg Enrichment
==============================
Fetches live prices, FX rates, and metadata from Bloomberg for all portfolio tickers.
Writes portfolio_prices.json and appends to nav_history.json.

Usage:
    python portfolio_prices.py              # fetch + write JSONs
    python portfolio_prices.py --nav-only   # just append NAV snapshot (skip price fetch)

Runs on self-hosted GitHub Actions runner or manually via Claude Code.
Requires Bloomberg Terminal + blpapi.
"""

import json
import os
import sys
import base64
import requests
from datetime import datetime

try:
    from xbbg import blp
except ImportError:
    print("ERROR: xbbg not available. Need Bloomberg Terminal + blpapi.")
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PRICES_PATH = os.path.join(SCRIPT_DIR, "portfolio_prices.json")
NAV_PATH = os.path.join(SCRIPT_DIR, "nav_history.json")
TICKER_MAP_PATH = os.path.join(SCRIPT_DIR, "ticker_map.json")

# GitHub API for reading trades
GH_TOKEN = os.environ.get("GITHUB_TOKEN", os.environ.get("GH_TOKEN", ""))
TRADES_API = "https://api.github.com/repos/aggasuk/trade-book/contents/portfolio_trades.json"


def load_trades():
    """Load trades from GitHub trade-book repo."""
    headers = {"Authorization": f"Bearer {GH_TOKEN}"} if GH_TOKEN else {}
    r = requests.get(TRADES_API, headers=headers)
    if r.status_code != 200:
        print(f"  WARN: Could not load trades from GitHub ({r.status_code})")
        return []
    data = r.json()
    content = base64.b64decode(data["content"]).decode("utf-8")
    return json.loads(content)


def load_ticker_map():
    """Load ticker map from local file."""
    if os.path.exists(TICKER_MAP_PATH):
        with open(TICKER_MAP_PATH) as f:
            return json.load(f)
    return {}


def get_unique_tickers(trades, ticker_map):
    """Get unique tickers with their Bloomberg IDs."""
    tickers = {}
    for t in trades:
        tk = t["ticker"]
        if tk not in tickers:
            bbg = t.get("bbg_ticker") or ticker_map.get(tk, {}).get("bbg", "")
            if bbg:
                tickers[tk] = bbg
    return tickers


def fetch_prices(bbg_tickers):
    """Fetch reference data from Bloomberg for all tickers."""
    if not bbg_tickers:
        return {}

    fields = [
        "PX_LAST", "PREV_CLOSE_VALUE_REALTIME", "CHG_PCT_1D",
        "CHG_PCT_YTD", "NAME", "CRNCY", "GICS_SECTOR_NAME",
        "COUNTRY_ISO", "MARKET_SECTOR_DES"
    ]

    tickers_list = list(bbg_tickers.values())
    print(f"  Fetching {len(tickers_list)} tickers from Bloomberg...")

    try:
        ref = blp.bdp(tickers_list, fields)
    except Exception as e:
        print(f"  ERROR: Bloomberg BDP failed: {e}")
        return {}

    results = {}
    for short_ticker, bbg_ticker in bbg_tickers.items():
        try:
            row = ref.loc[bbg_ticker]
            results[short_ticker] = {
                "bbg_ticker": bbg_ticker,
                "px_last": float(row.get("px_last", 0) or 0),
                "prev_close": float(row.get("prev_close_value_realtime", 0) or 0),
                "chg_pct_1d": float(row.get("chg_pct_1d", 0) or 0),
                "chg_pct_ytd": float(row.get("chg_pct_ytd", 0) or 0),
                "name": str(row.get("name", short_ticker) or short_ticker),
                "currency": str(row.get("crncy", "USD") or "USD"),
                "sector": str(row.get("gics_sector_name", "--") or "--"),
                "country": str(row.get("country_iso", "--") or "--"),
            }
            print(f"    {short_ticker}: {results[short_ticker]['px_last']} {results[short_ticker]['currency']}")
        except Exception as e:
            print(f"    {short_ticker}: FAILED ({e})")

    return results


def fetch_fx_rates(currencies):
    """Fetch FX rates to USD from Bloomberg."""
    fx_pairs = []
    for ccy in currencies:
        if ccy == "USD":
            continue
        fx_pairs.append(f"{ccy}USD Curncy")

    if not fx_pairs:
        return {}

    print(f"  Fetching FX rates: {', '.join(fx_pairs)}")
    try:
        ref = blp.bdp(fx_pairs, ["PX_LAST"])
    except Exception as e:
        print(f"  ERROR: FX fetch failed: {e}")
        return {}

    rates = {}
    for pair in fx_pairs:
        try:
            rate = float(ref.loc[pair]["px_last"])
            ccy = pair.replace("USD Curncy", "").replace(" ", "")
            rates[ccy + "USD"] = rate
            print(f"    {ccy}USD: {rate}")
        except Exception as e:
            print(f"    {pair}: FAILED ({e})")

    return rates


def compute_nav(trades, prices, fx_rates):
    """Compute total portfolio NAV from trades and prices."""
    grouped = {}
    for t in trades:
        tk = t["ticker"]
        if tk not in grouped:
            grouped[tk] = {"net_qty": 0, "total_cost": 0, "ccy": t.get("currency", "USD")}
        qty = float(t.get("qty", 0))
        px = float(t.get("price", 0))
        if t["side"] == "buy":
            grouped[tk]["net_qty"] += qty
            grouped[tk]["total_cost"] += qty * px
        else:
            avg = grouped[tk]["total_cost"] / grouped[tk]["net_qty"] if grouped[tk]["net_qty"] > 0 else px
            grouped[tk]["net_qty"] -= qty
            grouped[tk]["total_cost"] = avg * grouped[tk]["net_qty"] if grouped[tk]["net_qty"] > 0 else 0

    nav = 0
    cost = 0
    daily_pnl = 0
    positions = {}

    for tk, pos in grouped.items():
        if pos["net_qty"] <= 0:
            continue
        p = prices.get(tk, {})
        last = p.get("px_last", 0)
        prev = p.get("prev_close", last)
        ccy = pos["ccy"]

        fx = 1.0
        if ccy != "USD":
            fx = fx_rates.get(ccy + "USD", 1.0)

        mkt_val = pos["net_qty"] * last * fx
        cost_val = pos["total_cost"] * fx
        daily = pos["net_qty"] * (last - prev) * fx

        nav += mkt_val
        cost += cost_val
        daily_pnl += daily
        positions[tk] = {"qty": pos["net_qty"], "mkt_val_usd": round(mkt_val, 2)}

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

    # Sort by date
    history["snapshots"].sort(key=lambda s: s["date"])

    with open(NAV_PATH, "w") as f:
        json.dump(history, f, indent=2)
    print(f"  NAV snapshot saved: ${nav:,.2f} (SPX: {spx:.2f})")


def main():
    print("\nPortfolio Bloomberg Enrichment")
    print("=" * 50)

    # Load trades
    print("  Loading trades from GitHub...")
    trades = load_trades()
    if not trades:
        print("  No trades found, exiting.")
        return
    print(f"  {len(trades)} trades loaded")

    ticker_map = load_ticker_map()
    bbg_tickers = get_unique_tickers(trades, ticker_map)
    print(f"  {len(bbg_tickers)} unique tickers: {', '.join(bbg_tickers.keys())}")

    # Fetch prices
    prices = fetch_prices(bbg_tickers)

    # Determine currencies needed
    currencies = set()
    for tk, p in prices.items():
        ccy = p.get("currency", "USD")
        if ccy != "USD":
            currencies.add(ccy)
    # Also add from ticker_map
    for tk in bbg_tickers:
        ccy = ticker_map.get(tk, {}).get("ccy", "USD")
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
    nav, cost, daily_pnl, positions = compute_nav(trades, prices, fx_rates)
    append_nav_snapshot(nav, cost, daily_pnl, positions)

    print("\n  Done.")


if __name__ == "__main__":
    main()
