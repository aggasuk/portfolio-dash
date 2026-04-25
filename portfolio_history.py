"""
portfolio_history.py
====================
Reconstruct daily NAV time series from trade_journal.json + Bloomberg historical prices.

For each trading day from 2026-01-01 to today:
  1. Replay all trades up to and including that date -> open lots per ticker
  2. Fetch historical close price for each ticker on that date
  3. Compute mkt_value_usd = sum(qty * price * fx * price_factor)
  4. Cost basis = sum(qty * avg_cost * price_factor) at start of day
  5. Realized P&L = sum of all closes through that date

Outputs nav_history.json with daily snapshots.
"""

import json
import os
import sys
import math
from datetime import datetime, timedelta
from collections import defaultdict, deque


def scrub_nan(obj):
    if isinstance(obj, dict):
        return {k: scrub_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [scrub_nan(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

try:
    from xbbg import blp
    import pandas as pd
except ImportError:
    print("ERROR: xbbg + pandas required")
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
JOURNAL_PATH = os.path.join(SCRIPT_DIR, "trade_journal.json")
HISTORY_PATH = os.path.join(SCRIPT_DIR, "nav_history.json")

START_DATE = "2026-01-01"


def load_journal():
    with open(JOURNAL_PATH) as f:
        d = json.load(f)
    return d.get("trades", [])


def replay_to_date(trades, target_date):
    """FIFO replay all trades up to target_date inclusive.
    Returns (open_lots, realized_local_by_ccy)."""
    sorted_trades = sorted(trades, key=lambda t: (t.get("date",""), t.get("id","")))
    lots = defaultdict(deque)
    realized_by_ccy = defaultdict(float)
    meta = {}

    for t in sorted_trades:
        if t.get("date","") > target_date:
            break
        tk = t["ticker"]
        side = t.get("side")
        qty = float(t.get("qty",0) or 0)
        px = float(t.get("price",0) or 0)
        ccy = t.get("ccy","USD")
        pf = t.get("price_factor", 1.0)
        ac = t.get("asset_class","STK")
        if tk not in meta:
            meta[tk] = {"ccy": ccy, "bbg": t.get("bbg"), "price_factor": pf, "asset_class": ac, "multiplier": t.get("multiplier",1)}
        if side in ("buy","open"):
            lots[tk].append({"qty": qty, "price": px, "ccy": ccy, "price_factor": pf})
        elif side == "sell":
            remaining = qty
            while remaining > 0 and lots[tk]:
                lot = lots[tk][0]
                take = min(lot["qty"], remaining)
                realized = (px - lot["price"]) * take * pf
                realized_by_ccy[ccy] += realized
                lot["qty"] -= take
                remaining -= take
                if lot["qty"] <= 1e-9:
                    lots[tk].popleft()
        elif side == "split":
            lots[tk].append({"qty": qty, "price": 0.0, "ccy": ccy, "price_factor": pf})

    return lots, dict(realized_by_ccy), meta


def fetch_history(bbg_tickers, start, end):
    """Fetch historical PX_LAST for all tickers in one BDH call.
    Returns a date-sorted, forward-filled dataframe.
    NOTE: must sort_index before ffill — BDH unions cross-calendar tickers
    in insertion order (recent dates first, then jumbled), so ffill on the
    raw frame would propagate TODAY's prices into older NaN cells."""
    if not bbg_tickers:
        return pd.DataFrame()
    print(f"  BDH for {len(bbg_tickers)} tickers, {start} -> {end}...")
    try:
        df = blp.bdh(bbg_tickers, "PX_LAST", start, end)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]
        # Drop fully-empty rows (BDH sometimes includes non-trading dates with all-NaN)
        df = df.sort_index().dropna(how="all").ffill()
        return df
    except Exception as e:
        print(f"  BDH FAILED: {e}")
        return pd.DataFrame()


def fetch_quote_currencies(bbg_tickers):
    """For each BBG ticker, get its quote currency. Returns dict bbg -> ccy.
    Tickers quoting in GBp (pence) will need px / 100 to get GBP."""
    if not bbg_tickers: return {}
    try:
        ref = blp.bdp(bbg_tickers, ["CRNCY"])
        return {t: str(ref.loc[t]["crncy"]).strip() if t in ref.index else "USD" for t in bbg_tickers}
    except Exception as e:
        print(f"  CRNCY fetch failed: {e}")
        return {}


def fetch_fx_history(currencies, start, end):
    """Fetch USD-quoted FX rates daily — sorted + ffilled."""
    pairs = [f"{c}USD Curncy" for c in currencies if c != "USD"]
    if not pairs:
        return pd.DataFrame()
    try:
        df = blp.bdh(pairs, "PX_LAST", start, end)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0].replace("USD Curncy","") for c in df.columns]
        return df.sort_index().dropna(how="all").ffill()
    except Exception as e:
        print(f"  FX BDH FAILED: {e}")
        return pd.DataFrame()


def main():
    print("\nportfolio_history.py")
    print("=" * 60)

    trades = load_journal()
    print(f"  Journal: {len(trades)} trades")

    # Determine all unique BBG tickers + currencies + ticker meta
    bbg_to_tk = {}
    tk_meta = {}
    for t in trades:
        tk = t["ticker"]
        bbg = t.get("bbg")
        if bbg:
            bbg_to_tk[bbg] = tk
        if tk not in tk_meta:
            tk_meta[tk] = {
                "bbg": bbg, "ccy": t.get("ccy","USD"),
                "price_factor": t.get("price_factor",1.0),
                "asset_class": t.get("asset_class","STK"),
                "multiplier": t.get("multiplier",1),
            }

    today = datetime.now().strftime("%Y-%m-%d")
    bdh_start = START_DATE.replace("-","")
    bdh_end = today.replace("-","")

    print(f"  Date range: {START_DATE} -> {today}")

    # Fetch quote currencies — needed to apply GBp -> GBP /100 conversion
    bbg_list = list(bbg_to_tk.keys())
    quote_ccy = fetch_quote_currencies(bbg_list)
    print(f"  Quote currencies: {quote_ccy}")

    # Fetch all BBG histories in one shot
    px_df = fetch_history(bbg_list, bdh_start, bdh_end)
    if px_df.empty:
        print("\n  ABORT: BDH returned no data — Bloomberg Terminal probably disconnected.")
        print("  Refusing to overwrite nav_history.json.")
        sys.exit(2)

    # Apply GBp -> GBP conversion for tickers quoted in pence
    if not px_df.empty:
        for bbg in list(px_df.columns):
            if quote_ccy.get(bbg) == "GBp":
                px_df[bbg] = px_df[bbg] / 100.0
        # Map BBG columns back to internal tickers
        px_df.columns = [bbg_to_tk.get(c, c) for c in px_df.columns]
        # Already sorted + ffilled in fetch_history (must be in that order)

    # FX history (already sorted + ffilled in fetch_fx_history)
    currencies = sorted(set(m["ccy"] for m in tk_meta.values() if m["ccy"] != "USD"))
    fx_df = fetch_fx_history(currencies, bdh_start, bdh_end)

    # Iterate trading days
    if px_df.empty:
        print("  No price data — aborting")
        return
    snapshots = []
    for date_idx, row in px_df.iterrows():
        date_str = date_idx.strftime("%Y-%m-%d")
        lots, realized, meta = replay_to_date(trades, date_str)

        nav = 0.0
        cost = 0.0
        positions_snap = {}
        for tk, lot_q in lots.items():
            qty = sum(l["qty"] for l in lot_q)
            if qty <= 0: continue
            m = meta.get(tk) or tk_meta.get(tk, {})
            pf = m.get("price_factor", 1.0)
            ccy = m.get("ccy","USD")
            px = row.get(tk, None)
            if pd.isna(px) or px is None:
                # Skip tickers with no price that day; carry-forward might miss day-1
                continue
            fx = 1.0
            if ccy != "USD" and not fx_df.empty and ccy in fx_df.columns:
                fx_row = fx_df.loc[:date_idx, ccy]
                if not fx_row.empty:
                    fx = float(fx_row.iloc[-1])
            mkt_val = qty * float(px) * pf * fx
            lot_cost = sum(l["qty"] * l["price"] for l in lot_q) * pf * fx
            nav += mkt_val
            cost += lot_cost
            positions_snap[tk] = {"qty": qty, "mkt_val_usd": round(mkt_val, 2)}

        # Realized USD: sum across currencies via fx that day (approx — uses date's fx)
        realized_usd = 0.0
        for ccy, v in realized.items():
            if ccy == "USD":
                realized_usd += v
            elif not fx_df.empty and ccy in fx_df.columns:
                fx_row = fx_df.loc[:date_idx, ccy]
                if not fx_row.empty:
                    realized_usd += v * float(fx_row.iloc[-1])

        snapshots.append({
            "date": date_str,
            "nav_securities_usd": round(nav, 2),
            "cost_basis_usd": round(cost, 2),
            "unrealised_pnl_usd": round(nav - cost, 2),
            "realized_pnl_ytd_usd": round(realized_usd, 2),
            "total_pnl_ytd_usd": round(nav - cost + realized_usd, 2),
            "positions": positions_snap,
        })

    # Drop incomplete leading snapshots: BDH may include Jan 1 with sparse data
    # (US stocks don't trade Jan 1). Only keep snapshots once NAV has reached at
    # least 90% of the most-completes day's NAV (i.e. all open tickers priced).
    if snapshots:
        max_nav = max(s["nav_securities_usd"] for s in snapshots[:5])
        threshold = max_nav * 0.5
        while len(snapshots) > 1 and snapshots[0]["nav_securities_usd"] < threshold:
            print(f"  Dropping incomplete leading snapshot: {snapshots[0]['date']} (NAV ${snapshots[0]['nav_securities_usd']:,.0f} < threshold ${threshold:,.0f})")
            snapshots.pop(0)

    # Re-baseline YTD P&L so day 1 = 0:
    #   ytd_pnl_clean = (unrealised(t) - unrealised(jan_1)) + realized_ytd(t)
    if snapshots:
        baseline = snapshots[0]["unrealised_pnl_usd"]
        for s in snapshots:
            s["ytd_pnl_clean_usd"] = round(
                (s["unrealised_pnl_usd"] - baseline) + s["realized_pnl_ytd_usd"], 2
            )

    output = {
        "schema_version": 1,
        "last_updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "start_date": START_DATE,
        "snapshots": snapshots,
    }
    output = scrub_nan(output)
    with open(HISTORY_PATH, "w") as f:
        json.dump(output, f, indent=2, allow_nan=False)
    print(f"\n  Wrote {len(snapshots)} daily snapshots to {HISTORY_PATH}")
    if snapshots:
        first, last = snapshots[0], snapshots[-1]
        print(f"  First: {first['date']}  NAV ${first['nav_securities_usd']:,.0f}  TotalPnL ${first['total_pnl_ytd_usd']:,.0f}")
        print(f"  Last:  {last['date']}  NAV ${last['nav_securities_usd']:,.0f}  TotalPnL ${last['total_pnl_ytd_usd']:,.0f}")


if __name__ == "__main__":
    main()
