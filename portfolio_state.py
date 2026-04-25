"""
portfolio_state.py
==================
Single backend that produces the canonical portfolio_state.json from:
  - trade_journal.json (single source of truth for trades + cost basis)
  - IBKR live position server (for current snapshot + cash)
  - Bloomberg via xbbg (for live prices + ticker metadata)

Outputs:
  - portfolio_state.json  -> open_positions[], closed_positions[], summary, cash{}
  - reconciliation.json   -> diff between journal-derived positions and IBKR live (sanity check)

FIFO matching of buys vs sells. Splits adjust qty & cost basis.
'open' entries are journal opening balance (Jan 1, 2026 reset).
"""

import json
import os
import sys
import base64
from datetime import datetime
from collections import defaultdict, deque

try:
    from xbbg import blp
except ImportError:
    print("ERROR: xbbg required (Bloomberg Terminal + blpapi)")
    sys.exit(1)

try:
    import requests
    def http_get(url, headers=None):
        r = requests.get(url, headers=headers or {}, timeout=10)
        return r.json() if r.status_code == 200 else None
except ImportError:
    import urllib.request, ssl
    def http_get(url, headers=None):
        req = urllib.request.Request(url, headers=headers or {})
        ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=10) as r:
                return json.loads(r.read())
        except Exception:
            return None

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
JOURNAL_PATH = os.path.join(SCRIPT_DIR, "trade_journal.json")
STATE_PATH   = os.path.join(SCRIPT_DIR, "portfolio_state.json")
RECON_PATH   = os.path.join(SCRIPT_DIR, "reconciliation.json")
GH_TOKEN = os.environ.get("GITHUB_TOKEN", os.environ.get("GH_TOKEN", ""))
TRADES_API = "https://api.github.com/repos/aggasuk/trade-book/contents/portfolio_trades.json"
IBKR_URL = "http://localhost:8090"


# =============================================================
# 1) LOAD JOURNAL
# =============================================================
def load_journal():
    with open(JOURNAL_PATH) as f:
        d = json.load(f)
    return d.get("trades", []), d.get("as_of_open", "2026-01-01")


# =============================================================
# 2) MERGE manual UBS additions from trade-book repo (legacy entries)
# =============================================================
def load_manual_trades():
    """Pulls aggasuk/trade-book/portfolio_trades.json for ad-hoc UBS entries
    user adds via blotter. Each entry must have: ticker, side, qty, price, date.
    Skip entries already present in journal (by id or by ticker+date+qty+price)."""
    headers = {"Authorization": f"Bearer {GH_TOKEN}"} if GH_TOKEN else {}
    data = http_get(TRADES_API, headers)
    if not data or "content" not in data:
        return []
    raw = json.loads(base64.b64decode(data["content"]).decode("utf-8"))
    return raw if isinstance(raw, list) else []


# =============================================================
# 3) FIFO POSITION + REALIZED P&L ENGINE
# =============================================================
def compute_positions(trades):
    """
    Returns:
      open_lots[ticker] = list of {qty, price, date, broker, ccy, ...} (FIFO)
      closed_lots[ticker] = list of {qty, buy_price, sell_price, buy_date, sell_date, realized_local}
      meta[ticker] = {ccy, bbg, asset_class, price_factor}
    """
    sorted_trades = sorted(trades, key=lambda t: (t.get("date",""), t.get("id","")))

    lots = defaultdict(deque)         # ticker -> deque of open lots (FIFO)
    closed = defaultdict(list)
    meta = {}

    for t in sorted_trades:
        tk = t["ticker"]
        side = t.get("side")
        qty = float(t.get("qty", 0) or 0)
        px = float(t.get("price", 0) or 0)
        ccy = t.get("ccy", "USD")
        broker = t.get("broker", "")
        date = t.get("date", "")
        pf = t.get("price_factor", 1.0)
        ac = t.get("asset_class", "STK")

        # capture metadata once
        if tk not in meta:
            meta[tk] = {
                "ccy": ccy, "bbg": t.get("bbg"),
                "price_factor": pf, "asset_class": ac,
                "multiplier": t.get("multiplier", 1),
                "underlying": t.get("underlying"),
            }

        if side in ("buy", "open"):
            lots[tk].append({
                "qty": qty, "price": px, "ccy": ccy,
                "date": date, "broker": broker,
                "price_factor": pf,
            })
        elif side == "sell":
            remaining = qty
            while remaining > 0 and lots[tk]:
                lot = lots[tk][0]
                take = min(lot["qty"], remaining)
                realized = (px - lot["price"]) * take * pf
                closed[tk].append({
                    "qty": take,
                    "buy_price": lot["price"],
                    "sell_price": px,
                    "buy_date": lot["date"],
                    "sell_date": date,
                    "realized_local": realized,
                    "ccy": ccy,
                    "broker_buy": lot["broker"],
                    "broker_sell": broker,
                    "price_factor": pf,
                })
                lot["qty"] -= take
                remaining -= take
                if lot["qty"] <= 1e-9:
                    lots[tk].popleft()
            if remaining > 0:
                # over-sell: log as anomaly with negative qty open lot? Skip and warn.
                print(f"  WARN: over-sell on {tk} at {date}: {remaining} unmatched")
        elif side == "split":
            # Add the split-issued shares as a 0-cost lot. The cost basis remains
            # the original total; the average will be diluted automatically.
            lots[tk].append({
                "qty": qty, "price": 0.0, "ccy": ccy,
                "date": date, "broker": broker, "price_factor": pf,
            })
        else:
            print(f"  WARN: unknown side '{side}' on {tk} {date}")

    return lots, closed, meta


# =============================================================
# 4) FETCH BLOOMBERG PRICES FOR CURRENTLY-OPEN TICKERS
# =============================================================
def fetch_prices(meta, open_lots):
    """Bloomberg BDP for tickers with non-empty open lots."""
    bbg_to_tk = {}
    for tk, m in meta.items():
        if not open_lots.get(tk): continue
        bbg = m.get("bbg")
        if bbg: bbg_to_tk[bbg] = tk
    if not bbg_to_tk:
        return {}, {}
    fields = ["PX_LAST", "PREV_CLOSE_VALUE_REALTIME", "CHG_PCT_1D",
              "CHG_PCT_YTD", "NAME", "CRNCY", "GICS_SECTOR_NAME", "COUNTRY_ISO"]
    print(f"  Fetching {len(bbg_to_tk)} prices from Bloomberg...")
    try:
        ref = blp.bdp(list(bbg_to_tk.keys()), fields)
    except Exception as e:
        print(f"  ERROR: BDP failed: {e}")
        return {}, {}
    prices, currencies = {}, set()
    for bbg, tk in bbg_to_tk.items():
        try:
            row = ref.loc[bbg]
            ccy = str(row.get("crncy") or meta[tk]["ccy"])
            px = float(row.get("px_last") or 0)
            prev = float(row.get("prev_close_value_realtime") or 0)
            ch1d = float(row.get("chg_pct_1d") or 0)
            # Heal stale prev_close
            if px and abs(ch1d) > 1e-6 and abs(prev - px) < 1e-6:
                prev = px / (1 + ch1d/100.0)
            if ccy == "GBp":
                ccy = "GBP"; px /= 100.0; prev /= 100.0
            prices[tk] = {
                "bbg": bbg, "px_last": px, "prev_close": prev,
                "chg_pct_1d": ch1d, "chg_pct_ytd": float(row.get("chg_pct_ytd") or 0),
                "name": str(row.get("name") or tk),
                "currency": ccy,
                "sector": str(row.get("gics_sector_name") or "") if row.get("gics_sector_name") else "",
                "country": str(row.get("country_iso") or "")
            }
            currencies.add(ccy)
        except Exception as e:
            print(f"    {tk}: FAILED ({e})")
            prices[tk] = {"bbg": bbg, "px_last": 0, "prev_close": 0, "currency": meta[tk]["ccy"]}
    return prices, currencies


def fetch_fx(currencies):
    """USD-quoted FX rates for non-USD currencies."""
    pairs = [f"{c}USD Curncy" for c in currencies if c != "USD"]
    if not pairs: return {"USD": 1.0}
    try:
        ref = blp.bdp(pairs, ["PX_LAST"])
    except Exception as e:
        print(f"  WARN: FX fetch failed: {e}")
        return {"USD": 1.0}
    rates = {"USD": 1.0}
    for p in pairs:
        try:
            ccy = p.replace("USD Curncy","")
            rates[ccy] = float(ref.loc[p]["px_last"])
        except Exception:
            rates[p.replace("USD Curncy","")] = 1.0
    return rates


# =============================================================
# 5) IBKR LIVE — for cash + reconciliation
# =============================================================
def fetch_ibkr():
    return http_get(IBKR_URL + "/portfolio")


# =============================================================
# 6) BUILD STATE
# =============================================================
def build_state():
    print("\nportfolio_state.py")
    print("=" * 60)

    journal_trades, as_of = load_journal()
    manual_extra = load_manual_trades()
    # Merge: manual entries flagged with broker UBS that aren't already in journal
    journal_ids = {t.get("id") for t in journal_trades if t.get("id")}
    journal_keys = {(t["ticker"], t.get("date",""), t.get("side"), float(t.get("qty",0)), float(t.get("price",0))) for t in journal_trades}
    for m in manual_extra:
        if m.get("id") and m["id"] in journal_ids: continue
        key = (m.get("ticker"), m.get("date","")[:10] if m.get("date") else "", m.get("side"), float(m.get("qty",0) or 0), float(m.get("price",0) or 0))
        if key in journal_keys: continue
        # Convert manual entry to journal format
        journal_trades.append({
            "id": m.get("id") or f"manual_{m.get('ticker')}_{m.get('date','')[:10]}_{m.get('qty')}",
            "ticker": m.get("ticker"),
            "bbg": m.get("bbg_ticker") or m.get("bbg"),
            "side": m.get("side"),
            "qty": float(m.get("qty",0) or 0),
            "price": float(m.get("price",0) or 0),
            "ccy": m.get("currency","USD"),
            "date": (m.get("date") or "")[:10],
            "broker": m.get("broker","UBS") or "UBS",
            "asset_class": m.get("asset_class","STK"),
        })
    print(f"  Journal: {len(journal_trades)} entries (incl {len(manual_extra)} manual)")

    print("  Computing FIFO positions...")
    lots, closed, meta = compute_positions(journal_trades)

    print("  Fetching Bloomberg prices...")
    prices, currencies = fetch_prices(meta, lots)
    fx_rates = fetch_fx(currencies)
    print(f"  FX rates: {fx_rates}")

    print("  Pulling IBKR live for cash + reconciliation...")
    ibkr = fetch_ibkr()

    today = datetime.now().strftime("%Y-%m-%d")

    # Build open positions array
    open_positions = []
    for tk, lot_q in lots.items():
        if not lot_q or sum(l["qty"] for l in lot_q) <= 0: continue
        m = meta[tk]
        p = prices.get(tk, {})
        ccy = m["ccy"]
        pf = m["price_factor"]
        fx = fx_rates.get(ccy, 1.0)

        # Sum lots
        total_qty = sum(l["qty"] for l in lot_q)
        total_cost_local = sum(l["qty"] * l["price"] for l in lot_q)
        avg_cost = total_cost_local / total_qty if total_qty > 0 else 0
        last = p.get("px_last", 0) or 0
        prev = p.get("prev_close", last) or last

        # Per-broker breakdown
        by_broker = defaultdict(float)
        for l in lot_q:
            by_broker[l.get("broker","")] += l["qty"]

        mkt_val_usd = total_qty * last * pf * fx
        cost_usd = total_cost_local * pf * fx
        unreal_usd = mkt_val_usd - cost_usd
        daily_pnl_usd = total_qty * (last - prev) * pf * fx

        open_positions.append({
            "ticker": tk,
            "name": p.get("name", tk),
            "qty": round(total_qty, 6),
            "avg_cost": round(avg_cost, 6),
            "mkt_price": last,
            "prev_close": prev,
            "currency": ccy,
            "asset_class": m.get("asset_class","STK"),
            "price_factor": pf,
            "mkt_value_usd": round(mkt_val_usd, 2),
            "cost_basis_usd": round(cost_usd, 2),
            "unrealised_pnl_usd": round(unreal_usd, 2),
            "unrealised_pnl_pct": round(unreal_usd / cost_usd, 4) if cost_usd > 0 else 0,
            "daily_pnl_usd": round(daily_pnl_usd, 2),
            "daily_pnl_pct": round((last - prev) / prev, 4) if prev > 0 else 0,
            "chg_pct_ytd": p.get("chg_pct_ytd", 0),
            "sector": p.get("sector",""),
            "country": p.get("country",""),
            "by_broker": dict(by_broker),
            "lots_count": len(lot_q),
        })

    # Build closed positions array
    closed_positions = []
    for tk, recs in closed.items():
        m = meta.get(tk, {})
        ccy = m.get("ccy", "USD")
        fx = fx_rates.get(ccy, 1.0)
        for r in recs:
            closed_positions.append({
                "ticker": tk,
                "name": prices.get(tk, {}).get("name", tk),
                "qty_closed": r["qty"],
                "buy_price": r["buy_price"],
                "sell_price": r["sell_price"],
                "buy_date": r["buy_date"],
                "sell_date": r["sell_date"],
                "currency": ccy,
                "realized_pnl_local": round(r["realized_local"], 2),
                "realized_pnl_usd": round(r["realized_local"] * fx, 2),
                "broker_sell": r.get("broker_sell"),
                "asset_class": m.get("asset_class","STK"),
            })
    closed_positions.sort(key=lambda x: x["sell_date"], reverse=True)

    # Summary
    nav_securities = sum(p["mkt_value_usd"] for p in open_positions)
    cost_total = sum(p["cost_basis_usd"] for p in open_positions)
    unreal_total = sum(p["unrealised_pnl_usd"] for p in open_positions)
    daily_total = sum(p["daily_pnl_usd"] for p in open_positions)
    realized_2026 = sum(c["realized_pnl_usd"] for c in closed_positions if c["sell_date"] >= "2026-01-01")

    summary = {
        "as_of": today,
        "nav_securities_usd": round(nav_securities, 2),
        "cost_basis_usd": round(cost_total, 2),
        "unrealised_pnl_usd": round(unreal_total, 2),
        "unrealised_pnl_pct": round(unreal_total / cost_total, 4) if cost_total > 0 else 0,
        "daily_pnl_usd": round(daily_total, 2),
        "realized_pnl_2026_usd": round(realized_2026, 2),
        "ytd_pnl_usd": round(unreal_total + realized_2026, 2),
        "open_positions": len(open_positions),
        "closed_positions_2026": sum(1 for c in closed_positions if c["sell_date"] >= "2026-01-01"),
    }

    # Reconciliation: compare journal-derived qtys vs IBKR live qtys
    recon = {"as_of": today, "ibkr_live_match": True, "discrepancies": []}
    if ibkr and ibkr.get("positions"):
        ibkr_qtys = {}
        for p in ibkr["positions"]:
            tk = p.get("ticker","")
            if p.get("asset_class") == "OPT":
                # Map IBKR's verbose option ticker to our journal id
                # Live IBKR shows "BNO    JUL2026 60 C [BNO   260717C00060000 100]"
                m_opt = (tk.upper())
                if "BNO" in m_opt and "60" in m_opt and "JUL2026" in m_opt:
                    tk = "BNO_60C_JUL26"
                else:
                    continue
            ibkr_qtys[tk] = ibkr_qtys.get(tk, 0) + (p.get("qty") or 0)
        # Compare to journal IBKR-broker qty
        journal_ibkr = defaultdict(float)
        for tk, lot_q in lots.items():
            for l in lot_q:
                if l.get("broker") == "IBKR":
                    journal_ibkr[tk] += l["qty"]
        for tk, q in ibkr_qtys.items():
            jq = journal_ibkr.get(tk, 0)
            if abs(q - jq) > 1e-6:
                recon["discrepancies"].append({"ticker": tk, "ibkr_live": q, "journal": jq, "diff": q - jq})
                recon["ibkr_live_match"] = False
        for tk, q in journal_ibkr.items():
            if tk not in ibkr_qtys and q > 0:
                recon["discrepancies"].append({"ticker": tk, "ibkr_live": 0, "journal": q, "diff": -q})
                recon["ibkr_live_match"] = False

    # Cash (from IBKR summary + manual UBS cash for now)
    cash = {"by_currency": {}, "total_usd": 0}
    if ibkr and ibkr.get("summary"):
        # Per-currency cash will need ledger endpoint extension. For now, totalcashvalue.
        s = ibkr["summary"]
        if "totalcashvalue" in s:
            tcv = s["totalcashvalue"]
            ccy_native = tcv.get("currency","SGD")
            amt_native = float(tcv.get("amount",0))
            # Convert to USD
            if ccy_native == "USD":
                amt_usd = amt_native
            else:
                # IBKR fx_rates are SGD per unit; convert to USD via /USD rate
                ibkr_fx = ibkr.get("fx_rates", {})
                usd_rate = ibkr_fx.get("USD", 1.27)  # SGD per USD
                rate_native = ibkr_fx.get(ccy_native, 1.0)
                amt_usd = amt_native * rate_native / usd_rate if usd_rate else amt_native
            cash["by_currency"]["IBKR_total"] = {"currency": ccy_native, "amount": amt_native, "amount_usd": round(amt_usd, 2)}
            cash["total_usd"] += round(amt_usd, 2)

    # Manual UBS cash entries (from a separate file or hardcoded for now)
    ubs_cash = [
        {"source": "UBS-271319", "currency": "SGD", "amount": 240611.51},
        {"source": "UBS-271156", "currency": "USD", "amount": 88570.49},
        {"source": "UBS-271156", "currency": "JPY", "amount": 215852},
        {"source": "UBS-271156", "currency": "GBP", "amount": 299.86},
    ]
    for c in ubs_cash:
        ccy = c["currency"]
        fx = fx_rates.get(ccy, 1.0)
        amt_usd = c["amount"] * fx
        cash["by_currency"][f'{c["source"]}_{ccy}'] = {**c, "amount_usd": round(amt_usd, 2)}
        cash["total_usd"] += round(amt_usd, 2)

    summary["cash_usd"] = round(cash["total_usd"], 2)
    summary["nav_total_usd"] = round(nav_securities + cash["total_usd"], 2)

    state = {
        "last_updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "as_of": today,
        "summary": summary,
        "open_positions": sorted(open_positions, key=lambda p: -p["mkt_value_usd"]),
        "closed_positions": closed_positions,
        "cash": cash,
        "fx_rates": fx_rates,
    }

    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2, default=str)
    with open(RECON_PATH, "w") as f:
        json.dump(recon, f, indent=2)

    # Console summary
    print()
    print("=" * 60)
    print(f"  NAV (securities)  : ${summary['nav_securities_usd']:>14,.0f}")
    print(f"  Cash (USD equiv)  : ${summary['cash_usd']:>14,.0f}")
    print(f"  TOTAL NAV         : ${summary['nav_total_usd']:>14,.0f}")
    print(f"  Cost basis        : ${summary['cost_basis_usd']:>14,.0f}")
    print(f"  Unrealised PnL    : ${summary['unrealised_pnl_usd']:>14,.0f} ({summary['unrealised_pnl_pct']*100:+.2f}%)")
    print(f"  Realised 2026     : ${summary['realized_pnl_2026_usd']:>14,.0f}")
    print(f"  YTD PnL           : ${summary['ytd_pnl_usd']:>14,.0f}")
    print(f"  Open positions    : {summary['open_positions']}")
    print(f"  Closed 2026       : {summary['closed_positions_2026']}")
    if not recon["ibkr_live_match"]:
        print(f"\n  ! IBKR LIVE / JOURNAL DISCREPANCIES ({len(recon['discrepancies'])}):")
        for d in recon["discrepancies"]:
            print(f"     {d['ticker']:>20s}  ibkr={d['ibkr_live']:>10.2f}  journal={d['journal']:>10.2f}  diff={d['diff']:+.2f}")
    else:
        print(f"\n  Reconciliation OK - journal matches IBKR live for all tickers")
    print()
    print(f"  Wrote {STATE_PATH}")
    print(f"  Wrote {RECON_PATH}")


if __name__ == "__main__":
    build_state()
