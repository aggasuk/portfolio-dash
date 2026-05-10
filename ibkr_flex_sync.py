"""
ibkr_flex_sync.py
=================
Syncs IBKR trades from Flex Web Service into trade_journal.json.

  - SendRequest -> ReferenceCode
  - GetStatement -> XML with <Trade> elements
  - Parse, map to journal schema, dedupe (by tradeID OR by ticker+date+side+qty+price)
  - Append new entries to journal

Required env vars:
  IBKR_FLEX_TOKEN     - Flex Web Service token
  IBKR_FLEX_QUERY_ID  - Activity Flex Query ID
"""

import os
import sys
import json
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
JOURNAL_PATH = os.path.join(SCRIPT_DIR, "trade_journal.json")
# Cost-basis reset date: trades on/after this date are appended; earlier trades
# are already represented by open_* opening-balance entries dated this same day.
# Including pre-cutoff Flex trades double-counts them on top of the opening balance.
COST_BASIS_CUTOFF = "2026-01-01"

TOKEN = os.environ.get("IBKR_FLEX_TOKEN")
QUERY = os.environ.get("IBKR_FLEX_QUERY_ID")
SEND_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest"
GET_URL  = "https://gdcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement"

# Bloomberg ticker suffix by exchange/listing prefix or symbol pattern
def bbg_for_stock(symbol, currency):
    if not symbol:
        return None
    sym = symbol.strip()
    # Ticker like "1615.T", "AZN.L" -> map to JT/LN
    if "." in sym:
        root, suffix = sym.rsplit(".", 1)
        sfx_map = {"T": "JT", "L": "LN", "AS": "NA", "DE": "GR", "PA": "FP", "MI": "IM", "MC": "SM", "SS": "CH", "HK": "HK", "TO": "CN"}
        s = sfx_map.get(suffix.upper())
        if s:
            return f"{root} {s} Equity"
    # Numeric (Japanese listings) â€” assume JT
    if sym.isdigit():
        return f"{sym} JT Equity"
    # Default: US listing
    return f"{sym} US Equity"


def bbg_for_option(underlying, expiry, strike, put_call):
    """Format e.g. 'BNO US 07/17/26 C60 Equity'."""
    if not (underlying and expiry and strike and put_call):
        return None
    try:
        d = datetime.strptime(expiry, "%Y%m%d")
    except ValueError:
        return None
    mmddyy = d.strftime("%m/%d/%y")
    return f"{underlying} US {mmddyy} {put_call.upper()}{int(float(strike))} Equity"


def journal_ticker(asset_cat, symbol, underlying, strike, expiry, put_call):
    """Internal ticker name for the trade journal."""
    if asset_cat == "OPT" and underlying and expiry and strike and put_call:
        d = datetime.strptime(expiry, "%Y%m%d")
        return f"{underlying}_{int(float(strike))}{put_call.upper()}_{d.strftime('%b%y').upper()}"
    if symbol and "." in symbol:
        root, suffix = symbol.rsplit(".", 1)
        if suffix.upper() in {"T", "L", "AS", "DE", "PA", "MI", "MC", "SS", "HK", "TO"}:
            return root
    return symbol


def fetch_flex_xml():
    if not TOKEN or not QUERY:
        print("ERROR: IBKR_FLEX_TOKEN and IBKR_FLEX_QUERY_ID env vars required")
        sys.exit(1)
    print(f"  SendRequest q={QUERY}...")
    send_full = f"{SEND_URL}?t={TOKEN}&q={QUERY}&v=3"
    with urllib.request.urlopen(send_full, timeout=30) as r:
        body = r.read()
    x = ET.fromstring(body)
    if x.findtext("Status") != "Success":
        print(f"  SendRequest failed: {x.findtext('ErrorMessage') or body.decode()[:300]}")
        sys.exit(1)
    ref = x.findtext("ReferenceCode")
    print(f"  ReferenceCode: {ref}")
    # Poll for the statement (IBKR processes async; usually ready in seconds)
    get_full = f"{GET_URL}?t={TOKEN}&q={ref}&v=3"
    for attempt in range(12):
        time.sleep(3 if attempt == 0 else 5)
        with urllib.request.urlopen(get_full, timeout=60) as r:
            body = r.read()
        # If the response is a FlexStatementResponse with Status=Warn, statement still pending.
        try:
            x = ET.fromstring(body)
        except ET.ParseError:
            x = None
        if x is not None and x.tag == "FlexStatementResponse":
            status = x.findtext("Status") or ""
            if status == "Success":
                # Old/unexpected â€” fall through to assume body is data
                break
            print(f"  Pending (attempt {attempt+1}/12)...")
            continue
        # If we got <FlexQueryResponse>, this is the data
        if x is not None and x.tag == "FlexQueryResponse":
            return body
        break
    return body


def parse_trades(xml_bytes):
    """Returns list of trade dicts in journal format."""
    root = ET.fromstring(xml_bytes)
    out = []
    for trade in root.iter("Trade"):
        a = trade.attrib
        bs = a.get("buySell", "").upper()
        if bs not in ("BUY", "SELL"):
            continue
        side = "buy" if bs == "BUY" else "sell"
        qty = abs(float(a.get("quantity", 0) or 0))
        if qty <= 0:
            continue
        # Skip FX conversions (IBKR auto-converts wires; symbol is XXX.YYY currency pair).
        # These come through as fake STK trades but are just cash conversions, not positions.
        sym_check = (a.get("symbol", "") or "").upper().strip()
        if len(sym_check) == 7 and sym_check[3] == "." and sym_check[:3].isalpha() and sym_check[4:].isalpha():
            continue
        # Also skip explicit CASH/FX asset categories
        if a.get("assetCategory", "") in ("CASH", "FX", "FOREX"):
            continue
        price = float(a.get("tradePrice", 0) or 0)
        ccy = a.get("currency", "USD")
        date_raw = a.get("tradeDate", "")
        try:
            date = datetime.strptime(date_raw, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            date = date_raw
        asset_cat = a.get("assetCategory", "STK")
        symbol = a.get("symbol", "")
        underlying = a.get("underlyingSymbol", symbol)
        strike = a.get("strike", "")
        expiry = a.get("expiry", "")
        put_call = a.get("putCall", "")
        trade_id = a.get("tradeID", "")
        conid = a.get("conid", "")

        tk = journal_ticker(asset_cat, symbol, underlying, strike, expiry, put_call)
        if asset_cat == "OPT":
            bbg = bbg_for_option(underlying, expiry, strike, put_call)
            ac = "OPT"
            pf = float(a.get("multiplier", 100) or 100)
        elif asset_cat == "BOND":
            # Use CUSIP if available; the conid alone won't fetch BBG. User may
            # need to maintain a manual conid->bbg map. Fallback: skip BBG.
            bbg = None
            ac = "BOND"
            pf = 0.01
        else:
            bbg = bbg_for_stock(symbol, ccy)
            ac = "STK"
            pf = 1.0

        entry = {
            "id": f"flex_{trade_id}",
            "date": date,
            "side": side,
            "ticker": tk,
            "bbg": bbg,
            "qty": qty,
            "price": price,
            "ccy": ccy,
            "broker": "IBKR",
            "asset_class": ac,
            "price_factor": pf,
            "ibkr_trade_id": trade_id,
            "ibkr_conid": conid,
        }
        if asset_cat == "OPT":
            entry["underlying"] = underlying
            entry["strike"] = float(strike) if strike else None
            entry["expiry"] = datetime.strptime(expiry, "%Y%m%d").strftime("%Y-%m-%d") if expiry else None
            entry["right"] = put_call
            entry["multiplier"] = pf
        out.append(entry)
    return out


def main():
    print("\nibkr_flex_sync.py")
    print("=" * 50)

    xml = fetch_flex_xml()
    flex_trades = parse_trades(xml)
    print(f"  Flex returned {len(flex_trades)} trades")

    if not os.path.exists(JOURNAL_PATH):
        print(f"  ERROR: journal not found at {JOURNAL_PATH}")
        sys.exit(1)

    with open(JOURNAL_PATH) as f:
        journal = json.load(f)

    existing = journal.get("trades", [])
    # Dedupe pipeline (in order, first match wins):
    #   (a) ibkr_trade_id           — canonical IBKR fill ID, exact
    #   (b) ibkr_conid + fill key   — instrument-stable across ticker spellings
    #                                  (catches bond/option spelling mismatches IF
    #                                   both sides recorded a conid)
    #   (c) ticker + fill key       — legacy composite (manual entries with same
    #                                  ticker spelling)
    #   (d) ccy + fill key          — fingerprint dedup (catches bond/option
    #                                  spelling mismatches even when manual entry
    #                                  has no conid). 6-decimal price match makes
    #                                  false positives extremely unlikely; we log
    #                                  matches as a warning so user can audit.
    # fill key = (date, side, qty[4dp], price[6dp])
    def _fkey(t):
        return ((t.get("date") or "")[:10], t.get("side"),
                round(float(t.get("qty", 0) or 0), 4),
                round(float(t.get("price", 0) or 0), 6))

    existing_trade_ids = {t.get("ibkr_trade_id") for t in existing if t.get("ibkr_trade_id")}
    existing_by_conid = {
        (t.get("ibkr_conid"), *_fkey(t))
        for t in existing if t.get("ibkr_conid")
    }
    existing_by_ticker = {
        (t.get("ticker"), *_fkey(t)) for t in existing
    }
    # Map ccy-fingerprint -> existing entry (so we can show what we matched)
    existing_by_ccy = {}
    for t in existing:
        key = (t.get("ccy"), *_fkey(t))
        existing_by_ccy.setdefault(key, []).append(t)

    appended = 0
    skipped_id = skipped_conid = skipped_comp = skipped_fp = skipped_precutoff = 0
    fingerprint_matches = []  # (flex_entry, matched_existing_entry)
    for ft in flex_trades:
        if (ft.get("date") or "") < COST_BASIS_CUTOFF:
            skipped_precutoff += 1
            continue
        if ft["ibkr_trade_id"] in existing_trade_ids:
            skipped_id += 1
            continue
        fkey = _fkey(ft)
        # (b) conid + fill key
        if ft.get("ibkr_conid") and (ft["ibkr_conid"], *fkey) in existing_by_conid:
            skipped_conid += 1
            continue
        # (c) ticker + fill key
        if (ft["ticker"], *fkey) in existing_by_ticker:
            skipped_comp += 1
            continue
        # (d) ccy fingerprint — catches ticker-spelling mismatches
        ccy_key = (ft.get("ccy"), *fkey)
        ccy_match = existing_by_ccy.get(ccy_key)
        if ccy_match:
            # Same ccy + same date + same side + same qty (4dp) + same price (6dp)
            # is virtually unique — treat as duplicate but log for audit.
            skipped_fp += 1
            fingerprint_matches.append((ft, ccy_match[0]))
            continue
        existing.append(ft)
        appended += 1

    print(f"  Appended: {appended}")
    print(f"  Skipped (matched IBKR trade_id): {skipped_id}")
    print(f"  Skipped (matched conid + fill key): {skipped_conid}")
    print(f"  Skipped (matched ticker + fill key): {skipped_comp}")
    print(f"  Skipped (matched ccy fingerprint — likely ticker-spelling mismatch): {skipped_fp}")
    print(f"  Skipped (pre-cutoff < {COST_BASIS_CUTOFF}): {skipped_precutoff}")
    if fingerprint_matches:
        print()
        print(f"  ! Fingerprint dedup matched {len(fingerprint_matches)} flex trade(s) to existing journal entries")
        print(f"    with mismatched ticker spellings — review and consider unifying the ticker names:")
        for ft, ex in fingerprint_matches:
            print(f"     flex: id={ft.get('id')}  ticker={ft.get('ticker')!r}  conid={ft.get('ibkr_conid')}")
            print(f"     jrnl: id={ex.get('id')}  ticker={ex.get('ticker')!r}  conid={ex.get('ibkr_conid') or '(none)'}")
            print(f"           {ft.get('side')} {ft.get('qty')} @ {ft.get('price')} {ft.get('ccy')} on {ft.get('date')}")

    # Defensive check: catch the historical "manual aggregated entry duplicates
    # flex per-fill rows" pattern (e.g. legacy xom_2026_s3 qty=310 vs four flex_*
    # rows summing to 310). The composite dedupe above can't catch this because
    # the qty differs.
    #
    # To avoid false positives (e.g. a manual entry that matches flex qty by
    # coincidence but is actually a different fill at a different price — this
    # bit us with uso_2026_s2 which had qty=200 at $86.95 vs a same-day flex
    # bucket of 250 at $87.21), require BOTH:
    #   (1) manual qty ~= sum of flex qtys in the (ticker, date, side) bucket
    #   (2) manual price ~= weighted-avg flex price in that bucket
    # Tolerance is 0.5% on each, generous for rounding but tight enough to flag
    # only genuine aggregated duplicates.
    from collections import defaultdict
    bucket_flex_qty = defaultdict(float)
    bucket_flex_notional = defaultdict(float)  # sum(qty*price) for weighted avg
    bucket_manual = defaultdict(list)
    for t in existing:
        key = (t.get("ticker"), (t.get("date") or "")[:10], t.get("side"))
        if not all(key): continue
        q = float(t.get("qty", 0) or 0)
        p = float(t.get("price", 0) or 0)
        if (t.get("id") or "").startswith("flex_"):
            bucket_flex_qty[key] += q
            bucket_flex_notional[key] += q * p
        else:
            bucket_manual[key].append(t)
    suspect = []
    for key, manuals in bucket_manual.items():
        flex_total = bucket_flex_qty.get(key, 0)
        if flex_total <= 0: continue
        flex_wavg_px = bucket_flex_notional[key] / flex_total
        for m in manuals:
            mqty = float(m.get("qty", 0) or 0)
            mpx  = float(m.get("price", 0) or 0)
            qty_match = mqty > 0 and abs(mqty - flex_total) / max(mqty, flex_total) < 0.005
            px_match  = (mpx > 0 and flex_wavg_px > 0 and
                         abs(mpx - flex_wavg_px) / max(mpx, flex_wavg_px) < 0.005)
            if qty_match and px_match:
                suspect.append((m, flex_total, flex_wavg_px, key))
    if suspect:
        print()
        print(f"  ! WARNING: {len(suspect)} manual journal entries appear to duplicate flex per-fill aggregates")
        print(f"    (qty AND weighted-avg price both match flex bucket within 0.5%):")
        for m, flex_total, flex_wavg_px, key in suspect:
            tk, dt, sd = key
            print(f"     id={m.get('id')} {sd} {tk} {dt} qty={m.get('qty')} px={m.get('price')} "
                  f"~= sum(flex)={flex_total:.2f} @ wavg={flex_wavg_px:.4f}")
        print(f"  Action: review and consider deleting the manual ids above (flex_* rows are authoritative).")

    if appended:
        existing.sort(key=lambda t: ((t.get("date") or ""), t.get("id","")))
        journal["trades"] = existing
        journal["last_updated"] = datetime.now().strftime("%Y-%m-%d")
        with open(JOURNAL_PATH, "w") as f:
            json.dump(journal, f, indent=2)
        print(f"  Wrote {JOURNAL_PATH}")
    else:
        print(f"  No new trades â€” journal unchanged.")


if __name__ == "__main__":
    main()
