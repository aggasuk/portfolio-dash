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
    # Numeric (Japanese listings) — assume JT
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
                # Old/unexpected — fall through to assume body is data
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
    # Dedupe sets:
    #   (a) by ibkr_trade_id (canonical)
    #   (b) by composite (ticker, date, side, qty, price) — catches manual entries
    existing_trade_ids = {t.get("ibkr_trade_id") for t in existing if t.get("ibkr_trade_id")}
    existing_composites = {
        (t.get("ticker"), (t.get("date") or "")[:10], t.get("side"),
         round(float(t.get("qty", 0) or 0), 4), round(float(t.get("price", 0) or 0), 6))
        for t in existing
    }

    appended = 0
    skipped_id, skipped_comp = 0, 0
    for ft in flex_trades:
        if ft["ibkr_trade_id"] in existing_trade_ids:
            skipped_id += 1
            continue
        comp = (ft["ticker"], ft["date"], ft["side"],
                round(ft["qty"], 4), round(ft["price"], 6))
        if comp in existing_composites:
            skipped_comp += 1
            continue
        existing.append(ft)
        appended += 1

    print(f"  Appended: {appended}")
    print(f"  Skipped (matched IBKR trade_id): {skipped_id}")
    print(f"  Skipped (matched ticker/date/qty/price): {skipped_comp}")

    if appended:
        existing.sort(key=lambda t: ((t.get("date") or ""), t.get("id","")))
        journal["trades"] = existing
        journal["last_updated"] = datetime.now().strftime("%Y-%m-%d")
        with open(JOURNAL_PATH, "w") as f:
            json.dump(journal, f, indent=2)
        print(f"  Wrote {JOURNAL_PATH}")
    else:
        print(f"  No new trades — journal unchanged.")


if __name__ == "__main__":
    main()
