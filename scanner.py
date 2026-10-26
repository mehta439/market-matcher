#!/usr/bin/env python3
"""
market-matcher: find the same event listed on Kalshi and Polymarket and flag price gaps.

Pipeline:
  1. Pull every open market from both venues (public APIs, no keys needed).
  2. Match markets across venues: token blocking -> fuzzy text score -> number + date checks.
  3. For each match, price both arbitrage legs (YES on one venue + NO on the other) after fees.
  4. Write the latest report and append a timestamped snapshot to a history file.

Matches are CANDIDATES. Two markets with near-identical titles can still resolve differently,
so always read both venues' resolution rules before treating a gap as real.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from rapidfuzz import fuzz

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
GAMMA_API = "https://gamma-api.polymarket.com"
HEADERS = {"User-Agent": "market-matcher/0.1 (student research project)"}

KALSHI_FEE_RATE = 0.07  # Kalshi taker fee ~ 0.07 * P * (1 - P) per contract (some series differ)

STOPWORDS = set(
    """will the a an of in on by to for be is are at and or vs v who what which when how
    does do did has have get gets than before after end yes market price there any happen
    officially it its this that with from as win wins winner won""".split()
)


@dataclass
class Market:
    venue: str
    id: str
    text: str
    close: str | None
    yes_ask: float | None  # cost to buy YES, in dollars (0-1)
    no_ask: float | None   # cost to buy NO, in dollars (0-1)
    yes_bid: float | None
    volume: float
    url: str


# ---------------------------------------------------------------- HTTP helpers

def get_json(url: str, params: dict | None = None, retries: int = 4):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            print(f"  network error ({e}); retrying...", file=sys.stderr)
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 429:  # rate limited
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"Gave up on {url} after {retries} tries")


def _to_float(v):
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def kalshi_price(m: dict, field: str) -> float | None:
    """Kalshi has shipped prices both as cents (yes_ask: 56) and dollars (yes_ask_dollars: "0.5600")."""
    v = _to_float(m.get(f"{field}_dollars"))
    if v is None:
        cents = _to_float(m.get(field))
        v = cents / 100 if cents is not None else None
    return v


def valid_ask(p):  # an ask of 0 or >= $1 means "no one is selling"
    return p if p is not None and 0 < p < 1 else None


def valid_bid(p):
    return p if p is not None and 0 < p < 1 else None


def json_list(v):
    if isinstance(v, list):
        return v
    try:
        return json.loads(v or "[]")
    except (TypeError, ValueError):
        return []


# ---------------------------------------------------------------- fetchers

def fetch_kalshi(max_pages: int) -> list[Market]:
    out, cursor = [], None
    for page in range(max_pages):
        params = {"status": "open", "with_nested_markets": "true", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = get_json(f"{KALSHI_API}/events", params)
        for ev in data.get("events", []):
            ev_title = ev.get("title") or ""
            series = (ev.get("series_ticker") or "").lower()
            for m in ev.get("markets") or []:
                ticker = m.get("ticker") or ""
                if ticker.startswith("KXMVE"):  # multi-leg parlay combos: no Polymarket equivalent
                    continue
                if m.get("status") not in (None, "active", "open"):
                    continue
                sub = m.get("yes_sub_title") or m.get("subtitle") or ""
                text = f"{ev_title} {sub}".strip() if sub else (m.get("title") or ev_title)
                out.append(Market(
                    venue="kalshi",
                    id=ticker,
                    text=text,
                    close=m.get("close_time") or m.get("expiration_time"),
                    yes_ask=valid_ask(kalshi_price(m, "yes_ask")),
                    no_ask=valid_ask(kalshi_price(m, "no_ask")),
                    yes_bid=valid_bid(kalshi_price(m, "yes_bid")),
                    volume=_to_float(m.get("volume")) or 0.0,
                    url=f"https://kalshi.com/markets/{series or ticker.lower()}",
                ))
        cursor = data.get("cursor")
        print(f"  kalshi page {page + 1}: {len(out)} markets so far", end="\r", file=sys.stderr)
        if not cursor:
            break
        time.sleep(0.15)
    print(file=sys.stderr)
    return out


def fetch_polymarket(max_pages: int, page_size: int = 100) -> list[Market]:
    out = []
    # Gamma caps how deep offset paging can go (~2,000 events), so pull the highest-volume events
    # first: those are the liquid markets where a price gap could actually be traded.
    order = {"order": "volume", "ascending": "false"}
    for page in range(max_pages):
        params = {"active": "true", "closed": "false", "archived": "false",
                  "limit": page_size, "offset": page * page_size, **order}
        try:
            data = get_json(f"{GAMMA_API}/events", params)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if page == 0 and order and status and 400 <= status < 500:
                print("  sorting by volume not accepted; falling back to default order", file=sys.stderr)
                order = {}
                data = get_json(f"{GAMMA_API}/events", {k: v for k, v in params.items()
                                                       if k not in ("order", "ascending")})
            elif page > 0 and status and 400 <= status < 500:
                print(f"\n  polymarket stopped paging at offset {page * page_size} (API limit)", file=sys.stderr)
                break
            else:
                raise
        if isinstance(data, dict):
            data = data.get("data") or data.get("events") or []
        if not data:
            break
        for ev in data:
            slug = ev.get("slug") or ""
            for m in ev.get("markets") or []:
                if m.get("closed") or m.get("active") is False:
                    continue
                outcomes = [str(o).lower() for o in json_list(m.get("outcomes"))]
                if outcomes != ["yes", "no"]:  # v1 only handles binary Yes/No markets
                    continue
                yes_ask = valid_ask(_to_float(m.get("bestAsk")))
                yes_bid = valid_bid(_to_float(m.get("bestBid")))
                # Buying NO = selling YES, so NO costs (1 - best YES bid).
                no_ask = valid_ask(round(1 - yes_bid, 4)) if yes_bid is not None else None
                out.append(Market(
                    venue="polymarket",
                    id=str(m.get("id")),
                    text=m.get("question") or ev.get("title") or "",
                    close=m.get("endDate") or ev.get("endDate"),
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    yes_bid=yes_bid,
                    volume=_to_float(m.get("volumeNum")) or _to_float(m.get("volume")) or 0.0,
                    url=f"https://polymarket.com/event/{slug}",
                ))
        print(f"  polymarket page {page + 1}: {len(out)} markets so far", end="\r", file=sys.stderr)
        time.sleep(0.15)
    print(file=sys.stderr)
    return out


# ---------------------------------------------------------------- matching

NUM_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")
WORD_RE = re.compile(r"\d+(?:\.\d+)?|[a-z]+")


def numbers(text: str) -> frozenset[str]:
    """'Above 4.50%' and 'above 4.5%' -> {'4.5'}; '$100,000' -> {'100000'}."""
    nums = set()
    for raw in NUM_RE.findall(text):
        try:
            nums.add(f"{float(raw.replace(',', '')):g}")
        except ValueError:
            pass
    return frozenset(nums)


# Map different spellings of the same idea onto one word, so both the fuzzy score and the
# qualifier check treat "nomination"/"nominee" or "above"/"at least" as the same thing.
SYNONYMS = {
    "nomination": "nominee", "nominated": "nominee", "democrat": "democratic", "democrats": "democratic",
    "dem": "democratic", "gop": "republican", "republicans": "republican", "vice": "vp", "mate": "vp",
    "tickets": "ticket", "primaries": "primary", "global": "world", "worldwide": "world",
    # direction of a threshold
    "above": "gt", "over": "gt", "more": "gt", "greater": "gt", "exceed": "gt", "exceeds": "gt",
    "higher": "gt", "least": "gt", "below": "lt", "under": "lt", "less": "lt", "fewer": "lt",
    "lower": "lt", "most": "lt",
    # negation
    "no": "neg", "not": "neg", "none": "neg", "never": "neg",
    # tournament stage
    "semifinals": "semifinal", "semifinalist": "semifinal", "semifinalists": "semifinal",
    "finals": "final", "finalist": "final", "finalists": "final", "quarterfinals": "quarterfinal",
    "quarterfinalist": "quarterfinal", "quarterfinalists": "quarterfinal", "playoffs": "playoff",
    "tie": "draw",
    # legal / political outcomes
    "removal": "removed", "remove": "removed", "resigns": "resign", "resignation": "resign",
    "resigned": "resign", "conviction": "convicted", "convict": "convicted", "indictment": "indicted",
    "indict": "indicted", "impeachment": "impeached", "impeach": "impeached",
    # months
    "jan": "january", "feb": "february", "mar": "march", "apr": "april", "jun": "june", "jul": "july",
    "aug": "august", "sep": "september", "sept": "september", "oct": "october", "nov": "november",
    "dec": "december",
}

# Words that change WHAT is being predicted. Both titles must contain exactly the same set:
# "VP nominee" != "nominee", "above 0.5%" != "less than 0.5%", "US GDP" != "UK GDP", "Oct 30" != "Nov 30".
QUALIFIERS = {
    "nominee", "ticket", "vp", "primary", "democratic", "republican", "senate", "house", "governor",
    "mayor", "speaker", "popular", "electoral", "margin", "turnout", "nominal", "core",
    "gt", "lt", "between", "neg",
    "semifinal", "final", "quarterfinal", "playoff", "draw",
    "impeached", "removed", "resign", "convicted", "indicted", "arrested", "pardoned",
    "us", "uk", "world", "eu", "europe", "china", "japan", "canada", "germany", "france", "india",
    "korea", "mexico", "brazil", "russia", "ukraine", "israel", "iran", "taiwan",
    "january", "february", "march", "april", "june", "july", "august", "september", "october",
    "november", "december",
}

COUNTRY_ABBREV = [(re.compile(r"\bu\.\s?s\.?(a\.?)?(?![a-z])"), " us "), (re.compile(r"\bu\.\s?k\.?(?![a-z])"), " uk ")]


def words(text: str) -> list[str]:
    """Lowercase, unify numbers ('4.50%' -> '4.5', '$100,000' -> '100000'), U.S. -> us, apply synonyms."""
    # strip accents so "Çaykur" -> "caykur" and "Más" -> "mas"
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    for pattern, repl in COUNTRY_ABBREV:
        text = pattern.sub(repl, text)
    text = NUM_RE.sub(lambda m: f" {float(m.group().replace(',', '')):g} ", text)
    return [SYNONYMS.get(w, w) for w in WORD_RE.findall(text)]


def normalize(text: str) -> str:
    return " ".join(w for w in words(text) if w not in STOPWORDS and (len(w) > 1 or w[0].isdigit()))


def tokens(text: str) -> set[str]:
    return set(normalize(text).split())


def qualifiers(text: str) -> frozenset[str]:
    return frozenset(w for w in words(text) if w in QUALIFIERS)


def parse_date(s: str | None):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def text_score(a: str, b: str) -> float:
    # Score normalized text. token_set alone over-rewards "A is a subset of B";
    # averaging with token_sort tempers that.
    a, b = normalize(a), normalize(b)
    return 0.5 * fuzz.token_set_ratio(a, b) + 0.5 * fuzz.token_sort_ratio(a, b)


def match_markets(kalshi: list[Market], poly: list[Market], min_score: float, max_days: int):
    poly_tokens = [tokens(p.text) for p in poly]
    poly_quals = [qualifiers(p.text) for p in poly]
    df = Counter(t for toks in poly_tokens for t in toks)
    too_common = max(50, int(0.02 * len(poly)))  # ignore tokens in >2% of markets when blocking

    index = defaultdict(list)
    for i, toks in enumerate(poly_tokens):
        for t in toks:
            if df[t] <= too_common:
                index[t].append(i)

    candidates = []  # (score, kalshi_idx, poly_idx, days_apart)
    for ki, k in enumerate(kalshi):
        k_toks = tokens(k.text)
        shared = Counter()
        for t in k_toks:
            for i in index.get(t, ()):
                shared[i] += 1
        need = 2 if len(k_toks) > 2 else 1
        k_nums, k_quals, k_date = numbers(k.text), qualifiers(k.text), parse_date(k.close)

        for i, n_shared in shared.items():
            if n_shared < need:
                continue
            p = poly[i]
            if numbers(p.text) != k_nums:  # "above 4.5%" must not match "above 5%"
                continue
            if poly_quals[i] != k_quals:   # "VP nominee" must not match "presidential nominee"
                continue
            # Every content word in the Polymarket question must appear on the Kalshi side:
            # "Olise ATTEND the ceremony" or "end in a DRAW" is a different bet.
            if poly_tokens[i] - k_toks:
                continue
            p_date = parse_date(p.close)
            days_apart = abs((k_date - p_date).days) if (k_date and p_date) else None
            if days_apart is not None and days_apart > max_days:
                continue
            s = text_score(k.text, p.text)
            if s >= min_score:
                candidates.append((s, ki, i, days_apart))

    # One-to-one: each market pairs with at most one market on the other venue, best scores first.
    candidates.sort(key=lambda c: -c[0])
    used_k, used_p, matches = set(), set(), []
    for s, ki, pi, days in candidates:
        if ki in used_k or pi in used_p:
            continue
        used_k.add(ki)
        used_p.add(pi)
        matches.append((kalshi[ki], poly[pi], s, days))
    return matches


# ---------------------------------------------------------------- pricing

def kalshi_fee(p: float) -> float:
    return KALSHI_FEE_RATE * p * (1 - p)


def price_pair(k: Market, p: Market, poly_fee_rate: float) -> dict:
    """Two ways to lock in $1 at resolution: YES here + NO there, or NO here + YES there."""
    legs = {}
    if k.yes_ask and p.no_ask:
        cost = k.yes_ask + p.no_ask + kalshi_fee(k.yes_ask) + poly_fee_rate * p.no_ask * (1 - p.no_ask)
        legs["Kalshi YES + Poly NO"] = cost
    if k.no_ask and p.yes_ask:
        cost = k.no_ask + p.yes_ask + kalshi_fee(k.no_ask) + poly_fee_rate * p.yes_ask * (1 - p.yes_ask)
        legs["Kalshi NO + Poly YES"] = cost
    if not legs:
        return {"strategy": "", "cost_after_fees": None, "net_edge": None}
    strat, cost = min(legs.items(), key=lambda kv: kv[1])
    return {"strategy": strat, "cost_after_fees": round(cost, 4), "net_edge": round(1 - cost, 4)}


def days_to_close(k: Market, p: Market, now: datetime):
    dates = [d for d in (parse_date(k.close), parse_date(p.close)) if d]
    return max(1, (max(dates) - now).days) if dates else None


def mid(m: Market):
    if m.yes_ask is not None and m.yes_bid is not None:
        return (m.yes_ask + m.yes_bid) / 2
    return m.yes_ask or m.yes_bid


# ---------------------------------------------------------------- output

FIELDS = ["snapshot_utc", "match_score", "days_apart", "kalshi_ticker", "kalshi_text", "poly_id", "poly_text",
          "kalshi_yes_ask", "kalshi_no_ask", "poly_yes_ask", "poly_no_ask", "yes_mid_gap",
          "strategy", "cost_after_fees", "net_edge", "days_to_close", "annualized_return", "kalshi_volume", "poly_volume",
          "kalshi_url", "poly_url", "flag", "same_event_label"]


def build_rows(matches, poly_fee_rate, snapshot):
    rows = []
    now = datetime.now(timezone.utc)
    for k, p, score, days in matches:
        km, pm = mid(k), mid(p)
        priced = price_pair(k, p, poly_fee_rate)
        dtc = days_to_close(k, p, now)
        ann = None
        if priced["net_edge"] is not None and dtc:
            ann = round(priced["net_edge"] / priced["cost_after_fees"] * 365 / dtc, 4)
        rows.append({
            "snapshot_utc": snapshot,
            "match_score": round(score, 1),
            "days_apart": days,
            "kalshi_ticker": k.id, "kalshi_text": k.text,
            "poly_id": p.id, "poly_text": p.text,
            "kalshi_yes_ask": k.yes_ask, "kalshi_no_ask": k.no_ask,
            "poly_yes_ask": p.yes_ask, "poly_no_ask": p.no_ask,
            "yes_mid_gap": round(abs(km - pm), 4) if km is not None and pm is not None else None,
            **priced,
            "days_to_close": dtc,
            "annualized_return": ann,
            "kalshi_volume": k.volume, "poly_volume": p.volume,
            "kalshi_url": k.url, "poly_url": p.url,
            "flag": "edge > 10%: probably a mismatch, verify" if (priced["net_edge"] or 0) > 0.10 else "",
            "same_event_label": "",  # fill in 1/0 by hand -> training data for an ML matcher
        })
    rows.sort(key=lambda r: (r["net_edge"] is None, -(r["net_edge"] or 0)))
    return rows


def write_csv(path, rows, append=False):
    exists = os.path.exists(path)
    with open(path, "a" if append else "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if not (append and exists):
            w.writeheader()
        w.writerows(rows)


def run_once(args):
    snapshot = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{snapshot}] fetching Kalshi...", file=sys.stderr)
    kalshi = fetch_kalshi(args.kalshi_pages)
    print(f"[{snapshot}] fetching Polymarket...", file=sys.stderr)
    poly = fetch_polymarket(args.poly_pages)

    t0 = time.time()
    matches = match_markets(kalshi, poly, args.min_score, args.max_days)
    rows = build_rows(matches, args.poly_fee, snapshot)

    os.makedirs(args.out_dir, exist_ok=True)
    write_csv(os.path.join(args.out_dir, "latest.csv"), rows)
    write_csv(os.path.join(args.out_dir, "history.csv"), rows, append=True)

    arbs = [r for r in rows if (r["net_edge"] or 0) > args.min_edge]
    print(f"\nKalshi markets: {len(kalshi):,} | Polymarket markets: {len(poly):,} | "
          f"matched pairs: {len(rows):,} ({time.time() - t0:.1f}s) | "
          f"edge > {args.min_edge:.0%} after fees: {len(arbs)}")
    print(f"Saved {args.out_dir}/latest.csv and appended to {args.out_dir}/history.csv\n")
    print("  edge   /yr    days  score  kalshi  <->  polymarket   (! = edge >10%, likely mismatch)")
    for r in rows[: args.show]:
        edge = f"{r['net_edge']:+.3f}" if r["net_edge"] is not None else "  n/a "
        ann = f"{r['annualized_return']:+.0%}" if r["annualized_return"] is not None else "  n/a"
        days = r["days_to_close"] if r["days_to_close"] is not None else "?"
        mark = "!" if r["flag"] else " "
        print(f"{mark}{edge} {ann:>6} {days:>6}  [{r['match_score']:>5}]  "
              f"{r['kalshi_text'][:48]:<48} <-> {r['poly_text'][:48]}")
    if arbs:
        print("\nPositive edges are candidates only: check both resolution rules and order-book depth first.")


def main():
    ap = argparse.ArgumentParser(description="Match Kalshi and Polymarket markets and flag price gaps.")
    ap.add_argument("--kalshi-pages", type=int, default=100, help="pages of 200 events (default 100)")
    ap.add_argument("--poly-pages", type=int, default=100, help="pages of 100 events (default 100)")
    ap.add_argument("--min-score", type=float, default=82, help="fuzzy match threshold 0-100 (default 82)")
    ap.add_argument("--max-days", type=int, default=45, help="max days between close dates (default 45)")
    ap.add_argument("--poly-fee", type=float, default=0.0,
                    help="Polymarket taker fee rate applied as rate*P*(1-P); most markets are 0")
    ap.add_argument("--min-edge", type=float, default=0.0, help="edge to count as an arb (default 0)")
    ap.add_argument("--show", type=int, default=20, help="rows to print (default 20)")
    ap.add_argument("--out-dir", default="data")
    ap.add_argument("--loop", type=float, default=0, help="re-run every N minutes (0 = run once)")
    args = ap.parse_args()

    while True:
        try:
            run_once(args)
        except Exception as e:  # keep the loop alive through a bad API response
            if not args.loop:
                raise
            print(f"run failed: {e}", file=sys.stderr)
        if not args.loop:
            break
        time.sleep(args.loop * 60)


if __name__ == "__main__":
    main()
