# market-matcher

Finds the same real-world event listed on **Kalshi** and **Polymarket**, then flags price gaps between the two venues after fees.

Prediction markets list thousands of overlapping events with different wording ("Fed rate decision in December 2026: Above 4.50%" vs. "Will the Fed's upper bound be above 4.5% after the December 2026 meeting?"). Matching them is the hard part. Once they're matched, any pair where **YES on one venue + NO on the other costs less than $1 after fees** is a candidate arbitrage.

## How it works

1. **Fetch** every open market from Kalshi's public API (`/events` with nested markets) and Polymarket's Gamma API. No API keys needed.
2. **Block** candidate pairs with an inverted token index, so the scanner never compares every market to every other market.
3. **Match** with fuzzy text similarity on normalized titles, plus two hard rules:
   - The numbers in both titles must agree (`above 4.5%` never matches `above 5%`).
   - "Qualifier" words must agree, so a market's role and party can't silently differ (`VP nominee` ≠ `presidential nominee`, `presidential ticket` ≠ `nominee`, `Democratic` ≠ `Republican`).
   - Close dates must be within `--max-days` of each other.
   - Matching is one-to-one: each market pairs with at most one market on the other venue.
4. **Price** both arbitrage directions using best asks, Kalshi's taker fee (≈ 0.07 · P · (1−P)), and an optional Polymarket fee.
5. **Log**: `data/latest.csv` holds the current snapshot, and `data/history.csv` gets every run appended with a timestamp.

## Run it

```bash
python3 -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python scanner.py                                    # one full scan
python scanner.py --kalshi-pages 5 --poly-pages 5    # quick test run
python scanner.py --loop 15                          # re-scan every 15 minutes
```

Useful flags: `--min-score` (match threshold, default 82), `--max-days` (default 45), `--poly-fee`, `--min-edge`, `--show`.

## Output columns

| Column | Meaning |
|---|---|
| `match_score` | Fuzzy similarity of normalized titles (0–100) |
| `yes_mid_gap` | Difference in YES mid-price between venues |
| `strategy` | The cheaper way to lock in $1: `Kalshi YES + Poly NO` or `Kalshi NO + Poly YES` |
| `cost_after_fees` / `net_edge` | Cost of both legs, and `1 − cost` (positive = candidate arb) |
| `days_to_close` / `annualized_return` | Capital is locked until resolution, so a 2% edge on a 2028 market is far worse than 2% next week |
| `same_event_label` | Left blank on purpose: label pairs 1/0 by hand to build training data |

## Caveats

- **A match is a candidate, not a guarantee.** Two markets with nearly the same title can still resolve differently (different data sources, cutoff times, edge cases). Read both venues' rules before trading.
- Prices are top-of-book only. Order-book depth isn't checked yet, so a gap may only exist for a few contracts.
- v1 handles binary Yes/No markets only. Polymarket's team-vs-team markets are skipped.

## Roadmap

- [ ] Run on a schedule (GitHub Actions cron) to build a price-gap history
- [ ] Label ~500 candidate pairs and train a **learned matcher** (features: fuzzy scores, number/date agreement, sentence-embedding similarity) to replace the fixed threshold, then measure precision/recall against the baseline
- [ ] Pull order-book depth to size real opportunities
- [ ] Model how long gaps persist before closing
