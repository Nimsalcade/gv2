#!/usr/bin/env python3
"""Analyze gabagool22's trade history to reverse-engineer their strategy."""
import json
from collections import defaultdict
from datetime import datetime

with open("/Users/bradamanka/Downloads/gabagool-main/config/history.txt") as f:
    trades = json.load(f)

print(f"Total trades: {len(trades)}")
print(f"Date range: {datetime.fromtimestamp(trades[-1]['timestamp'])} → {datetime.fromtimestamp(trades[0]['timestamp'])}")
print()

# ─── By asset ───
assets = defaultdict(int)
for t in trades:
    if "Bitcoin" in t["title"] or "BTC" in t["title"]:
        assets["BTC"] += 1
    elif "Ethereum" in t["title"] or "ETH" in t["title"]:
        assets["ETH"] += 1
    elif "Solana" in t["title"] or "SOL" in t["title"]:
        assets["SOL"] += 1
    else:
        assets["OTHER"] += 1
print("=== ASSET DISTRIBUTION ===")
for a, c in sorted(assets.items(), key=lambda x: -x[1]):
    print(f"  {a}: {c} trades ({c/len(trades)*100:.1f}%)")

# ─── By market type ───
market_types = defaultdict(int)
for t in trades:
    slug = t.get("slug", "")
    if "5m" in slug:
        market_types["5min"] += 1
    elif "15m" in slug:
        market_types["15min"] += 1
    else:
        market_types["1hour"] += 1
print("\n=== MARKET DURATION ===")
for mt, c in sorted(market_types.items(), key=lambda x: -x[1]):
    print(f"  {mt}: {c} trades ({c/len(trades)*100:.1f}%)")

# ─── Side distribution ───
sides = defaultdict(int)
for t in trades:
    sides[t["outcome"]] += 1
print("\n=== SIDE DISTRIBUTION ===")
for s, c in sorted(sides.items(), key=lambda x: -x[1]):
    print(f"  {s}: {c} trades ({c/len(trades)*100:.1f}%)")

# ─── Price distribution ───
prices = [t["price"] for t in trades]
print(f"\n=== PRICE STATS ===")
print(f"  Mean: ${sum(prices)/len(prices):.3f}")
print(f"  Min:  ${min(prices):.3f}")
print(f"  Max:  ${max(prices):.3f}")

# Price buckets
buckets = defaultdict(int)
for p in prices:
    if p < 0.20: buckets["$0.01-$0.19"] += 1
    elif p < 0.40: buckets["$0.20-$0.39"] += 1
    elif p < 0.60: buckets["$0.40-$0.59"] += 1
    elif p < 0.80: buckets["$0.60-$0.79"] += 1
    else: buckets["$0.80-$0.99"] += 1
print("  Price buckets:")
for b in sorted(buckets.keys()):
    print(f"    {b}: {buckets[b]} ({buckets[b]/len(prices)*100:.1f}%)")

# ─── Sizing ───
sizes = [t["size"] for t in trades]
print(f"\n=== SIZE STATS ===")
print(f"  Mean: {sum(sizes)/len(sizes):.2f} shares")
print(f"  Min:  {min(sizes):.2f}")
print(f"  Max:  {max(sizes):.2f}")
print(f"  Total volume: ${sum(t['price']*t['size'] for t in trades):,.2f}")

# ─── Per-window analysis (group by conditionId) ───
windows = defaultdict(list)
for t in trades:
    key = t["conditionId"]
    windows[key].append(t)

print(f"\n=== WINDOW ANALYSIS ===")
print(f"  Total unique windows traded: {len(windows)}")

# Check hedging pattern
hedged = 0
one_sided = 0
for cid, wtrades in windows.items():
    outcomes = set(t["outcome"] for t in wtrades)
    if len(outcomes) == 2:
        hedged += 1
    else:
        one_sided += 1

print(f"  Hedged (both UP+DOWN): {hedged} ({hedged/len(windows)*100:.1f}%)")
print(f"  One-sided: {one_sided} ({one_sided/len(windows)*100:.1f}%)")

# ─── Snipe pattern detection ───
# Look for rapid successive trades (within 2 seconds) at high prices
print(f"\n=== SNIPE PATTERN DETECTION ===")
sorted_trades = sorted(trades, key=lambda t: t["timestamp"])
snipe_clusters = []
i = 0
while i < len(sorted_trades):
    t = sorted_trades[i]
    if t["price"] >= 0.70:  # high-price aggressive buy
        cluster = [t]
        j = i + 1
        while j < len(sorted_trades) and sorted_trades[j]["timestamp"] - t["timestamp"] <= 4:
            if sorted_trades[j]["price"] >= 0.60:
                cluster.append(sorted_trades[j])
            j += 1
        if len(cluster) >= 3:
            snipe_clusters.append(cluster)
        i = j
    else:
        i += 1

print(f"  Snipe bursts detected (3+ high-price trades within 4s): {len(snipe_clusters)}")
if snipe_clusters:
    for sc in snipe_clusters[:5]:
        ts = datetime.fromtimestamp(sc[0]["timestamp"])
        outcomes = [t["outcome"] for t in sc]
        prices_s = [f"${t['price']:.2f}" for t in sc]
        markets = set(t["title"].split(" - ")[0] for t in sc)
        print(f"    {ts} | {len(sc)} trades | markets={markets} | outcomes={outcomes[:4]} | prices={prices_s[:4]}")

# ─── Capital deployed per window ───
print(f"\n=== CAPITAL PER WINDOW ===")
window_caps = []
for cid, wtrades in windows.items():
    total = sum(t["price"] * t["size"] for t in wtrades)
    title = wtrades[0]["title"]
    window_caps.append((total, title, len(wtrades)))

window_caps.sort(key=lambda x: -x[0])
print(f"  Average capital per window: ${sum(c[0] for c in window_caps)/len(window_caps):.2f}")
print(f"  Median capital per window: ${sorted(c[0] for c in window_caps)[len(window_caps)//2]:.2f}")
print(f"  Max capital in single window: ${window_caps[0][0]:.2f} ({window_caps[0][1]})")
print(f"  Average trades per window: {sum(c[2] for c in window_caps)/len(window_caps):.1f}")

# ─── Timing analysis ───
print(f"\n=== TRADE TIMING (hour of day UTC) ===")
hours = defaultdict(int)
for t in trades:
    h = datetime.fromtimestamp(t["timestamp"]).hour
    hours[h] += 1
for h in sorted(hours.keys()):
    bar = "█" * (hours[h] // 10)
    print(f"  {h:02d}:00 | {hours[h]:4d} | {bar}")

# ─── SELL trades ───
sells = [t for t in trades if t.get("side") == "SELL"]
print(f"\n=== SELL TRADES ===")
print(f"  Total sells: {len(sells)}")
if sells:
    for s in sells[:10]:
        print(f"    {s['outcome']} @ ${s['price']:.3f} × {s['size']:.2f} | {s['title'][:50]}")
