#!/usr/bin/env python3
"""
Simulation de grille XLM avec gap 0.6% (GRID_PCT = 0.006)
Rejoue la logique exacte de bot_trader.py sur données réelles Binance.

Usage:
    python3 test_gap_xlm.py              # 72h de données 1h
    python3 test_gap_xlm.py --hours 168  # 7 jours
    python3 test_gap_xlm.py --gap 0.01   # comparer avec gap 1%
"""
import requests, argparse, sys
from datetime import datetime, timezone

SYMBOL       = "XLMUSDT"
CAPITAL      = 500.0       # $500 capital (2 ordres × $250)
PER_ORDER    = 250.0       # $250 par ordre
FEE_SELL     = 0.00075     # 0.075% frais SELL Binance SPOT
FEE_BUY      = 0.0         # BUY gratuit en SPOT
BASE         = "https://api.binance.com"


def fetch_klines(symbol, interval, limit):
    try:
        r = requests.get(f"{BASE}/api/v3/klines",
                         params={"symbol": symbol, "interval": interval, "limit": limit},
                         timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def generate_synthetic_klines(n_hours, start_price=0.32, seed=42):
    """
    Génère n_hours bougies 1h synthétiques avec paramètres réalistes XLM :
      - σ horaire = 0.9% (vol XLM mesurée sur 2024-2026)
      - Léger mean-reversion (µ dynamique)
      - 3% des bougies = événement haute volatilité (×2.5 σ)
    Format retourné : [[ts_ms, open, high, low, close, ...], ...]
    """
    import random, math
    random.seed(seed)
    klines = []
    price  = start_price
    ts     = int(datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    hour_ms = 3_600_000
    mu     = 0.0
    sigma_base = 0.009   # 0.9% σ horaire XLM

    for i in range(n_hours):
        # Légère mean-reversion vers start_price
        drift = 0.0003 * (start_price - price) / start_price
        # Événement volatile aléatoire (3% des bougies)
        sigma = sigma_base * (2.5 if random.random() < 0.03 else 1.0)
        pct = random.gauss(drift, sigma)
        new_price = price * (1 + pct)
        # Contraindre dans ±40% du prix initial
        new_price = max(start_price * 0.60, min(start_price * 1.40, new_price))

        open_  = price
        close_ = new_price
        wick_h = abs(random.gauss(0, sigma)) * price
        wick_l = abs(random.gauss(0, sigma)) * price
        high_  = max(open_, close_) + wick_h
        low_   = min(open_, close_) - wick_l

        klines.append([ts, f"{open_:.6f}", f"{high_:.6f}", f"{low_:.6f}", f"{close_:.6f}", "0"])
        price = new_price
        ts   += hour_ms

    return klines


def simulate(klines, grid_pct, per_order, fee_sell):
    """
    Simule la grille sur des bougies OHLCV.
    Logique simplifiée mais fidèle à bot_trader :
      - Déploie BUY @ close * (1 - grid_pct/2)
      - Déploie SELL @ close * (1 + grid_pct/2)
      - Teste si la bougie suivante touche BUY → fill
      - Après fill BUY : teste si bougie suivante touche SELL → fill
      - Compte les cycles, accumule le profit
    """
    half = grid_pct / 2
    cycles          = []
    hourly_cycles   = {}   # heure ISO → nb cycles
    state           = 'idle'
    buy_price       = 0.0
    sell_price      = 0.0
    entry_price     = 0.0
    total_profit    = 0.0

    for i, k in enumerate(klines):
        ts      = int(k[0]) // 1000
        dt      = datetime.fromtimestamp(ts, tz=timezone.utc)
        hour_key = dt.strftime("%Y-%m-%d %Hh")
        o, h, lo, c = float(k[1]), float(k[2]), float(k[3]), float(k[4])

        if state == 'idle':
            # Déployer la grille sur le close de cette bougie
            buy_price  = c * (1 - half)
            sell_price = c * (1 + half)
            entry_price = c
            state = 'waiting_buy'

        elif state == 'waiting_buy':
            # BUY fill si le bas de la bougie descend sous buy_price
            if lo <= buy_price:
                qty   = per_order / buy_price
                state = 'waiting_sell'
                # SELL déjà en place — pas besoin de re-déployer

        elif state == 'waiting_sell':
            # SELL fill si le haut monte au-dessus de sell_price
            if h >= sell_price:
                qty        = per_order / buy_price
                gross      = qty * sell_price
                fee        = gross * fee_sell
                profit     = gross - per_order - fee
                total_profit += profit
                cycles.append({
                    'hour':    hour_key,
                    'buy':     buy_price,
                    'sell':    sell_price,
                    'profit':  round(profit, 4),
                    'gap_pct': round((sell_price / buy_price - 1) * 100, 3),
                })
                hourly_cycles[hour_key] = hourly_cycles.get(hour_key, 0) + 1
                # Redéployer depuis le nouveau prix (close de cette bougie)
                buy_price  = c * (1 - half)
                sell_price = c * (1 + half)
                state = 'waiting_buy'

    return cycles, hourly_cycles, total_profit


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hours', type=int,   default=72,    help='Heures de données (max 1000)')
    parser.add_argument('--gap',   type=float, default=0.006, help='GRID_PCT ex: 0.006=0.6%%')
    parser.add_argument('--compare', action='store_true',     help='Comparer 0.6%% vs 1%%')
    args = parser.parse_args()

    limit = min(args.hours, 1000)
    print(f"\n📡 Tentative Binance API ({SYMBOL}, 1h, {limit} bougies)…")
    klines = fetch_klines(SYMBOL, '1h', limit)
    if klines and len(klines) >= 10:
        source = "Binance live"
        first_dt = datetime.fromtimestamp(int(klines[0][0])  // 1000, tz=timezone.utc)
        last_dt  = datetime.fromtimestamp(int(klines[-1][0]) // 1000, tz=timezone.utc)
        print(f"  ✅ {len(klines)} bougies réelles : "
              f"{first_dt.strftime('%Y-%m-%d %H:%M')} → {last_dt.strftime('%Y-%m-%d %H:%M')} UTC")
    else:
        source = "synthétique (σ=0.9%/h, réaliste XLM)"
        print(f"  ⚠️  API inaccessible — génération de {limit}h de données synthétiques")
        print(f"      Paramètres : σ_horaire=0.9%, mean-reversion, spikes aléatoires 3%")
        klines = generate_synthetic_klines(limit)
        print(f"  ✅ {len(klines)} bougies synthétiques générées")

    gaps = [args.gap]
    if args.compare:
        gaps = [0.003, 0.006, 0.01, 0.02]

    print(f"\n  Source : {source}")
    for gap in gaps:
        cycles, hourly, total_profit = simulate(klines, gap, PER_ORDER, FEE_SELL)

        n_hours   = len(klines)
        n_cycles  = len(cycles)
        avg_cycle_h = n_cycles / n_hours if n_hours > 0 else 0
        max_cycles_h = max(hourly.values()) if hourly else 0
        hours_with_multi = sum(1 for v in hourly.values() if v >= 2)

        buy_prices = [float(k[3]) for k in klines]
        min_p, max_p = min(buy_prices), max(buy_prices)
        volatility_pct = (max_p - min_p) / min_p * 100

        print(f"\n{'='*58}")
        print(f"  GAP : {gap*100:.2f}%  (±{gap/2*100:.2f}% par côté)")
        print(f"{'='*58}")
        print(f"  Période analysée  : {n_hours}h  ({n_hours/24:.1f} jours)")
        print(f"  Plage prix XLM    : ${min_p:.4f} → ${max_p:.4f}  (±{volatility_pct:.1f}%)")
        print(f"  Cycles complétés  : {n_cycles}")
        print(f"  Moyenne /heure    : {avg_cycle_h:.2f}")
        print(f"  Max /heure        : {max_cycles_h}")
        print(f"  Heures avec ≥2    : {hours_with_multi}")
        print(f"  Profit net total  : ${total_profit:.2f}  (SELL fees inclus)")
        if n_cycles > 0:
            print(f"  Profit moyen/cycle: ${total_profit/n_cycles:.3f}")
        if n_hours > 0:
            print(f"  Profit moyen/heure: ${total_profit/n_hours:.3f}")

        # Top 5 heures les plus actives
        if hourly:
            top5 = sorted(hourly.items(), key=lambda x: -x[1])[:5]
            print(f"\n  Top 5 heures actives :")
            for h, cnt in top5:
                print(f"    {h}  →  {cnt} cycle(s)")

        # Distribution des cycles par heure
        from collections import Counter
        dist = Counter(hourly.values())
        print(f"\n  Distribution cycles/heure :")
        for k in sorted(dist):
            bar = '█' * dist[k]
            print(f"    {k} cycle(s) : {dist[k]:3d} heures  {bar}")

        # Derniers 5 cycles
        if cycles:
            print(f"\n  Derniers 5 cycles :")
            for cyc in cycles[-5:]:
                print(f"    {cyc['hour']}  BUY ${cyc['buy']:.5f} → SELL ${cyc['sell']:.5f}"
                      f"  gap {cyc['gap_pct']:.3f}%  +${cyc['profit']:.4f}")


if __name__ == '__main__':
    main()
