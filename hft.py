#!/usr/bin/env python3
"""
⚡ HFT — WebSocket Price Feed + Order Stream
=============================================
Remplace le polling REST par des WebSocket pour :
- Prix temps réel (<50ms au lieu de ~250ms)
- Détection instantanée des fills (plus de polling /openOrders)
- Spread bid/ask en direct

Utilisation:
    from hft import PriceFeed, OrderStream

    feed = PriceFeed('BTCUSDT')
    feed.start()
    price = feed.price      # Dernier prix (thread-safe)
    spread = feed.spread    # Spread bid/ask
"""

import json
import time
import threading
import requests
import hmac
import hashlib
import urllib.parse

BASE = "https://api.binance.com"
WSS = "wss://stream.binance.com:9443/ws"


# ═══════════════════════════════════════════════════════════════
# 1. PRICE FEED — WebSocket ticker temps réel
# ═══════════════════════════════════════════════════════════════

class PriceFeed(threading.Thread):
    """
    Thread dédié au WebSocket ticker Binance.
    Met à jour le prix en mémoire sans bloquer la boucle principale.

    Usage:
        feed = PriceFeed('BTCUSDT')
        feed.start()
        # Dans la boucle principale:
        price = feed.price  # < 50ms, thread-safe
    """

    def __init__(self, symbol):
        super().__init__(daemon=True)
        self.symbol = symbol.lower()
        self._price = 0.0
        self._price_min = 0.0
        self._price_max = 0.0
        self._bid = 0.0
        self._ask = 0.0
        self._spread = 0.0
        self._last_update = 0
        self._ws = None
        self._connected = False

    def run(self):
        import websocket
        stream = f"{self.symbol}@ticker"
        url = f"{WSS}/{stream}"
        while True:
            try:
                self._ws = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_open=self._on_open,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                print(f"  ⚡ WS reconnect ({e})")
                time.sleep(5)

    def _on_message(self, ws, message):
        data = json.loads(message)
        self._price = float(data.get('c', self._price))
        # Track min/max continu (entre deux resets)
        if self._price_min == 0 or self._price < self._price_min:
            self._price_min = self._price
        if self._price > self._price_max:
            self._price_max = self._price
        self._bid = float(data.get('b', self._bid))
        self._ask = float(data.get('a', self._ask))
        self._spread = ((self._ask - self._bid) / self._bid) if self._bid > 0 else 0.0
        self._last_update = time.time()

    def _on_open(self, ws):
        self._connected = True
        print(f"  ⚡ WS connecté: {self.symbol}")

    def _on_close(self, ws, status, msg):
        self._connected = False
        print(f"  ⚡ WS déconnecté ({status})")

    def _on_error(self, ws, error):
        self._connected = False

    @property
    def price(self):
        return self._price

    @property
    def spread(self):
        return self._spread

    @property
    def connected(self):
        return self._connected

    @property
    def age(self):
        """Âge de la dernière mise à jour en secondes."""
        return time.time() - self._last_update if self._last_update > 0 else 999

    @property
    def price_min(self):
        """Prix le plus bas depuis le dernier reset."""
        return self._price_min

    @property
    def price_max(self):
        """Prix le plus haut depuis le dernier reset."""
        return self._price_max

    def reset_price_range(self):
        """Reset le tracker min/max. À appeler après chaque tick de la boucle principale."""
        self._price_min = self._price
        self._price_max = self._price

    def stop(self):
        if self._ws:
            self._ws.close()


# ═══════════════════════════════════════════════════════════════
# 2. ORDER STREAM — Détection instantanée des fills
# ═══════════════════════════════════════════════════════════════

class OrderStream(threading.Thread):
    """
    Écoute le User Data Stream Binance pour détecter les fills
    instantanément via executionReport.

    Nécessite:
    - Clé API Binance
    - Listen key (rafraîchie auto toutes les 30 min)

    Usage:
        stream = OrderStream(api_key, secret_key)
        stream.start()
        fills = stream.get_fills()  # Liste des fills depuis le dernier check
    """

    def __init__(self, api_key, secret_key):
        super().__init__(daemon=True)
        self.api_key = api_key
        self.secret_key = secret_key
        self._listen_key = None
        self._fills = []
        self._lock = threading.Lock()
        self._ws = None

    def _create_listen_key(self):
        """Crée une listen key via l'API REST Binance."""
        try:
            r = requests.post(
                f"{BASE}/api/v3/userDataStream",
                headers={'X-MBX-APIKEY': self.api_key},
                timeout=5
            )
            if r.status_code == 200:
                return r.json()['listenKey']
        except Exception as e:
            print(f"  ⚡ ListenKey error: {e}")
        return None

    def _keepalive_listen_key(self):
        """Prolonge la validité de la listen key (30 min)."""
        try:
            r = requests.put(
                f"{BASE}/api/v3/userDataStream",
                headers={'X-MBX-APIKEY': self.api_key},
                params={'listenKey': self._listen_key},
                timeout=5
            )
            return r.status_code == 200
        except:
            return False

    def run(self):
        import websocket

        self._listen_key = self._create_listen_key()
        if not self._listen_key:
            print("  ⚡ Impossible de créer la listen key")
            return

        # Thread dédié au keepalive (toutes les 20 min)
        # CORRECTION: run_forever() bloque — le keepalive DOIT être dans un thread séparé
        # sinon la listen key expire après 60 min et le WS se déconnecte silencieusement.
        def _keepalive_loop():
            while True:
                time.sleep(1200)  # 20 min
                if not self._keepalive_listen_key():
                    new_key = self._create_listen_key()
                    if new_key:
                        self._listen_key = new_key
                        print("  ⚡ ListenKey recréée (keepalive échoué)")

        threading.Thread(target=_keepalive_loop, daemon=True).start()

        while True:
            try:
                self._ws = websocket.WebSocketApp(
                    f"{WSS}/{self._listen_key}",
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                print(f"  ⚡ OrderStream reconnect ({e})")

            # Après déconnexion: renouveler la listen key si nécessaire
            time.sleep(5)
            if not self._keepalive_listen_key():
                new_key = self._create_listen_key()
                if new_key:
                    self._listen_key = new_key
                    print("  ⚡ ListenKey renouvelée après déconnexion")

    def _on_message(self, ws, message):
        data = json.loads(message)
        event_type = data.get('e', '')

        if event_type == 'executionReport':
            fill = {
                'symbol': data.get('s', ''),
                'side': data.get('S', '').lower(),  # BUY or SELL
                'type': data.get('o', ''),           # LIMIT, MARKET
                'price': float(data.get('L', 0)),   # Last executed price
                'qty': float(data.get('l', 0)),     # Last executed qty
                'cum_qty': float(data.get('z', 0)), # Cumulative filled qty
                'orig_qty': float(data.get('q', 0)),# Original order qty
                'status': data.get('X', ''),         # FILLED, PARTIALLY_FILLED, NEW
                'order_id': data.get('i', 0),
                'time': data.get('T', 0),
                'commission': float(data.get('n', 0)),
                'commission_asset': data.get('N', ''),
            }
            with self._lock:
                self._fills.append(fill)

    def _on_error(self, ws, error):
        pass

    def _on_close(self, ws, status, msg):
        pass

    def get_fills(self):
        """Récupère et vide la liste des fills."""
        with self._lock:
            fills = list(self._fills)
            self._fills.clear()
        return fills

    def stop(self):
        if self._ws:
            self._ws.close()


# ═══════════════════════════════════════════════════════════════
# 3. SPREAD ANALYZER — Utilise le carnet d'ordres
# ═══════════════════════════════════════════════════════════════

def get_order_book(symbol, limit=20):
    """Récupère le carnet d'ordres via REST (fallback si WS pas prêt)."""
    try:
        r = requests.get(
            f"{BASE}/api/v3/depth",
            params={'symbol': symbol, 'limit': limit},
            timeout=3
        )
        data = r.json()
        bids = [(float(b[0]), float(b[1])) for b in data.get('bids', [])]
        asks = [(float(a[0]), float(a[1])) for a in data.get('asks', [])]
        return {'bids': bids, 'asks': asks}
    except Exception as e:
        return None

def analyze_spread(bids, asks):
    """Analyse le spread et la liquidité du carnet d'ordres.
    Retourne:
        spread_pct: spread relatif
        bid_liquidity: volume total aux 5 meilleurs bids
        ask_liquidity: volume total aux 5 meilleurs asks
        imbalance: (ask_vol - bid_vol) / (ask_vol + bid_vol) → positif = pression vendeuse
    """
    if not bids or not asks:
        return None

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    if best_bid <= 0:
        return None
    spread_pct = (best_ask - best_bid) / best_bid

    bid_vol_5 = sum(b[0] * b[1] for b in bids[:5])
    ask_vol_5 = sum(a[0] * a[1] for a in asks[:5])
    total = bid_vol_5 + ask_vol_5
    imbalance = (ask_vol_5 - bid_vol_5) / total if total > 0 else 0.0

    return {
        'spread_pct': spread_pct,
        'best_bid': best_bid,
        'best_ask': best_ask,
        'bid_liquidity': round(bid_vol_5, 2),
        'ask_liquidity': round(ask_vol_5, 2),
        'imbalance': round(imbalance, 4),
    }


# ═══════════════════════════════════════════════════════════════
# 4. QUANT ENGINE — Kalman + Vol + OFI + Avellaneda-Stoikov
# ═══════════════════════════════════════════════════════════════

class QuantEngine:
    """
    Moteur quantitatif temps réel qui combine:
    - Kalman Fair Value (débruitage du mid-price)
    - EWMA Volatility (volatilité réalisée)
    - Order Flow Imbalance (déséquilibre du carnet)
    - Avellaneda-Stoikov (spread optimal pour la grille)

    Usage:
        engine = QuantEngine(bot_symbol='BTCUSDT')
        result = engine.tick(bot)  # Appelé à chaque cycle du bot

        # Résultats:
        result['fair_value']      → Prix central pour la grille
        result['grid_pct']        → GRID_PCT dynamique (remplace le fixe 0.06)
        result['sigma']            → Volatilité actuelle
        result['imbalance']        → Déséquilibre carnet (EMA lissé)
        result['center_price']     → Prix de réservation (centré selon inventaire)

    Paramètres Avellaneda-Stoikov réglables:
        GAMMA:     Aversion au risque. 0.01-0.1. +haut = +large spread.
        K:         Sensibilité ordres. 1.0-3.0.
        T_HORIZON: Horizon en secondes. 3600 (1h) par défaut.
    """

    # ══════════════════════════════════════════════════════════
    # Constantes réglables
    # ══════════════════════════════════════════════════════════
    AVS_ENABLED = True             # Activer le spread Avellaneda-Stoikov
    AVS_GAMMA = 0.01               # Aversion au risque (calibré pour GRID_PCT ≈ 6% en vol BTC)
    AVS_K = 1.5                    # Sensibilité ordres (1.0-3.0)
    AVS_T_HORIZON = 600            # Horizon en secondes (10 min, idéal pour grid)
    AVS_GRID_PCT_MIN = 0.02        # GRID_PCT minimum (2% — pas en dessous, sinon trop serré)
    AVS_GRID_PCT_MAX = 0.04        # GRID_PCT maximum (4% — grille serrée pour sell rapide)
    AVS_USE_KALMAN_CENTER = False   # Désactivé : centrer la grille sur le prix réel, pas le Kalman
    AVS_USE_OFI_ADJUST = True      # Ajuster le centre selon l'OFI

    # Kalman
    KALMAN_Q = 0.01                # Bruit de processus (0.01 = réactif)
    KALMAN_R = 0.25                # Bruit d'observation (0.25 = ~0.5*spread²)

    # Vol
    VOL_HALFLIFE = 100             # Demi-vie EWMA (100 ticks ~ 17 min à 10s/tick)
    VOL_INITIAL = 15.0             # Vol initiale (BTC: ~$16/tick pour 2.5% journalier)

    # OFI
    OFI_DEPTH = 5                  # Niveaux de carnet à considérer
    OFI_EMA_ALPHA = 0.15           # Lissage EMA (0.15 = réactif mais stable)
    OFI_ALPHA_IMPACT = 2.0         # Impact USD par unité d'imbalance

    def __init__(self, symbol='BTCUSDT'):
        self.symbol = symbol
        self._kalman = None
        self._vol = None
        self._ofi = None
        self._avs = None
        self._last_mid = 0.0
        self._initialized = False

    def _lazy_init(self):
        """Initialisation paresseuse (ne rien créer tant qu'on a pas de données)."""
        if not self._initialized:
            from kalman_fair_value import KalmanFairValue
            from vol_estimator import RealizedVolEstimator
            from imbalance_signal import OrderFlowImbalance
            from avellaneda_quote import AvellanedaStoikov

            self._kalman = KalmanFairValue(
                process_noise_var=self.KALMAN_Q,
                measurement_noise_var=self.KALMAN_R,
            )
            self._vol = RealizedVolEstimator(
                halflife=self.VOL_HALFLIFE,
                initial_vol=self.VOL_INITIAL,
            )
            self._ofi = OrderFlowImbalance(
                depth=self.OFI_DEPTH,
                ema_alpha=self.OFI_EMA_ALPHA,
                alpha_impact=self.OFI_ALPHA_IMPACT,
            )
            self._avs = AvellanedaStoikov(
                gamma=self.AVS_GAMMA,
                k=self.AVS_K,
                T_horizon=self.AVS_T_HORIZON,
            )
            self._initialized = True

    def tick(self, price: float, inventory: float = 0.0,
             max_inventory: float = 1.0,
             order_book: dict | None = None) -> dict:
        """
        Exécute un cycle complet du moteur quantitatif.

        Args:
            price:          Prix actuel (mid-price ou last price)
            inventory:      Position actuelle en tokens (0 si pas de position)
            max_inventory:  Inventaire max toléré
            order_book:     Carnet d'ordres {'bids': [...], 'asks': [...]} (optionnel)

        Retourne dict avec:
            fair_value:     Prix central débruité
            grid_pct:       GRID_PCT dynamique (remplace 0.06 fixe)
            center_price:   Prix de réservation (centré selon inventaire)
            sigma:          Volatilité USD/sec
            imbalance:      OFI lissé [-1, +1]
            half_spread:    Demi-spread optimal (USD)
            grid_pct_raw:   GRID_PCT avant clamp
            enabled:        True si AVS_ENABLED
        """
        self._lazy_init()

        if not self.AVS_ENABLED:
            return {
                'fair_value': price,
                'grid_pct': 0.06,
                'center_price': price,
                'sigma': 0.0,
                'imbalance': 0.0,
                'half_spread': 0.0,
                'grid_pct_raw': 0.06,
                'enabled': False,
            }

        # 1. Kalman Fair Value — débruitage du mid-price
        fair_value = self._kalman.update(price)

        # 2. EWMA Volatilité
        sigma = self._vol.update(price)

        # 3. Order Flow Imbalance (si carnet disponible)
        imbalance = 0.0
        if order_book and 'bids' in order_book and 'asks' in order_book:
            imbalance = self._ofi.update(
                order_book['bids'], order_book['asks']
            )

        # 4. Ajustement du fair value selon OFI
        if self.AVS_USE_OFI_ADJUST and self._ofi.is_ready:
            ofi_adj = self._ofi.fair_value_adjustment
            fair_value_adj = fair_value + ofi_adj
        else:
            fair_value_adj = fair_value

        # 5. Avellaneda-Stoikov — spread optimal
        avs_result = self._avs.compute(
            fair_value=fair_value_adj,
            sigma=sigma,
            inventory=inventory,
            max_inventory=max_inventory,
        )

        # GRID_PCT final
        grid_pct = avs_result.grid_pct
        grid_pct = max(self.AVS_GRID_PCT_MIN,
                       min(self.AVS_GRID_PCT_MAX, grid_pct))

        # Prix central
        if self.AVS_USE_KALMAN_CENTER:
            center_price = avs_result.reservation
        else:
            center_price = price

        self._last_mid = price

        return {
            'fair_value': round(fair_value, 2),
            'grid_pct': round(grid_pct, 6),
            'center_price': round(center_price, 2),
            'sigma': round(sigma, 6),
            'imbalance': round(imbalance, 4),
            'half_spread': round(avs_result.half_spread, 2),
            'grid_pct_raw': round(avs_result.grid_pct, 6),
            'enabled': True,
        }

    @property
    def kalman(self):
        self._lazy_init()
        return self._kalman

    @property
    def vol(self):
        self._lazy_init()
        return self._vol

    @property
    def ofi(self):
        self._lazy_init()
        return self._ofi

    @property
    def avs(self):
        self._lazy_init()
        return self._avs
