#!/usr/bin/env python3
"""
⚡ Bot Trader GÉNÉRIQUE — Grille dynamique + Mode GEL + Kelly Allocation
Utilisation:
  Paper:   python3 bot_trader.py --symbol BTCUSDT --capital 50
  Réel:    python3 bot_trader.py --symbol BTCUSDT --capital 50 --real
  Kelly:   python3 bot_trader.py --symbol BTCUSDT --kelly --real
"""
import json, requests, time, os, sys, argparse, hashlib, hmac, urllib.parse
from datetime import datetime, date
from decimal import Decimal, ROUND_DOWN, ROUND_UP

import perf_tracker  # 📊 Performance Tracker + Kelly Allocation
from hft import PriceFeed, OrderStream, analyze_spread, get_order_book, QuantEngine  # ⚡ HFT
from sr_levels import SRLevels  # 📊 Supports/Résistances + Volume Profile + Trendlines
from market_gate import MarketGate  # 🚦 Market Gate — Protection macro marché
from news_feed import NewsFeed  # 📡 News Feed — Multi-source sentiment + impact
from sync_engine import SyncEngine  # 🔄 Sync Engine — Synchronisation universelle

BASE = "https://api.binance.com"
BASE_DIR = "/root/.hermes/profiles/immo/scripts/bots"

# Paramètres grille (communs)
GRID_PCT = 0.02          # Plage 2% → BUY/SELL espacés de ~1%
GRID_LEVELS = 2          # 1 BUY + 1 SELL
CAPITAL_ALLOC = 1.0      # 100% du capital en ordres
PER_ORDER_VALUE = 250.0  # 💰 $250/ordre → 2 niveaux (1 BUY + 1 SELL), $500 capital total
INITIAL_POS_PCT = 0.98   # 98% dans le BUY initial, 2% buffer USDT
MAX_DAILY_LOSS_PCT = 0.03
NUCLEAR_FLOOR = 0.35
PHASE2_STOP_PCT = 0.02
REDEPLOY_THRESHOLD_PCT = 0.50
FREEZE_DROP_PCT = 0.03
FREEZE_WINDOW_H = 2
FREEZE_UNFREEZE_BOUNCE = 0.015
FREEZE_UNFREEZE_RANGE = 0.02
FREEZE_UNFREEZE_MIN_TIME_BASE = 600  # base 10 min (surchargé par calculateur adaptatif)
FREEZE_UNFREEZE_MAX_TIME = 1800   # max 30 min
FREEZE_L2_MANUAL_ONLY = True     # -8%+ → dégel manuel uniquement

# 📐 EMA Dynamic Buy Zones
EMA_ENABLED = True                # Activer les zones d'achat dynamiques
EMA_PERIOD = 30                   # Période EMA (bougies 1h)
EMA_ZONE_EXPENSIVE = 0.05         # > +5% au-dessus EMA → ne pas acheter
EMA_ZONE_DIP_WEAK = -0.015        # 0 à -1.5% → 0.75x
EMA_ZONE_DIP_NORMAL = -0.03       # -1.5% à -3% → 1.0x
EMA_ZONE_DIP_STRONG = -0.05       # -3% à -5% → 1.5x
EMA_ABOVE_MULTIPLIER = 0.5        # Multiplicateur quand au-dessus EMA
EMA_WEAK_MULTIPLIER = 0.75        # Petit dip
EMA_NORMAL_MULTIPLIER = 1.0       # Dip normal
EMA_STRONG_MULTIPLIER = 1.5       # Gros dip
EMA_HARD_STOP_MULTIPLIER = 0.0    # Crash → ne pas acheter

# 🎯 Trailing Profit Lock
TRAILING_ENABLED = True            # Activer le verrouillage de profit
TRAILING_PROFIT_PCT = 0.012        # S'active à +1.2% de profit
TRAILING_CALLBACK_PCT = 0.003      # Vendre sur -0.3% de pullback du pic

# 🛟 Capital Liberation — Déblocage automatique des positions bloquées
CAPITAL_LIBERATION_ENABLED = True    # Activer le déblocage
CAPITAL_LIB_MIN_HOLD = 1800         # ⏱️ Temps minimum avant action: 30 min
CAPITAL_LIB_DROP_PCT = 0.015        # 📉 Baisse min sous le BUY: -1.5%
CAPITAL_LIB_PERSIST_SEC = 300       # ⏳ Vérification persistante: 5 min
CAPITAL_LIB_NEW_SPREAD = 0.003      # 📐 Nouveau SELL à +0.3% du prix actuel

# ── Option B : Reconstruction orphelins ──
ORPHAN_REBUILD_ENABLED = True       # Activer la reconstruction
ORPHAN_REBUILD_SPREAD = 0.005       # 0.5% au-dessus du marché
ORPHAN_REBUILD_ESCALATE_DAYS = 7    # Jours avant le premier réajustement
ORPHAN_MAX_LOSS_PCT = 0.03          # Perte max autorisée (3%)
ORPHAN_REBUILD_MIN_WAIT = 30        # Secondes avant de considérer comme orphelin

# 🧠 P1 Filter — Blocage BUY si tendance 24h négative
P1_FILTER_ENABLED = True           # Activer le filtre P1
P1_SYMBOLS = ['ALL']               # 'ALL' = toutes les paires, ou liste: ['XLM', 'ADA', ...]
P1_MIN_DROP_PCT = 1.0              # Bloquer seulement si la baisse 24h dépasse ce seuil (%)

# 🔄 Rebalancing — Auto-stop pour paires défaillantes
REBALANCE_ENABLED = True           # Activer le rebalancement automatique
REBALANCE_MIN_CYCLES = 10          # Cycles minimum avant analyse
REBALANCE_WR_THRESHOLD = 0.40      # WR en dessous → considéré défaillant
REBALANCE_CAPITAL_FLOOR = 20.0     # Capital minimum pour une paire défaillante

# ⚡ HFT — Optimisations basse latence
HFT_ENABLED = True                 # Activer le mode HFT (WebSocket)
HFT_USE_WS_PRICE = True            # Prix via WebSocket au lieu de REST
HFT_USE_ORDER_STREAM = True        # Détection fills via User Data Stream
HFT_FALLBACK_REST = True           # Fallback REST si WebSocket déconnecté

# 📐 AVS — Avellaneda-Stoikov Dynamic Grid (remplace GRID_PCT fixe)
AVS_ENABLED = False                 # Désactivé — on veut un GAP fixe de 1%
# Paramètres AVS réglables dans hft.py → QuantEngine (attributs de classe)

# 📊 Order Book Grid Asymmetry
GRID_ASYMMETRY_ENABLED = True      # Décaler la grille selon l'imbalance du carnet
GRID_ASYMMETRY_MAX_SHIFT = 0.30    # Shift max: 30% de la largeur de grille
GRID_ASYMMETRY_THRESHOLD = 0.20    # Seuil d'imbalance pour activer l'asymétrie

# 🧠 MAF — Multi-Agent Freeze (remplace le seuil fixe -3%)
MAF_ENABLED = False                 # Désactivé — annulait les ordres valides sur des dips normaux (sabotage répétitif)

# 📡 TA — TrendAgent (SuperTrend + RSI + DMI + Heikin Ashi)
TA_ENABLED = True                  # Activer l'analyse de tendance temps réel
TA_SUPER_PERIOD = 10               # Période SuperTrend (ATR)
TA_SUPER_MULTIPLIER = 3            # Multiplicateur SuperTrend
TA_RSI_PERIOD = 14                 # Période RSI
TA_DMI_PERIOD = 14                 # Période DMI

# 🚦 MG — Market Gate (Protection macro marché)
MG_ENABLED = True                  # Activer la protection macro
MG_REFRESH_SEC = 1800              # Rafraîchir toutes les 30 min
MG_BLOCK_BUYS = True               # Bloquer les BUY si score ≥ 6

# 📡 NF — NewsFeed (Multi-source sentiment + impact news)
NF_ENABLED = True                  # Activer la veille news temps réel
NF_REFRESH_SEC = 900               # Rafraîchir toutes les 15 min
NF_BLOCK_BUYS = True               # Bloquer les BUY si sentiment très négatif
NF_SENTIMENT_THRESHOLD = -0.2      # Seuil sentiment pour bloquer BUY
NF_GRID_ADJUST_ENABLED = True      # Ajuster le spread de grille selon le sentiment

# 📊 SR — Supports/Résistances (ajuste freeze + déploiement)
SR_ENABLED = True                  # Activer l'analyse S/R
SR_INTERVAL = '1h'                 # Résolution des bougies pour l'analyse
SR_LOOKBACK = 100                  # Nombre de bougies à analyser
SR_REFRESH_SEC = 1800              # Rafraîchir le cache toutes les 30 min
SR_FREEZE_BUFFER_PCT = 0.01        # Si prix <1% d'un support → moins agressif
SR_FREEZE_DIV_NEAR_SUPPORT = 1.3  # Divisor seuil freeze quand près d'un support (élargit le seuil)
SR_GRID_ALIGN_SUPPORT = True       # Centrer la grille près du support le + proche

# ─── Calcul adaptatif du temps de dégel ──────────────────────
def _get_unfreeze_min_time(drop_pct):
    """Timer adaptatif basé sur l'amplitude de la baisse qui a causé le gel.
    -3% → ~9 min, -5% → ~15 min, -7% → ~21 min
    Max 30 min. -8%+ → retourne None = pas d'auto-dégel.
    """
    if drop_pct >= 0.08 and FREEZE_L2_MANUAL_ONLY:
        return None
    t = max(drop_pct * 300.0, FREEZE_UNFREEZE_MIN_TIME_BASE)
    return min(int(t), FREEZE_UNFREEZE_MAX_TIME)

# 🛡️ Price Circuit Breaker
PRICE_CB_L1 = 0.05

# ─── Frais de trading ────────────────────────────────────────
FEE_RATE = 0.00075  # 0.075% maker avec BNB discount (ordres LIMIT)
PRICE_CB_L2 = 0.08

# ─── Clés API ──────────────────────────────────────────────
BINANCE_API_KEY = ""
BINANCE_SECRET_KEY = ""
TG_TOKEN = ""
TG_CHAT = "7950223827"

_env = "/root/.hermes/profiles/immo/.env"
NF_CRYPTOPANIC_KEY = ""
NF_NEWSAPI_KEY = ""
if os.path.exists(_env):
    for l in open(_env):
        if l.startswith("BINANCE_API_KEY="):
            BINANCE_API_KEY = l.split("=", 1)[1].strip().strip('"').strip("'")
        elif l.startswith("BINANCE_SECRET_KEY="):
            BINANCE_SECRET_KEY = l.split("=", 1)[1].strip().strip('"').strip("'")
        elif l.startswith("TELEGRAM_BOT_TOKEN="):
            TG_TOKEN = l.split("=", 1)[1].strip().strip('"').strip("'")
        elif l.startswith("CRYPTOPANIC_API_KEY="):
            NF_CRYPTOPANIC_KEY = l.split("=", 1)[1].strip().strip('"').strip("'")
        elif l.startswith("NEWSAPI_KEY="):
            NF_NEWSAPI_KEY = l.split("=", 1)[1].strip().strip('"').strip("'")

def fmt_qty(q, step=0.1): return float((Decimal(str(q))/Decimal(str(step))).to_integral_value(rounding=ROUND_DOWN)*Decimal(str(step)))
def fmt_price(p, tick=0.0001): return float((Decimal(str(p))/Decimal(str(tick))).to_integral_value(rounding=ROUND_DOWN)*Decimal(str(tick)))

# ─── EMA Calculation ────────────────────────────────────────
def get_ema(symbol, period=30, interval='1h'):
    """Calcule l'EMA sur N bougies Binance.
    Retourne (ema_value, current_price) ou None si erreur."""
    try:
        r = requests.get(
            f"{BASE}/api/v3/klines",
            params={'symbol': symbol, 'interval': interval, 'limit': period + 10},
            timeout=5
        )
        data = r.json()
        closes = [float(k[4]) for k in data]  # k[4] = close price
        if len(closes) < period:
            return None

        # SMA comme seed de l'EMA
        sma = sum(closes[:period]) / period
        multiplier = 2.0 / (period + 1)

        ema = sma
        for price in closes[period:]:
            ema = (price - ema) * multiplier + ema

        current_price = closes[-1]
        return (ema, current_price)
    except Exception as e:
        print(f"  ⚠️ EMA error: {e}")
        return None

def get_ema_multiplier(ema, current_price):
    """Calcule le multiplicateur d'achat basé sur la distance à l'EMA.
    Retourne (multiplier, zone_name)."""
    if ema is None or ema == 0 or not EMA_ENABLED:
        return 1.0, "disabled"

    dist = (current_price - ema) / ema  # distance relative à l'EMA

    if dist >= EMA_ZONE_EXPENSIVE:
        return EMA_HARD_STOP_MULTIPLIER, "expensive"     # 0.0x → no buy
    elif dist > 0:
        return EMA_ABOVE_MULTIPLIER, "above_ema"          # 0.5x
    elif dist >= EMA_ZONE_DIP_WEAK:
        return EMA_WEAK_MULTIPLIER, "weak_dip"            # 0.75x
    elif dist >= EMA_ZONE_DIP_NORMAL:
        return EMA_NORMAL_MULTIPLIER, "normal_dip"        # 1.0x
    elif dist >= EMA_ZONE_DIP_STRONG:
        return EMA_STRONG_MULTIPLIER, "strong_dip"        # 1.5x
    else:
        return EMA_HARD_STOP_MULTIPLIER, "hard_stop"      # 0.0x → crash zone

def get_price(symbol):
    for _ in range(3):
        try:
            r = requests.get(f"{BASE}/api/v3/ticker/price?symbol={symbol}", timeout=5)
            return float(r.json()['price'])
        except Exception: time.sleep(1)
    return 0.0

# 🧠 P1 Filter — Vérifie si la tendance 24h est négative
def get_trend_24h(symbol):
    """Retourne True si le prix actuel < prix il y a 24h (tendance négative)"""
    try:
        r = requests.get(
            f"{BASE}/api/v3/klines",
            params={'symbol': symbol, 'interval': '1h', 'limit': 25},
            timeout=5
        )
        data = r.json()
        if len(data) < 2:
            return False, 0.0, 0.0
        price_now = float(data[-1][4])
        price_24h = float(data[0][4])
        trend_down = price_now < price_24h
        change_pct = (price_now - price_24h) / price_24h * 100
        return trend_down, price_now, price_24h
    except Exception as e:
        print(f"  ⚠️ P1 error: {e}")
        return False, 0.0, 0.0

def _binance_call(method, url, headers, timeout=10, max_retries=3):
    """Exécute un appel HTTP Binance avec retry exponentiel sur 429/5xx.
    Ne reessaie PAS les 4xx (erreurs client — ordre invalide, solde insuffisant, etc.)
    car ce sont des erreurs permanentes."""
    delay = 1
    for attempt in range(max_retries):
        try:
            r = getattr(requests, method)(url, headers=headers, timeout=timeout)
            if r.status_code == 429:
                # Rate limit: respecter le header Retry-After si présent
                wait = int(r.headers.get('Retry-After', delay * 2))
                print(f"⚠️ Rate limit (429) — attente {wait}s (tentative {attempt+1}/{max_retries})")
                time.sleep(wait)
                delay = wait
                continue
            if r.status_code >= 500:
                if attempt < max_retries - 1:
                    print(f"⚠️ Erreur serveur {r.status_code} — retry dans {delay}s")
                    time.sleep(delay)
                    delay = min(delay * 2, 16)
                    continue
            return r
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"⚠️ Réseau: {e} — retry dans {delay}s")
                time.sleep(delay)
                delay = min(delay * 2, 16)
            else:
                print(f"⚠️ Réseau: {e} — abandon après {max_retries} tentatives")
    return None

def binance_signed_get(path, params=None):
    """Appel API Binance authentifié (GET) avec retry exponentiel."""
    if not BINANCE_API_KEY:
        return None
    params = params or {}
    params['timestamp'] = int(time.time() * 1000)
    query = urllib.parse.urlencode(params)
    signature = hmac.new(BINANCE_SECRET_KEY.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"{BASE}{path}?{query}&signature={signature}"
    headers = {'X-MBX-APIKEY': BINANCE_API_KEY}
    r = _binance_call('get', url, headers)
    if r and r.status_code != 200:
        print(f"⚠️ API Error {r.status_code}: {r.text[:200]}")
    return r

def binance_signed_post(path, params):
    """Appel API Binance authentifié (POST) avec retry exponentiel."""
    if not BINANCE_API_KEY:
        return None
    params['timestamp'] = int(time.time() * 1000)
    query = urllib.parse.urlencode(params)
    signature = hmac.new(BINANCE_SECRET_KEY.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"{BASE}{path}?{query}&signature={signature}"
    headers = {'X-MBX-APIKEY': BINANCE_API_KEY}
    r = _binance_call('post', url, headers)
    if r and r.status_code != 200:
        print(f"⚠️ API Error {r.status_code}: {r.text[:200]}")
    return r

def binance_signed_delete(path, params):
    """Appel API Binance authentifié (DELETE) avec retry exponentiel."""
    if not BINANCE_API_KEY:
        return None
    params['timestamp'] = int(time.time() * 1000)
    query = urllib.parse.urlencode(params)
    signature = hmac.new(BINANCE_SECRET_KEY.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"{BASE}{path}?{query}&signature={signature}"
    headers = {'X-MBX-APIKEY': BINANCE_API_KEY}
    r = _binance_call('delete', url, headers)
    if r and r.status_code != 200:
        print(f"⚠️ API Error {r.status_code}: {r.text[:200]}")
    return r

class BotTrader:
    def __init__(self, symbol, capital=100.0, name="", real_mode=False, dry_run=False, kelly=False):
        self.symbol = symbol
        self.pair = symbol.replace("USDT", "/USDT")
        self.name = name or symbol.replace("USDT", "")
        self.real_mode = real_mode
        self.dry_run = dry_run
        self.kelly = kelly

        # 📊 Kelly Allocation — remplace le capital fixe
        if kelly:
            perf_tracker.register_bot(self.name)
            self.capital = perf_tracker.calc_kelly_capital(self.name)
            self._kelly_updated = time.time()
        else:
            self.capital = capital

        # ⏱️ Cycle timing tracker — stocke le timestamp de chaque BUY fill
        self._buy_fill_times = {}  # {price_str: timestamp}

        # ⚡ HFT — WebSocket price feed + order stream
        self._price_feed = None
        self._order_stream = None
        if HFT_ENABLED:
            self._price_feed = PriceFeed(symbol)
            self._price_feed.start()
            if HFT_USE_ORDER_STREAM and real_mode and BINANCE_API_KEY:
                self._order_stream = OrderStream(BINANCE_API_KEY, BINANCE_SECRET_KEY)
                self._order_stream.start()
                print(f"  ⚡ HFT: WS price + OrderStream actif")
            else:
                print(f"  ⚡ HFT: WS price actif")

        # 📐 AVS — QuantEngine (Kalman + Vol + OFI + Avellaneda-Stoikov)
        self._quant = QuantEngine(symbol)
        self._state = {}

        # 📊 SR — Supports/Résistances (cache-aware, refresh périodique)
        self._sr = SRLevels(interval=SR_INTERVAL, lookback=SR_LOOKBACK) if SR_ENABLED else None
        self._last_sr_refresh = 0

        # 🚦 MG — Market Gate (protection macro marché)
        self._mg = MarketGate() if MG_ENABLED else None
        self._last_mg_refresh = 0
        self._mg_result = None

        # 📡 NF — NewsFeed (multi-source sentiment + impact news)
        self._nf = NewsFeed(cryptopanic_key=NF_CRYPTOPANIC_KEY,
                            newsapi_key=NF_NEWSAPI_KEY) if NF_ENABLED else None
        self._last_nf_refresh = 0
        self._nf_result = None

        # Lot size & tick size
        self.lot_step = 0.1
        self.tick_size = 0.0001
        try:
            r = requests.get(f"{BASE}/api/v3/exchangeInfo?symbol={symbol}", timeout=5)
            data = r.json()
            for s in data.get('symbols', []):
                for f in s.get('filters', []):
                    if f['filterType'] == 'LOT_SIZE':
                        self.lot_step = float(f['stepSize'])
                    elif f['filterType'] == 'PRICE_FILTER':
                        self.tick_size = float(f['tickSize'])
                    elif f['filterType'] in ('MIN_NOTIONAL', 'NOTIONAL'):
                        self.min_notional = float(f['minNotional'])
        except Exception:
            pass
        self.min_notional = getattr(self, 'min_notional', 5.0)
        print(f"  Lot step: {self.lot_step}, Tick size: {self.tick_size}, Min notional: ${self.min_notional:.0f}")
        if self.real_mode:
            print(f"  🌐 Mode RÉEL{' (DRY-RUN)' if self.dry_run else ''}")

        # Fichiers
        os.makedirs(BASE_DIR, exist_ok=True)
        base = symbol.lower().replace("usdt", "")
        self.state_file = f"{BASE_DIR}/{base}_state.json"
        self.fills_file = f"{BASE_DIR}/{base}_fills.json"
        self.stop_file = f"/tmp/BOT_STOP_{symbol}"

        # 🔄 Sync Engine — XRP pilote uniquement
        if symbol == 'XRPUSDT' and real_mode:
            if not self.dry_run:
                self.engine = SyncEngine(
                    api_key=BINANCE_API_KEY,
                    secret_key=BINANCE_SECRET_KEY,
                    symbol=symbol,
                    bot_name=self.name,
                    tick_size=self.tick_size,
                    lot_step=self.lot_step,
                    balance_asset='XRP'
                )
                print(f"  🔄 Sync Engine actif (XRP pilote)")
            else:
                self.engine = None
        else:
            self.engine = None
        # 📦 Checkpoint automatique (flag pour éviter les boucles)
        self._checkpoint_dir = "/root/.hermes/checkpoints"
        self._checkpoint_script = "/root/.hermes/profiles/immo/scripts/checkpoint.py"
        # 📊 Phase 1 — Fichier de divergence (lecture seule)
        self.divergence_file = f"{BASE_DIR}/{base}_divergence_log.json"

        self.last_price_time = 0
        self.last_redeploy_time = 0
        self._price_history = []

    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file) as f: return json.load(f)
            except Exception: pass
        return None

    def save_state(self, s):
        self._state = s
        # Écriture atomique: temp file + rename pour éviter la corruption si crash mid-write
        tmp = self.state_file + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(s, f, indent=2)
        os.replace(tmp, self.state_file)

    def _state_from_report(self, report):
        """Convertit un RebuildReport en dict state local (pour XRP pilote)"""
        if report.state == 'LONG_OPEN':
            s = {
                'state': 'LONG_OPEN',
                'capital': self.capital,
                'initial_capital': self.capital,
                'tokens_free': report.tokens_free,
                'tokens_locked': report.tokens_locked,
                'usdt_free': report.usdt_free,
                'usdt_locked': report.usdt_locked,
                'level_orders': {},
                'total_cycles': 0,
                'total_profit_est': 0.0,
                'pending_cycles': 0,
                'frozen': False,
                'real_mode': self.real_mode,
                'last_buy_price': report.pru,
                'last_buy_time': time.time(),
                'entry_price': report.pru if report.pru > 0 else self._get_price(),
                'day_date': datetime.now().strftime('%Y-%m-%d'),
                'day_start_value': report.capital,
                'daily_loss': 0.0,
                'levels': [report.pru, round(report.pru * 1.01, 4)],
                'qty': 224.1,
            }
        else:
            s = {
                'state': report.state,
                'capital': self.capital,
                'initial_capital': self.capital,
                'tokens_free': report.tokens_free,
                'tokens_locked': report.tokens_locked,
                'usdt_free': report.usdt_free,
                'usdt_locked': report.usdt_locked,
                'level_orders': report.level_orders,
                'total_cycles': 0,
                'total_profit_est': 0.0,
                'pending_cycles': 0,
                'frozen': False,
                'real_mode': self.real_mode,
                'last_buy_price': report.pru,
                'last_buy_time': time.time(),
                'entry_price': report.pru if report.pru > 0 else self._get_price(),
                'day_date': datetime.now().strftime('%Y-%m-%d'),
                'day_start_value': report.capital,
                'daily_loss': 0.0,
                'levels': [],
                'qty': 0,
            }
        self.save_state(s)
        return s

    def log_fill(self, fill):
        try:
            fills = []
            if os.path.exists(self.fills_file):
                with open(self.fills_file) as f: fills = json.load(f)
            fill['time'] = time.time()
            fills.append(fill)
            with open(self.fills_file, 'w') as f: json.dump(fills, f, indent=2)
        except Exception: pass

    def tg_send(self, msg):
        if not TG_TOKEN: return
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
            requests.post(url, json={'chat_id': TG_CHAT, 'text': f"🤖 <b>{self.name}</b>\n{msg}", 'parse_mode': 'HTML'}, timeout=5)
        except Exception: pass

    # ⚡ HFT: prix via WebSocket (50ms) ou fallback REST (250ms)
    def _get_price(self):
        """Récupère le dernier prix. WebSocket d'abord, REST en fallback.
        Track aussi le min/max du WS entre deux ticks pour les fills."""
        if HFT_USE_WS_PRICE and self._price_feed and self._price_feed.connected and self._price_feed.age < 10:
            p = self._price_feed.price
            if p > 0:
                # Track min/max WS entre ticks
                if not hasattr(self, '_ws_min_since_tick') or self._ws_min_since_tick == 0:
                    self._ws_min_since_tick = p
                    self._ws_max_since_tick = p
                else:
                    self._ws_min_since_tick = min(self._ws_min_since_tick, p)
                    self._ws_max_since_tick = max(self._ws_max_since_tick, p)
                return p
        if HFT_FALLBACK_REST:
            return get_price(self.symbol)
        return 0.0

    # ── Cycle Performance Logging ────────────────────────────
    def _log_cycle(self, buy_price, sell_price, qty, profit):
        """Logge un cycle complété dans perf_tracker."""
        now = time.time()
        # Estimer la durée du cycle depuis le BUY fill
        buy_key = str(buy_price)
        buy_time = self._buy_fill_times.pop(buy_key, None)
        cycle_duration = (now - buy_time) if buy_time else 0.0

        perf_tracker.log_cycle(
            pair_name=self.name,
            buy_price=buy_price,
            sell_price=sell_price,
            qty=qty,
            profit=profit,
            cycle_duration=cycle_duration,
        )

    def check_stop(self):
        if os.path.exists(self.stop_file):
            with open(self.stop_file) as f: return f.read().strip()
        return ""

    def _check_aging_buys(self, s):
        """
        🔄 BUY_REPOSITIONED — Repositionne les BUY vieillissants (>24h, >6% sous marché).
        N'intervient pas sur XRP (SyncEngine gère).
        Max 1 repositionnement par ordre sur 24h.
        """
        if not self.real_mode or self.dry_run or self.engine:
            return

        now = time.time()
        price = self._get_price()
        if price <= 0:
            return

        # Récupérer les ordres ouverts depuis Binance
        open_orders = self._real_get_open_orders()
        if open_orders is None:
            return  # Erreur API

        # Historique des repositionnements (depuis l'état)
        repos = s.get('_buy_repositions', {})

        for o in open_orders:
            if o['side'].upper() != 'BUY':
                continue

            oid = o['orderId']
            buy_price = float(o['price'])
            orig_qty = float(o['origQty'])
            executed_qty = float(o['executedQty'])

            # Condition : non partiellement exécuté
            if executed_qty > 0:
                continue

            # Âge de l'ordre
            # Récupérer updateTime depuis allOrders si possible, sinon via orderId
            try:
                r = binance_signed_get('/api/v3/order', {
                    'symbol': self.symbol, 'orderId': oid
                })
                if r and r.status_code == 200:
                    od = r.json()
                    create_time = int(od.get('time', 0)) / 1000
                else:
                    continue
            except:
                continue

            age_h = (now - create_time) / 3600

            # Condition : > 24h
            if age_h < 24:
                continue

            # Distance au marché
            dist_pct = (buy_price - price) / price * 100  # négatif si sous le marché

            # Condition : > 6% sous le marché
            if dist_pct >= -6.0:
                continue

            # Condition : max 1 repositionnement par 24h
            last_repo = repos.get(str(oid), 0)
            if last_repo > 0 and (now - last_repo) < 86400:
                continue

            # ── Annuler l'ancien BUY ──
            qty_str = f"{orig_qty:.{max(0, -int(Decimal(str(self.lot_step)).as_tuple().exponent))}f}"
            price_str = f"{buy_price:.{max(0, -int(Decimal(str(self.tick_size)).as_tuple().exponent))}f}"

            r = binance_signed_delete('/api/v3/order', {
                'symbol': self.symbol, 'orderId': oid
            })
            if not (r and r.status_code == 200):
                print(f"  ❌ BUY_REPOSITIONED: échec annulation ordre {oid}")
                continue

            # ── Calculer le nouveau prix BUY ──
            # Stratégie : 1% sous le marché actuel (comme le grid standard)
            new_price = round(price * 0.99,
                              max(0, -int(Decimal(str(self.tick_size)).as_tuple().exponent)))
            new_price_str = f"{new_price:.{max(0, -int(Decimal(str(self.tick_size)).as_tuple().exponent))}f}"

            # ── Placer le nouveau BUY ──
            params = {
                'symbol': self.symbol,
                'side': 'BUY',
                'type': 'LIMIT',
                'timeInForce': 'GTC',
                'quantity': qty_str,
                'price': new_price_str,
            }
            r2 = binance_signed_post('/api/v3/order', params)
            placed = r2 and r2.status_code == 200
            new_oid = r2.json().get('orderId') if placed else None

            # ── Journaliser ──
            repos[str(oid)] = now
            s['_buy_repositions'] = repos
            self.save_state(s)

            log_entry = (
                f"🔄 BUY_REPOSITIONED | {self.symbol}\n"
                f"  Ancien: ${buy_price:.4f} × {qty_str} (id={oid})\n"
                f"  Nouveau: ${new_price:.4f} (id={new_oid if placed else 'ÉCHEC'})\n"
                f"  Marché: ${price:.4f} | Distance: {dist_pct:.1f}% | Âge: {age_h:.0f}h\n"
                f"  Statut: {'✅ Placé' if placed else '❌ Échec'}"
            )
            print(f"\n  {log_entry.replace(chr(10), chr(10)+'  ')}")
            self.tg_send(log_entry)

    # 📦 Checkpoint automatique (non-bloquant)
    def _checkpoint(self, reason, timeout=20):
        """Crée un checkpoint de manière asynchrone. Ne bloque jamais le bot."""
        try:
            import subprocess
            subprocess.Popen(
                [sys.executable, self._checkpoint_script, "create",
                 "--reason", f"{reason}-{self.name}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        except Exception:
            pass  # Échec silencieux — ne jamais bloquer le bot

    # ── API Binance (ordres réels) ──────────────────────────

    def _real_place_order(self, side, qty, price):
        """Place un ordre LIMIT réel (ou dry-run)"""
        qty_str = f"{qty:.{max(0, -int(Decimal(str(self.lot_step)).as_tuple().exponent))}f}"
        price_str = f"{price:.{max(0, -int(Decimal(str(self.tick_size)).as_tuple().exponent))}f}"
        params = {
            'symbol': self.symbol,
            'side': side.upper(),
            'type': 'LIMIT',
            'timeInForce': 'GTC',
            'quantity': qty_str,
            'price': price_str,
        }
        if self.dry_run:
            print(f"  [DRY-RUN] {side} {qty_str} @ {price_str}")
            return {'status': 'DRY_RUN', 'orderId': -1, 'side': side, 'origQty': qty_str, 'price': price_str}
        r = binance_signed_post('/api/v3/order', params)
        if r and r.status_code == 200:
            data = r.json()
            print(f"  ✅ Ordre {side} {qty_str}@{price_str} → ID {data.get('orderId')}")
            return data
        else:
            print(f"  ❌ Échec ordre {side} {qty_str}@{price_str}")
            return None

    def _real_cancel_all(self):
        """Annule tous les ordres ouverts sur cette paire (BUY + SELL).
        ⚠️ Usage restreint — réservé aux arrêts explicitement autorisés par l'utilisateur
        (stop file, Ctrl+C). Pour les gels et protections automatiques, utiliser
        _real_cancel_buys_only() qui préserve les SELL."""
        if self.dry_run:
            print(f"  [DRY-RUN] Cancel all orders {self.symbol}")
            return True
        r = binance_signed_delete('/api/v3/openOrders', {'symbol': self.symbol})
        if r and r.status_code == 200:
            cancelled = r.json()
            if cancelled:
                print(f"  ✅ {len(cancelled)} ordres annulés")
            return True
        return False

    def _real_cancel_buys_only(self):
        """Annule UNIQUEMENT les ordres BUY ouverts — ne touche jamais aux SELL.
        Règle SPOT: les SELL ne peuvent être annulés sans autorisation explicite
        de l'utilisateur (frais déjà engagés économiquement sur position longue)."""
        if self.dry_run:
            print(f"  [DRY-RUN] Cancel BUY orders only {self.symbol} (SELL préservés)")
            return True, 0
        open_orders = self._real_get_open_orders()
        if open_orders is None:
            return False, 0
        cancelled = 0
        preserved = 0
        for o in open_orders:
            if o['side'].upper() == 'BUY':
                r = binance_signed_delete('/api/v3/order', {
                    'symbol': self.symbol, 'orderId': o['orderId']
                })
                if r and r.status_code == 200:
                    cancelled += 1
                else:
                    print(f"  ⚠️ Échec cancel BUY {o['orderId']}")
            else:
                preserved += 1
        if cancelled or preserved:
            print(f"  ✅ {cancelled} BUY annulés | {preserved} SELL préservés")
        return True, preserved

    def _real_get_open_orders(self):
        """Récupère tous les ordres ouverts sur cette paire (None = erreur API)"""
        if self.dry_run:
            return []
        r = binance_signed_get('/api/v3/openOrders', {'symbol': self.symbol})
        if r and r.status_code == 200:
            return r.json()
        return None  # ⚠️ None = erreur, pas "pas d'ordres"

    def _real_check_balance(self, asset):
        """Vérifie le solde disponible d'un asset"""
        if self.dry_run:
            return None
        r = binance_signed_get('/api/v3/account')
        if r and r.status_code == 200:
            data = r.json()
            for b in data['balances']:
                if b['asset'] == asset:
                    return float(b['free']), float(b['locked'])
        return None, None

    def _real_sync_state_from_orders(self, s):
        """Met à jour l'état du bot à partir des ordres réels ouverts sur Binance"""
        if self.dry_run:
            return s
        open_orders = self._real_get_open_orders()
        if open_orders is None:
            print(f"  ⚠️ Sync: erreur API, état inchangé")
            return s
        lo = {}
        usdt_free = 0.0
        usdt_locked = 0.0
        tokens_free = 0.0
        tokens_locked = 0.0
        qty = s.get('qty', 0.1)

        for o in open_orders:
            side = o['side'].lower()
            price = float(o['price'])
            orig_qty = float(o['origQty'])
            executed_qty = float(o['executedQty'])
            status = o['status']

            p_str = str(price)
            if status == 'NEW' or status == 'PARTIALLY_FILLED':
                remaining = orig_qty - executed_qty
                if side == 'buy':
                    lo[p_str] = 'buy'
                    usdt_locked += price * remaining * 1.001
                else:
                    lo[p_str] = 'sell'
                    tokens_locked += remaining

        # ⚠️ NE PAS écraser les soldes avec le vrai solde Binance !
        # La balance réelle contient le capital de TOUS les bots ($500).
        # Chaque bot n'utilise que $50 → on garde les valeurs calculées par deploy().
        # On met à jour UNIQUEMENT level_orders depuis les ordres réels ouverts.
        # ⚠️ PRÉSERVER TOUJOURS les SELL simulés, NE JAMAIS les supprimer.
        # Raison: _real_cancel_all() pendant deploy() vide les ordres sur Binance,
        # et la sync (5s) ne doit PAS interpréter ça comme "SELL mort".
        # Si on supprime le SELL ici, level_orders devient vide → should_redeploy
        # retourne True → nouveau cancel + place → boucle infinie de redeploiements.
        # SEUL cas où un SELL disparaît : quand le bot le remplace volontairement
        # (via deploy_grid() qui écrase level_orders après avoir placé les nouveaux).
        old_lo = s.get('level_orders', {})
        is_frozen = s.get('frozen', False)
        # 🛡️ Flag _deploy_in_progress : protège la race condition pendant deploy()
        # (remplace l'ancienne fenêtre 120s basée sur deploy_time)
        deploy_in_progress = s.get('_deploy_in_progress', False)
        if deploy_in_progress:
            dp_since = s.get('_deploy_in_progress_since', 0)
            if time.time() - dp_since > 30:
                print(f"  ⚠️ Flag _deploy_in_progress expiré ({time.time()-dp_since:.0f}s) — reset")
                deploy_in_progress = False
        for p_str, otype in old_lo.items():
            if p_str not in lo:
                if otype == 'sell' and deploy_in_progress:
                    # 🛡️ Anti-corruption : préservé UNIQUEMENT si deploy en cours
                    # Raison : race condition deploy() cancel → place (2-5s window)
                    # La sync (5s) ne doit pas interpréter "SELL disparu" comme "SELL mort"
                    lo[p_str] = 'sell'
                elif otype == 'buy' and not is_frozen:
                    # BUY disparu de Binance + pas gelé → rempli → laisser recycle() traiter
                    # 🛡️ Flags récents fill : pour forcer sync balances avant daily loss
                    # (corrige race condition ADA 09/06 : fill non reflété dans tokens_free)
                    s['_recent_fill_ts'] = time.time()
                    s['_recent_fill_symbol'] = self.symbol
                    s['_recent_fill_side'] = 'buy'
                    s['_recent_fill_qty'] = float(p_str)  # le prix comme identifiant
                    lo[p_str] = 'buy'
                elif otype == 'sell' and not is_frozen:
                    # SELL disparu → rempli
                    s['_recent_fill_ts'] = time.time()
                    s['_recent_fill_symbol'] = self.symbol
                    s['_recent_fill_side'] = 'sell'
                    s['_recent_fill_qty'] = s.get('qty', 0)
                    # 💰 Valeur économique du fill (tokens_locked × prix SELL)
                    # Utilisée pour ajuster la PV du daily loss sans synchroniser USDT
                    # (corrige bug ADA 09/06 : fill SELL drop PV → faux STOP)
                    s['_recent_fill_value_usdt'] = round(
                        s.get('tokens_locked', 0) * float(p_str), 2
                    )
                    lo[p_str] = 'sell'
        # Si plus aucun ordre après sync, forcer redeploy
        s['level_orders'] = lo
        # 🧹 Nettoyer _pending_sell si le SELL correspondant existe déjà sur Binance
        # (couvre le crash après placement réussi mais avant del dans le retry)
        pending = s.get('_pending_sell')
        if pending:
            pending_key = (f"{float(pending['price'])}:{pending['qty']}:sell")
            actual_keys = set(
                f"{float(o['price'])}:{float(o['origQty'])}:{o['side'].lower()}"
                for o in open_orders if o
            )
            if pending_key in actual_keys:
                print(f"  🧹 _pending_sell nettoyé (SELL @ ${pending['price']:.4f} × {pending['qty']} déjà sur Binance)")
                del s['_pending_sell']

        # Recalculer usdt_locked / tokens_locked depuis les vrais ordres ouverts
        old_usdt_locked = s.get('usdt_locked', 0)
        s['usdt_locked'] = round(usdt_locked, 2)
        # ⚠️ FIX A: Si un BUY a été préservé (disparu de Binance, en attente recycle),
        #    NE PAS réinitialiser usdt_locked à 0 — cela créerait un faux daily loss.
        #    recycle() mettra à jour usdt_locked et tokens quand il traitera le fill.
        preserved_buys = sum(1 for v in lo.values() if v == 'buy')
        if preserved_buys > 0 and s['usdt_locked'] == 0.0 and old_usdt_locked > 0:
            s['usdt_locked'] = old_usdt_locked
        s['tokens_locked'] = float(f"{tokens_locked:.{s.get('_qty_dec', 1)}f}")

        # 🧹 Si des ordres réels existent sur Binance, le flag orphelin n est plus pertinent
        if lo and s.get('_orphan_rebuild_flag'):
            s['_orphan_rebuild_flag'] = False
            s.pop('_orphan_rebuild_time', None)
            print(f"  🧹 Flag orphelin nettoyé ({len(lo)} ordres réels détectés)")

        self.save_state(s)

        # 📊 Phase 1 — Validation vs Binance (lecture seule)
        if self.real_mode and not self.dry_run:
            self._validate_vs_binance(s, open_orders)

        return s

    def _real_sync_balances(self, s):
        """Sync les soldes réels du compte Binance avec l'état du bot.
        Lit le solde réel du token et ajuste tokens_free dans l'état.
        USDT n'est pas touché car partagé entre tous les bots.
        """
        if self.dry_run or not self.real_mode:
            return s
        try:
            r = binance_signed_get('/api/v3/account')
            if not r or r.status_code != 200:
                return s
            data = r.json()
            token_asset = self.symbol.replace('USDT', '')
            for b in data['balances']:
                if b['asset'] == token_asset:
                    # Sur Binance Spot, les SELL LIMIT ne lock PAS les tokens.
                    # free = total tokens disponibles (inclut ceux réservés par le state)
                    # locked dans l'état = tokens dédiés à des SELL simulés
                    # → tokens_free_réel = total_Binance - tokens_locked_state
                    total = float(b['free']) + float(b['locked'])
                    locked_in_state = s.get('tokens_locked', 0)
                    qty_dec = s.get('_qty_dec', 1)
                    old_free = s.get('tokens_free', 0)
                    new_free = max(0, total - locked_in_state)
                    s['tokens_free'] = float(f"{new_free:.{qty_dec}f}")
                    if abs(old_free - new_free) > 0.001:
                        print(f"  🔄 Balance sync: {token_asset}: {old_free:.4f} → {new_free:.4f}")
                    # 🧹 Nettoyer les flags _recent_fill (plus besoin, state à jour)
                    if '_recent_fill_ts' in s:
                        del s['_recent_fill_ts']
                        for k in ['_recent_fill_symbol', '_recent_fill_side', '_recent_fill_qty', '_recent_fill_value_usdt']:
                            s.pop(k, None)
                        print(f"  🧹 Flags _recent_fill nettoyés (state synchronisé)")
                    break
            self.save_state(s)
        except Exception as e:
            print(f"  ⚠️ Balance sync error: {e}")
        return s

    # ═══════════════════════════════════════════════════════════
    # 📊 Phase 1 — Validation vs Binance (lecture seule)
    # ═══════════════════════════════════════════════════════════

    _DIVERGENCE_LOG_ENABLED = True  # Flag global pour désactiver rapidement

    def _init_divergence_log(self):
        """Charge le fichier de divergence, crée la structure si absent ou corrompu."""
        import os
        if not hasattr(self, 'divergence_file') or not self.divergence_file:
            return None
        if os.path.exists(self.divergence_file):
            try:
                with open(self.divergence_file) as f:
                    data = json.load(f)
                if 'stats' in data and 'history' in data:
                    return data
            except Exception:
                print(f"  ⚠️ Divergence log corrompu, recréation")
        return {
            "meta": {
                "symbol": self.symbol,
                "name": self.name,
                "started": time.time(),
                "bot_version": "bot_trader.py"
            },
            "stats": {
                "total_checks": 0,
                "total_divergences": 0,
                "unexplained_divergences": 0,
                "explainable_divergences": 0,
                "max_severity": "none",
                "max_amplitude_locked": 0.0,
                "max_amplitude_tokens": 0.0,
                "max_amplitude_orders": 0,
                "current_unresolved": 0,
                "last_divergence_ts": 0,
                "last_resolved_ts": 0,
                "checks_since_last_div": 0,
                "fields_affected": {}
            },
            "history": []
        }

    def _write_divergence_log(self, data):
        """Écrit le fichier de divergence atomiquement."""
        try:
            with open(self.divergence_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"  ⚠️ Divergence log write error: {e}")

    def _classify_divergence(self, field, dtype, severity, context):
        """Classifie une divergence : explainable vs unexplained + impact capital."""
        # Explainable = attendue dans l'architecture actuelle
        if severity == 'explainable':
            return 'explainable', 'none'

        # Critical = peut provoquer : SELL manquant, double ordre, faux capital, faux daily loss, exposition
        critical_patterns = [
            # SELL manquant
            lambda: dtype == 'missing_in_state' and field == 'level_orders' and context.get('binance_side') == 'sell',
            # Side mismatch
            lambda: dtype == 'side_mismatch',
            # Extra SELL que Binance ne confirme pas → exposition non prévue
            lambda: field == 'level_orders' and dtype == 'extra_in_state' and context.get('state_side') == 'sell',
            # Usdt_locked à 0 alors qu'attendu → faux daily loss
            lambda: field == 'usdt_locked' and dtype == 'value_mismatch' and context.get('state_val', 1) > 0 and context.get('binance_val', 0) == 0,
        ]

        for check in critical_patterns:
            if check():
                return 'unexplained', 'critical'

        # High amplitude
        if context.get('amplitude', 0) > 50 or context.get('pct', 0) > 50:
            return 'unexplained', 'high'

        if context.get('amplitude', 0) > 10:
            return 'unexplained', 'medium'

        return 'unexplained', 'low'

    def _validate_vs_binance(self, s, raw_orders):
        """📊 Phase 1 — Compare état local vs vérité Binance. Lecture seule.

        Mesure les divergences pour déterminer objectivement si la Sync Inverse
        est nécessaire. NE MODIFIE RIEN. Zéro impact sur le comportement.
        """
        if not self._DIVERGENCE_LOG_ENABLED:
            return

        # Flags de contexte
        is_deploy = s.get('_deploy_in_progress', False)
        dp_since = s.get('_deploy_in_progress_since', 0)
        if is_deploy and dp_since > 0 and time.time() - dp_since > 30:
            is_deploy = False
        is_frozen = s.get('frozen', False)

        # --- 1. Reconstruire état "pur Binance" (sans merge, sans préservation) ---
        binance_lo = {}
        binance_usdt_locked = 0.0
        binance_tokens_locked = 0.0
        binance_qty = s.get('qty', 0)

        for o in (raw_orders or []):
            side = o['side'].lower()
            price = float(o['price'])
            orig_qty = float(o['origQty'])
            executed_qty = float(o['executedQty'])
            status = o['status']

            p_str = str(price)
            if status in ('NEW', 'PARTIALLY_FILLED'):
                remaining = orig_qty - executed_qty
                binance_lo[p_str] = side
                if side == 'buy':
                    binance_usdt_locked += price * remaining * 1.001
                else:
                    binance_tokens_locked += remaining
                if remaining > 0:
                    binance_qty = remaining

        # --- 2. Comparer champ par champ ---
        state_lo = s.get('level_orders', {})
        divergences = []
        qty_dec = s.get('_qty_dec', 1)

        # Niveaux : Binance a qchose que state n'a pas
        for k, v in binance_lo.items():
            if k not in state_lo:
                divergences.append({
                    'field': 'level_orders',
                    'type': 'missing_in_state',
                    'detail': f"@{k}/{v}",
                    'binance_side': v,
                    'state_val': 0,
                    'binance_val': 1,
                    'amplitude': 1
                })

        # Niveaux : state a qchose que Binance n'a pas
        for k, v in state_lo.items():
            if k not in binance_lo:
                divergences.append({
                    'field': 'level_orders',
                    'type': 'extra_in_state',
                    'detail': f"@{k}/{v}",
                    'state_side': v,
                    'state_val': 1,
                    'binance_val': 0,
                    'amplitude': 1
                })

        # Niveaux : même prix, side différent
        for k in state_lo:
            if k in binance_lo and state_lo[k] != binance_lo[k]:
                divergences.append({
                    'field': 'level_orders',
                    'type': 'side_mismatch',
                    'detail': f"S:{state_lo[k]} B:{binance_lo[k]} @{k}",
                    'state_val': 1,
                    'binance_val': 1,
                    'amplitude': 1
                })

        # usdt_locked
        state_usdt = s.get('usdt_locked', 0)
        binance_usdt = round(binance_usdt_locked, 2)
        usdt_diff = abs(state_usdt - binance_usdt)
        if usdt_diff > 1.0:
            divergences.append({
                'field': 'usdt_locked',
                'type': 'value_mismatch',
                'detail': f"$S:{state_usdt:.2f} $B:{binance_usdt:.2f}",
                'severity_hint': 'explainable' if is_deploy or is_frozen else None,
                'state_val': state_usdt,
                'binance_val': binance_usdt,
                'amplitude': usdt_diff
            })

        # tokens_locked
        state_tok = s.get('tokens_locked', 0)
        binance_tok = round(binance_tokens_locked, qty_dec)
        tok_diff = abs(state_tok - binance_tok)
        if tok_diff > 0:
            divergences.append({
                'field': 'tokens_locked',
                'type': 'value_mismatch',
                'detail': f"T:S:{state_tok} T:B:{binance_tok}",
                'severity_hint': 'explainable' if is_deploy or is_frozen else None,
                'state_val': state_tok,
                'binance_val': binance_tok,
                'amplitude': tok_diff
            })

        # qty
        state_qty = s.get('qty', 0)
        if state_qty > 0 and binance_qty > 0:
            qty_diff_pct = abs(state_qty - binance_qty) / state_qty * 100
            if qty_diff_pct > 1.0:
                divergences.append({
                    'field': 'qty',
                    'type': 'value_mismatch',
                    'detail': f"S:{state_qty} B:{binance_qty} ({qty_diff_pct:.1f}%)",
                    'state_val': state_qty,
                    'binance_val': binance_qty,
                    'amplitude': round(qty_diff_pct, 1),
                    'pct': qty_diff_pct
                })

        # --- 3. Classifier et logger ---
        # Charger l'historique
        log = self._init_divergence_log()
        if log is None:
            return

        log['stats']['total_checks'] += 1

        if divergences:
            n_explainable = 0
            n_unexplained = 0
            max_severity = 'none'
            max_amp = 0
            max_sev_val = 0
            SEV_ORDER = {'none': 0, 'low': 1, 'medium': 2, 'high': 3, 'critical': 4}

            entry_divs = []
            for d in divergences:
                # Classifier
                context = {
                    'binance_side': d.get('binance_side'),
                    'state_side': d.get('state_side'),
                    'state_val': d.get('state_val', 0),
                    'binance_val': d.get('binance_val', 0),
                    'amplitude': d.get('amplitude', 0),
                    'pct': d.get('pct', 0)
                }
                cls, imp = self._classify_divergence(
                    d['field'], d['type'],
                    d.get('severity_hint', 'unknown'),
                    context
                )
                entry_d = {
                    'field': d['field'],
                    'type': d['type'],
                    'detail': d['detail'],
                    'classification': cls,
                    'impact': imp,
                    'amplitude': d.get('amplitude', 0)
                }
                entry_divs.append(entry_d)

                if cls == 'explainable':
                    n_explainable += 1
                else:
                    n_unexplained += 1
                    sev = SEV_ORDER.get(imp, 0)
                    if sev > max_sev_val:
                        max_sev_val = sev
                        max_severity = imp
                    max_amp = max(max_amp, d.get('amplitude', 0))

                # Stats par champ
                if d['field'] not in log['stats']['fields_affected']:
                    log['stats']['fields_affected'][d['field']] = {
                        'total': 0, 'explainable': 0, 'unexplained': 0, 'types': {}
                    }
                fstat = log['stats']['fields_affected'][d['field']]
                fstat['total'] += 1
                fstat[cls] += 1
                if d['type'] not in fstat['types']:
                    fstat['types'][d['type']] = 0
                fstat['types'][d['type']] += 1

            # Mettre à jour les stats cumulatives
            log['stats']['total_divergences'] += len(divergences)
            log['stats']['unexplained_divergences'] += n_unexplained
            log['stats']['explainable_divergences'] += n_explainable
            log['stats']['current_unresolved'] += 1
            log['stats']['last_divergence_ts'] = time.time()
            log['stats']['checks_since_last_div'] = 0
            log['stats']['max_amplitude_locked'] = max(
                log['stats']['max_amplitude_locked'],
                max((d.get('amplitude', 0) for d in divergences if d['field'] == 'usdt_locked'), default=0)
            )
            log['stats']['max_amplitude_tokens'] = max(
                log['stats']['max_amplitude_tokens'],
                max((d.get('amplitude', 0) for d in divergences if d['field'] == 'tokens_locked'), default=0)
            )
            log['stats']['max_amplitude_orders'] = max(
                log['stats']['max_amplitude_orders'],
                max((d.get('amplitude', 0) for d in divergences if d['field'] == 'level_orders'), default=0)
            )
            if SEV_ORDER.get(max_severity, 0) > SEV_ORDER.get(log['stats']['max_severity'], 0):
                log['stats']['max_severity'] = max_severity

            # Ajouter l'entrée d'historique (garder max 500)
            entry = {
                'ts': time.time(),
                'n_divergences': len(divergences),
                'n_explainable': n_explainable,
                'n_unexplained': n_unexplained,
                'max_severity': max_severity,
                'context': {
                    'deploy_in_progress': is_deploy,
                    'frozen': is_frozen
                },
                'divergences': entry_divs
            }
            log['history'].append(entry)
            if len(log['history']) > 500:
                log['history'] = log['history'][-500:]

            # Print résumé console (1/sync max)
            print(f"  📊 Validation: {n_explainable}E+{n_unexplained}U div "
                  f"(max: {max_severity}, amp:{max_amp})")

        else:
            # Aucune divergence → résolution si précédemment non résolu
            if log['stats']['current_unresolved'] > 0:
                log['stats']['current_unresolved'] = 0
                log['stats']['last_resolved_ts'] = time.time()
                # Print toutes les 60 checks sans div
            log['stats']['checks_since_last_div'] += 1
            if log['stats']['checks_since_last_div'] % 60 == 0:
                print(f"  📊 Validation: {log['stats']['total_checks']} checks, "
                      f"{log['stats']['total_divergences']} div "
                      f"({log['stats']['explainable_divergences']}E+{log['stats']['unexplained_divergences']}U) "
                      f"— clean streak: {log['stats']['checks_since_last_div']}")

        # Écrire (atomique — fichier JSON entier)
        self._write_divergence_log(log)

    # ── Logique métier ──────────────────────────────────────

    def _get_imbalance(self):
        """Récupère l'imbalance du carnet d'ordres via REST (toujours).
        
        ⚠️ N'utilise PAS le spread du ticker WebSocket car spread ≠ imbalance.
        spread = (ask-bid)/bid (ex: 0.05%), imbalance = (ask_vol-bid_vol)/total (ex: -1 à +1).
        Le seuil GRID_ASYMMETRY_THRESHOLD=0.20 n'est jamais atteint par le spread.
        """
        if not GRID_ASYMMETRY_ENABLED:
            return 0.0
        ob = get_order_book(self.symbol, limit=10)
        if ob:
            analysis = analyze_spread(ob['bids'], ob['asks'])
            if analysis:
                return analysis['imbalance']
        return 0.0

    def build_grid(self, price, n_levels=None, imbalance=0.0, grid_pct=None):
        if n_levels is None: n_levels = GRID_LEVELS
        if grid_pct is None: grid_pct = GRID_PCT
        half = price * grid_pct / 2
        lo = fmt_price(price - half, self.tick_size)
        hi = fmt_price(price + half, self.tick_size)
        if hi <= lo: lo, hi = price * 0.97, price * 1.03

        # 📊 Grid asymmetry: décaler le centre selon l'imbalance du carnet d'ordres
        if GRID_ASYMMETRY_ENABLED and abs(imbalance) >= GRID_ASYMMETRY_THRESHOLD:
            shift_ratio = min(abs(imbalance), 1.0) * GRID_ASYMMETRY_MAX_SHIFT
            if imbalance > 0:
                # Pression vendeuse → + de BUY levels (descendre la grille)
                shift_amount = half * shift_ratio
                lo = fmt_price(lo - shift_amount, self.tick_size)
                hi = fmt_price(hi - shift_amount * 0.3, self.tick_size)  # Rétrécir un peu le haut
                if hi <= lo: lo, hi = price * 0.96, price * 1.02
                print(f"  📊 Asymétrie SELL (imbalance={imbalance:+.3f}) → grille décalée bas de {shift_ratio*100:.0f}%")
            else:
                # Pression acheteuse → + de SELL levels (monter la grille)
                shift_amount = half * shift_ratio
                hi = fmt_price(hi + shift_amount, self.tick_size)
                lo = fmt_price(lo + shift_amount * 0.3, self.tick_size)
                if hi <= lo: lo, hi = price * 0.98, price * 1.04
                print(f"  📊 Asymétrie BUY (imbalance={imbalance:+.3f}) → grille décalée haut de {shift_ratio*100:.0f}%")

        step = (hi - lo) / n_levels
        return sorted(set(fmt_price(lo + (i + 0.5) * step, self.tick_size) for i in range(n_levels)))

    def deploy(self, price, capital=None, prev_cycles=0, prev_profit=0.0):
        if capital is None: capital = self.capital

        # 📐 AVS — QuantEngine: fair value + dynamic GRID_PCT
        # Estimer l'inventaire: tokens_locked + tokens_free approx
        inventory_est = self._state.get('tokens_free', 0.0) + self._state.get('tokens_locked', 0.0) if hasattr(self, '_state') else 0.0
        # Inventaire max: capital / price (≈capital total en tokens)
        max_inv = capital / price if price > 0 else 1.0

        quant = self._quant.tick(price, inventory=inventory_est,
                                 max_inventory=max_inv)
        fair_value = quant['center_price'] if AVS_ENABLED else price
        grid_pct_dynamic = quant['grid_pct'] if AVS_ENABLED else GRID_PCT

        # 📡 NF — Ajuster le spread de grille selon le sentiment news
        if NF_ENABLED and NF_GRID_ADJUST_ENABLED and self._nf:
            nf_mult = self._nf.grid_multiplier()
            if nf_mult != 1.0:
                old_pct = grid_pct_dynamic
                grid_pct_dynamic = grid_pct_dynamic * nf_mult
                # Garder dans les limites AVS (depuis QuantEngine)
                if AVS_ENABLED:
                    avs_min = getattr(self._quant, 'AVS_GRID_PCT_MIN', 0.01)
                    avs_max = getattr(self._quant, 'AVS_GRID_PCT_MAX', 0.15)
                    grid_pct_dynamic = min(max(grid_pct_dynamic, avs_min), avs_max)
                else:
                    grid_pct_dynamic = min(max(grid_pct_dynamic, 0.01), 0.20)
                print(f"  📡 NF: spread {old_pct*100:.2f}%→{grid_pct_dynamic*100:.2f}% (sentiment ×{nf_mult:.2f})")

        if AVS_ENABLED and quant['enabled']:
            print(f"  📐 AVS: FV={quant['fair_value']:.2f} σ={quant['sigma']:.4f} "
                  f"OFI={quant['imbalance']:+.4f} grid_pct={grid_pct_dynamic*100:.2f}%")

        # 📊 SR — Ajuster le fair value selon le support le plus proche
        if SR_ENABLED and SR_GRID_ALIGN_SUPPORT and self._sr:
            sr_result = self._sr.set_price(self.symbol, price)
            if sr_result and sr_result['nearest_support'] > 0:
                ns = sr_result['nearest_support']
                prox = sr_result['support_proximity_pct']
                if prox <= 0.02:  # Dans les 2% du support
                    # Centrer la grille entre le support et le prix actuel
                    grid_center = (price + ns) / 2
                    old_fv = fair_value
                    fair_value = grid_center
                    print(f"  📊 SR: grille centrée ${ns:.2f}→${price:.2f} "
                          f"(FV ${old_fv:.2f} → ${fair_value:.2f})")

        # 📊 Obtenir l'imbalance du carnet d'ordres pour asymétrie de grille
        imbalance = self._get_imbalance()

        # Construire la grille asymétrique avec le GRID_PCT dynamique AVS
        levels = self.build_grid(fair_value, imbalance=imbalance, grid_pct=grid_pct_dynamic)
        buy_levels = [p for p in levels if p < fair_value]
        sell_levels = [p for p in levels if p > fair_value]

        # 📐 MIN_NOTIONAL = base de tous les calculs (mode réel)
        # Chaque ordre vaut exactement $MN → qty = MN / price
        if self.real_mode:
            min_notional = getattr(self, 'min_notional', 5.0)
            center_price = levels[len(levels)//2]
            step = self.lot_step
            qty = float((Decimal(str(PER_ORDER_VALUE)) / Decimal(str(center_price)) /
                         Decimal(str(step))).to_integral_value(rounding=ROUND_UP) *
                        Decimal(str(step)))
            qty = fmt_qty(qty, step)
            # 🔧 Capper qty au capital disponible (INITIAL_POS_PCT détermine l'allocation BUY)
            max_buy_qty = fmt_qty(capital * INITIAL_POS_PCT / center_price, step)
            if qty > max_buy_qty and max_buy_qty * center_price >= min_notional:
                print(f"  ⚠️ Qty réduite: {qty} → {max_buy_qty} ($ {PER_ORDER_VALUE:.0f}→$ {max_buy_qty*center_price:.0f}/ordre, capital ${capital:.2f})")
                qty = max_buy_qty
            val_per_order = qty * center_price

            # Nombre de niveaux max selon le capital disponible
            max_alloc = capital * CAPITAL_ALLOC
            max_levels = int(max_alloc / PER_ORDER_VALUE)
            actual_levels = min(max_levels, len(levels))
            if actual_levels < 2:
                actual_levels = max_levels  # accepter 1 niveau si capital limité
            if actual_levels < GRID_LEVELS:
                if actual_levels < 1:
                    # Même 1 ordre ne tient pas → skip
                    print(f"  ⚠️ Capital insuffisant: ${max_alloc:.2f} < ${PER_ORDER_VALUE:.0f} min — skip")
                    return None
                levels = self.build_grid(fair_value, n_levels=actual_levels,
                                          imbalance=imbalance, grid_pct=grid_pct_dynamic)
                buy_levels = [p for p in levels if p < fair_value]
                sell_levels = [p for p in levels if p > fair_value]
                print(f"  ⚠️ Niveaux: {GRID_LEVELS}→{actual_levels} (${PER_ORDER_VALUE:.0f}/ordre, ${max_alloc:.0f} dispo)")

        # Recalculer usdt et tokens avec les vrais niveaux
        # ⚠️ Multi-bot: NE PAS utiliser le solde réel Binance (partagé entre tous les bots)
        # → toujours utiliser le calcul théorique basé sur capital et INITIAL_POS_PCT
        if self.real_mode:
            usdt = capital  # capital total (sera réduit par les BUY placés)
            tokens = self._state.get('tokens_free', 0.0) if hasattr(self, '_state') and self._state else 0.0
            if tokens == 0.0:
                tokens = fmt_qty(capital * INITIAL_POS_PCT / price, self.lot_step)
        else:
            usdt = capital * (1 - INITIAL_POS_PCT)
            tokens = fmt_qty(capital * INITIAL_POS_PCT / price, self.lot_step)
            # Fallback qty pour mode paper (pas dans le bloc real_mode)
            qty = fmt_qty(PER_ORDER_VALUE / price, self.lot_step)

        # 📐 EMA Dynamic Buy Zones — ajuster la qty selon la distance à l'EMA
        ema_data = get_ema(self.symbol, EMA_PERIOD) if EMA_ENABLED else None
        ema_block_buys = False
        ema_block_sells = False
        if ema_data:
            ema_val, _ = ema_data
            mult, zone = get_ema_multiplier(ema_val, price)
            if mult <= 0:
                if zone == 'expensive':
                    ema_block_buys = True
                    print(f"  📐 Zone {zone.upper()} — BUY bloqués (prix trop haut), SELL autorisés")
                elif zone == 'hard_stop':
                    ema_block_sells = True
                    print(f"  📐 Zone {zone.upper()} — SELL bloqués (prix trop bas), BUY autorisés")
                else:
                    print(f"  📐 Zone {zone.upper()} (mult={mult}) — PAS DE DÉPLOIEMENT")
                    return None

        # 🚦 MG — Market Gate: bloquer les BUY si score ≥ 6
        if MG_ENABLED and MG_BLOCK_BUYS and self._mg_result and not self._mg_result['can_buy']:
            mg = self._mg_result
            print(f"  🚦 MG: BUY bloqués (score {mg['score']}) — déploiement BUY-only annulé")
            return None

        # 📡 NF — NewsFeed: bloquer les BUY si sentiment très négatif + breaking news
        if NF_ENABLED and NF_BLOCK_BUYS and self._nf_result and self._nf.should_block_buys(
            sentiment_threshold=NF_SENTIMENT_THRESHOLD,
            breaking_critical=True,
        ):
            print(f"  📡 NF: BUY bloqués (sentiment {self._nf_result['sentiment']:+.2f}, "
                  f"breaking={len(self._nf_result['breaking'])})")
            return None

        # Suite de la zone EMA (ajustement du nombre de niveaux, PAS de la qty)
        if ema_data:
            ema_val, _ = ema_data
            mult, zone = get_ema_multiplier(ema_val, price)
            if mult > 0 and mult != 1.0 and self.real_mode:
                # Avec MN comme base, l'EMA ajuste le nombre de niveaux, pas la qty
                # (la qty reste à $5/price pour chaque ordre)
                # actual_levels défini dans le bloc self.real_mode (671-688)
                old_n = actual_levels if self.real_mode else len(levels)
                new_n = max(2, int(old_n * mult))
                if new_n < old_n:
                    levels = self.build_grid(fair_value, n_levels=new_n,
                                              imbalance=imbalance, grid_pct=grid_pct_dynamic)
                    buy_levels = [p for p in levels if p < fair_value]
                    sell_levels = [p for p in levels if p > fair_value]
                    print(f"  📐 Zone {zone.upper()} (dist={(price-ema_val)/ema_val*100:+.1f}%) → niveaux {old_n} → {new_n} (×{mult})")
            elif mult > 0 and mult != 1.0 and not self.real_mode:
                # Mode paper: ajustement qty (ancien comportement)
                qty_old = qty
                qty = fmt_qty(qty * mult, self.lot_step)
                if qty > qty_old:
                    extra_tokens = fmt_qty((qty - qty_old) * len(sell_levels), self.lot_step)
                    if tokens >= extra_tokens:
                        tokens -= extra_tokens
                    else:
                        tokens = 0.0
                print(f"  📐 Zone {zone.upper()} (dist={(price-ema_val)/ema_val*100:+.1f}%) → qty {qty_old} → {qty} (×{mult})")
            zone = zone if mult > 0 else "no_ema"
        else:
            zone = "no_ema"

        # 🔄 Rebalancing — réduire le capital des paires défaillantes
        if REBALANCE_ENABLED:
            try:
                perf_data = perf_tracker._load_perf()
                bot_data = perf_data.get("bots", {}).get(self.name, {})
                perf_cycles = bot_data.get("cycles", 0)
                if perf_cycles >= REBALANCE_MIN_CYCLES:
                    perf_wins = bot_data.get("wins", 0)
                    perf_wr = perf_wins / perf_cycles if perf_cycles > 0 else 0.0
                    perf_pnl = bot_data.get("total_profit", 0.0)
                    if perf_wr < REBALANCE_WR_THRESHOLD and perf_pnl < 0:
                        # Paire défaillante → réduire capital au minimum
                        old_cap = capital
                        capital = min(capital, REBALANCE_CAPITAL_FLOOR)
                        if capital < old_cap:
                            print(f"  🔄 Rebalance: {self.name} WR={perf_wr:.0%} PnL=${perf_pnl:.2f} → "
                                  f"capital réduit: ${old_cap:.0f} → ${capital:.0f}")
                            self.tg_send(
                                f"🔄 Rebalance: *{self.name}*\n"
                                f"WR {perf_wr:.0%}, PnL ${perf_pnl:.2f}\n"
                                f"Capital: ${old_cap:.0f} → ${capital:.0f}"
                            )
                            # Recalculer usdt, tokens avec le nouveau capital
                            usdt = capital * (1 - INITIAL_POS_PCT)
                            if self.real_mode:
                                try:
                                    r_usdt, _ = self._real_check_balance('USDT')
                                    if r_usdt is not None and r_usdt >= 0:
                                        usdt = r_usdt
                                except:
                                    pass
                            tokens = fmt_qty(capital * INITIAL_POS_PCT / price, self.lot_step)
            except Exception as e:
                print(f"  ⚠️ Rebalance check error: {e}")

        # ── Mode réel : vérifier que les ordres passent MIN_NOTIONAL ──
        if self.real_mode:
            min_notional = getattr(self, 'min_notional', 5.0)
            actual_val = qty * levels[len(levels)//2]
            if actual_val < min_notional:
                print(f"  ⚠️ Ordre sous MIN_NOTIONAL: ${actual_val:.2f} — skip")
                # Sera rattrapé par le deadlock check plus bas

        # ── Mode réel : BUY impossibles si USDT insuffisant ──
        if self.real_mode and buy_levels:
            cost_first_buy = buy_levels[0] * qty * 1.0015
            if usdt < cost_first_buy:
                # On ne crée PAS d'USDT fictif — on skip les BUY
                # Les SELL remplis apporteront l'USDT pour les BUY plus tard
                print(f"  ⚠️ USDT insuffisant: ${usdt:.2f} < ${cost_first_buy:.2f} — BUY skippés")

        level_orders = {}
        # En mode réel avec BUY bloqués, capper qty aux tokens dispo
        # pour éviter de bloquer avec une qty MIN_NOTIONAL trop grosse
        min_notional = getattr(self, 'min_notional', 5.0)
        if self.real_mode and ema_block_buys and sell_levels:
            max_sell_qty = fmt_qty(tokens / len(sell_levels), self.lot_step)
            max_sell_val = max_sell_qty * sell_levels[0]
            if max_sell_val < min_notional:
                # Deadlock : tokens insuffisants pour MIN_NOTIONAL
                # On skip — le bot attendra que le prix baisse pour racheter
                pass
            elif max_sell_qty < qty and max_sell_qty >= self.lot_step:
                old_qty = qty
                qty = max_sell_qty
                print(f"  ⚠️ Qty réduite: {old_qty} → {qty} (tokens dispo: {tokens:.1f})")
        for l in buy_levels:
            if ema_block_buys:
                continue  # Zone chère → pas de nouveaux BUY
            cost = l * qty * 1.0015
            if usdt >= cost: usdt -= cost; level_orders[str(l)] = 'buy'
            elif usdt >= l * self.lot_step * 1.0015:
                # 🔧 USDT insuffisant pour qty complète → réduire la qty
                max_qty = fmt_qty(usdt / (l * 1.0015), self.lot_step)
                if max_qty * l >= self.min_notional:
                    print(f"  ⚠️ Qty BUY réduite: {qty} → {max_qty} (USDT=${usdt:.2f})")
                    cost = l * max_qty * 1.0015
                    usdt -= cost; level_orders[str(l)] = 'buy'
                    # Propager la nouvelle qty pour les SELL
                    qty = max_qty
        _sell_qtys = {}  # Collecteur des quantites SELL ajustees
        for l in sell_levels:
            if ema_block_sells:
                continue
            # ── Mode reel : verifier les tokens reels avant de placer un SELL
            # (pas de SELL fictif — on ne peut vendre que ce qu'on a)
            has_tokens = tokens
            if self.real_mode:
                try:
                    r_tok, _ = self._real_check_balance(self.symbol.replace('USDT', ''))
                    if r_tok is not None: has_tokens = r_tok
                except:
                    pass
            sell_qty = qty
            qty_dec_val = max(0, -int(Decimal(str(self.lot_step)).as_tuple().exponent))
            # Si on a des tokens mais moins que la qty standard, vendre tout ce qu'on a
            if has_tokens < sell_qty and has_tokens >= self.lot_step and has_tokens * float(l) >= min_notional:
                sell_qty = float(Decimal(str(has_tokens)).quantize(Decimal(str(self.lot_step)), rounding=ROUND_DOWN))
                print(f"  ⚠️ Qty SELL réduite: {qty} → {sell_qty} (tokens dispos: {has_tokens:.{qty_dec_val}f})")
            # 📦 Sauvegarder la qty réelle du SELL pour la réutiliser dans le placement
            _sell_qtys[str(l)] = sell_qty
            if has_tokens >= sell_qty:
                tokens -= sell_qty
                level_orders[str(l)] = 'sell'
                if sell_qty != qty:
                    # Propager la qty réduite aux BUY pour garder l'équilibre
                    pass  # Les BUY restent à qty standard — le prochain cycle équilibrera
        buy_locked = sum(float(l) * qty * 1.0015 for ls, v in level_orders.items() if v == 'buy' for l in [float(ls)])
        sell_locked = sum(1 for v in level_orders.values() if v == 'sell') * qty
        qty_dec = max(0, -int(Decimal(str(self.lot_step)).as_tuple().exponent))

        # 📊 Fills file conservé — ne plus effacer au redéploiement
        # L'historique des cycles persiste pour le dashboard

        state = {
            'deploy_time': time.time(), 'deploy_price': price, 'entry_price': price,
            'levels': [float(l) for l in levels], 'level_orders': level_orders,
            'qty': float(qty), 'total_cycles': 0, 'total_profit_est': 0.0,
            'usdt_free': float(f"{usdt:.2f}"), 'usdt_locked': float(f"{buy_locked:.2f}"),
            'tokens_free': float(f"{tokens:.{qty_dec}f}"), 'tokens_locked': float(f"{sell_locked:.{qty_dec}f}"),
            '_qty_dec': qty_dec,
            'day_date': date.today().isoformat(), 'day_start_value': capital,
            'daily_loss': 0.0, 'capital': capital, 'initial_capital': self.capital, 'phase2_start': capital,
            'frozen': False, 'peak_price': price, 'peak_time': time.time(),
            'freeze_price': 0.0, 'freeze_time': 0, 'pending_cycles': 0, 'drop_level': 0,
            'real_mode': self.real_mode,  # Indicateur réel/paper pour le dashboard
            '_deploy_in_progress': False,
            '_deploy_in_progress_since': 0,
        }

        # ── Aucun ordre à placer ? Skip le déploiement ──
        if self.real_mode and not level_orders:
            skip_until = time.time() + 1800  # 30 min de cooldown (deadlock MIN_NOTIONAL)
            # Préserver le state existant — lire depuis le disque, ajouter _skip_until
            disk_s = self.load_state()
            if disk_s:
                disk_s['_skip_until'] = skip_until
                disk_s['frozen'] = False  # s'assurer que le bot n'est pas gelé
                self.save_state(disk_s)
            else:
                state['_skip_until'] = skip_until
                self.save_state(state)
            print(f"  🌐 Déploiement RÉEL — 0 ordres possibles (skip, prochain essai dans 30min)")
            return None

        # ── Mode réel : placer les ordres sur Binance ──
        if self.real_mode:
            # 📦 Checkpoint avant déploiement
            self._checkpoint("pre-deploy")

            # ⚡ Signaler le début du déploiement (protection anti-corruption sync)
            state['_deploy_in_progress'] = True
            state['_deploy_in_progress_since'] = time.time()
            self.save_state(state)

            # 🔬 Stratégie chirurgicale : ne toucher QUE les ordres qui ont changé
            # Au lieu de cancel_all + replace (qui crée une race condition avec la sync)
            # → comparer ce qui existe déjà sur Binance, ajuster à la marge
            existing_orders = self._real_get_open_orders() or []
            existing_by_key = {}
            for o in existing_orders:
                key = f"{float(o['price'])}:{o['side'].lower()}"
                existing_by_key[key] = o['orderId']

            needed_keys = set(f"{float(p)}:{t}" for p, t in level_orders.items())
            existing_keys = set(existing_by_key.keys())

            keys_to_cancel_raw = existing_keys - needed_keys
            keys_to_place = needed_keys - existing_keys

            # Règle SPOT: ne jamais annuler un SELL sans autorisation explicite.
            # Si un redéploiement veut supprimer un SELL existant, on le préserve.
            sell_keys_preserved = {k for k in keys_to_cancel_raw if k.endswith(':sell')}
            keys_to_cancel = keys_to_cancel_raw - sell_keys_preserved
            if sell_keys_preserved:
                print(f"  🛡️ {len(sell_keys_preserved)} SELL préservé(s) — non annulé(s) sans autorisation")
                # Retirer aussi le placement du SELL déjà présent sur Binance
                keys_to_place -= sell_keys_preserved

            if keys_to_cancel or keys_to_place:
                print(f"  🌐 Déploiement RÉEL — {len(keys_to_cancel)} annulations, {len(keys_to_place)} placements")

            # Annuler UNIQUEMENT les ordres BUY qui ne sont plus nécessaires
            cancelled_ids = set()
            for key in keys_to_cancel:
                oid = existing_by_key[key]
                r = binance_signed_delete('/api/v3/order', {'symbol': self.symbol, 'orderId': oid})
                if r and r.status_code == 200:
                    cancelled_ids.add(oid)
                    print(f"  ✅ Cancel ordre {oid}")
                else:
                    print(f"  ⚠️ Échec cancel ordre {oid}")

            # Placer UNIQUEMENT les nouveaux ordres nécessaires
            placed_orders = {}
            for p_str, otype in list(level_orders.items()):
                key = f"{float(p_str)}:{otype}"
                if key not in existing_keys or existing_by_key.get(key) in cancelled_ids:
                    p = float(p_str)
                    if otype == 'sell':
                        actual_qty = _sell_qtys.get(p_str, qty)
                    else:
                        actual_qty = qty
                    result = self._real_place_order(otype, actual_qty, p)
                    if (self.dry_run) or (result and result.get('status') not in ('ERROR', None)):
                        placed_orders[p_str] = otype
                    else:
                        # Si l'ordre a échoué, libérer les fonds/tokens réservés
                        if otype == 'buy':
                            buy_locked -= p * qty * 1.0015
                            try:
                                r_usdt, _ = self._real_check_balance('USDT')
                                usdt = r_usdt if r_usdt is not None else usdt
                            except:
                                pass
                        else:  # sell
                            sell_locked = max(0, sell_locked - qty)
                            tokens += qty
                        del level_orders[p_str]
                else:
                    # Ordre déjà existant sur Binance → le garder
                    placed_orders[p_str] = otype
            state['level_orders'] = placed_orders
            state['usdt_free'] = float(f"{usdt:.2f}")
            state['usdt_locked'] = float(f"{buy_locked:.2f}")
            state['tokens_free'] = float(f"{tokens:.{qty_dec}f}")
            state['tokens_locked'] = float(f"{sell_locked:.{qty_dec}f}")

            # ✅ Fin du déploiement — le flag est sauvegardé par le save_state() en fin de fonction
            state['_deploy_in_progress'] = False
            state['_deploy_in_progress_since'] = 0

        self.save_state(state)
        return state

    def portfolio_value(self, s, price):
        free = s.get('usdt_free', 0) + s.get('tokens_free', 0) * price
        locked = s.get('usdt_locked', 0) + s.get('tokens_locked', 0) * price
        return free + locked

    def recycle(self, s, price):
        levels = s.get('levels', [])
        qty = s.get('qty', 0.1)
        lo = s.get('level_orders', {})
        usdt = s.get('usdt_free', 0)
        tokens = s.get('tokens_free', 0)
        usdt_l = s.get('usdt_locked', 0)
        tokens_l = s.get('tokens_locked', 0)
        if not levels or qty <= 0.0: return s, []
        # ⚠️ Quand gelé, ne PAS traiter les fills !
        if s.get('frozen', False):
            return s, []

        # ⚡ HFT: utiliser les prix min/max WS pour détecter les touches de niveau
        # (le prix spot peut manquer un pic creux entre deux ticks)
        check_price_buy = price
        check_price_sell = price
        if HFT_ENABLED and self._price_feed and self._price_feed.connected:
            ws_min = self._price_feed.price_min
            ws_max = self._price_feed.price_max
            if ws_min > 0 and ws_min < price:
                check_price_buy = ws_min
            if ws_max > 0 and ws_max > price:
                check_price_sell = ws_max
            if ws_min > 0 or ws_max > 0:
                self._price_feed.reset_price_range()
        idx_map = {str(l): i for i, l in enumerate(levels)}
        notifs = []

        # OPTIMISATION: un seul appel API par tick (au lieu de 1 appel par ordre)
        # En mode réel, pré-charger les ordres ouverts une seule fois pour tout le cycle recycle().
        # Si l'API échoue, on abandonne le tick entier (pas de fills traités partiellement).
        _open_orders_cache = None  # None = non chargé, [] = vide
        if self.real_mode and not self.dry_run:
            _open_orders_cache = self._real_get_open_orders()
            if _open_orders_cache is None:
                return s, []  # Erreur API — skip tout le tick

        tick_size_dec = max(0, -int(Decimal(str(self.tick_size)).as_tuple().exponent))

        for p_str, etype in list(lo.items()):
            idx = idx_map.get(p_str)
            if idx is None: continue
            p = float(p_str)
            if etype == 'buy':
                is_filled = check_price_buy <= p
                # ── Mode réel : vérifier via cache pré-chargé ──
                if self.real_mode:
                    open_orders = _open_orders_cache
                    if open_orders is None:
                        continue  # Erreur API (ne devrait plus arriver ici)
                    p_str_fmt = f"{p:.{tick_size_dec}f}"
                    still_open = any(
                        o['side'].lower() == 'buy'
                        and f"{float(o['price']):.{tick_size_dec}f}" == p_str_fmt
                        for o in open_orders
                    )
                    if not still_open:
                        is_filled = True  # Ordre disparu de Binance = filled
                    elif not is_filled:
                        continue        # Prix au-dessus et ordre encore ouvert
                    if self.dry_run:
                        pass  # En dry-run on simule le fill
                elif not is_filled:
                    continue  # Prix au-dessus et pas mode réel → skip
                usdt_l = max(0, usdt_l - p * qty * 1.0015)
                tokens += qty; del lo[p_str]
                # 💾 Enregistrer le timestamp et le prix du BUY pour le déblocage
                s['last_buy_time'] = time.time()
                s['last_buy_price'] = p
                # ⏱️ Timestamp du BUY fill pour le cycle timing
                self._buy_fill_times[str(p)] = time.time()
                if idx + 1 < len(levels):
                    sl = str(levels[idx + 1])
                    if sl not in lo and tokens >= qty:
                        sell_placed = True
                        # ── Mode réel : placer le SELL sur Binance AVANT de modifier l'état ──
                        if self.real_mode:
                            sell_qty = min(qty, tokens)
                            # 🔄 XRP pilote : SyncEngine place_and_verify()
                            if self.engine:
                                result = self.engine.place_and_verify('sell', sell_qty, float(sl))
                                if not result.success:
                                    sell_placed = False
                                    attempts = len(result.attempts)
                                    print(f"  🔴 SELL {qty}@{float(sl):.4f} — {attempts} tentatives échouées. ERROR_RECOVERY.")
                                    self.tg_send(
                                        f"🚨 XRP SELL ÉCHOUÉ ({attempts} retries)\\\n"
                                        f"BUY @ ${p:.4f} × {qty} FILLED\\\\n"
                                        f"SELL @ ${float(sl):.4f} NON PLACÉ\\\\n"
                                        f"Erreur: {result.error[:100]}"
                                    )
                                    # ERROR_RECOVERY : garder LONG_OPEN,
                                    # rebuild_state() au prochain cycle
                                    s['_sync_recovery'] = True
                                else:
                                    sell_placed = True
                            else:
                                result = self._real_place_order('sell', sell_qty, float(sl))
                                if not result:
                                    sell_placed = False
                                    err_msg = str(result) if result is not None else "API returned None"
                                    s['_pending_sell'] = {
                                        'price': float(sl), 'qty': qty,
                                        'buy_price': p,
                                        'since': time.time(),
                                        'attempts': s.get('_pending_sell', {}).get('attempts', 0) + 1,
                                        'last_error': err_msg
                                    }
                                    print(f"  ⚠️ SELL {qty}@{float(sl):.4f} échoué — retry actif (tentative #{s['_pending_sell']['attempts']})")
                                    self.tg_send(
                                        f"🚨 ALERTE: {self.name}\\\n"
                                        f"BUY @ ${p:.4f} × {qty} FILLED\\\\n"
                                        f"SELL @ ${float(sl):.4f} NON PLACÉ\\\\n"
                                        f"Erreur: {err_msg[:100]}"
                                    )
                        if sell_placed:
                            tokens -= qty; tokens_l += qty; lo[sl] = 'sell'
                            s['total_cycles'] = s.get('total_cycles', 0) + 1
                            profit = (levels[idx+1] - levels[idx]) * qty - (levels[idx+1] + levels[idx])/2*qty*FEE_RATE
                            s['total_profit_est'] = s.get('total_profit_est', 0) + profit
                            s['pending_cycles'] = s.get('pending_cycles', 0) + 1
                            notifs.append({'type':'BUY_FILL','buy':levels[idx],'sell':levels[idx+1],'qty':qty,'profit':profit,'cycle':s['total_cycles']})
            elif etype == 'sell':
                is_filled = check_price_sell >= p
                # ── Mode réel : vérifier via cache pré-chargé ──
                if self.real_mode:
                    open_orders = _open_orders_cache
                    if open_orders is None:
                        continue  # Erreur API (ne devrait plus arriver ici)
                    p_str_sell = f"{p:.{tick_size_dec}f}"
                    still_open = any(
                        o['side'].lower() == 'sell'
                        and f"{float(o['price']):.{tick_size_dec}f}" == p_str_sell
                        for o in open_orders
                    )
                    if not still_open:
                        is_filled = True  # Ordre disparu de Binance = filled
                    elif not is_filled:
                        continue        # Prix en dessous et ordre encore ouvert
                elif not is_filled:
                    continue  # Prix en dessous et pas mode réel → skip
                tokens_l = max(0, tokens_l - qty)
                tokens = max(0, tokens - qty)  # ⚠️ Aussi diminuer les tokens libres (balance sync peut les avoir augmentés)
                usdt += p * qty * (1 - FEE_RATE); del lo[p_str]
                # 📊 Log cycle complété (SELL filled)
                if idx > 0:
                    buy_price = levels[idx - 1]
                    cycle_profit = (p - buy_price) * qty - (p + buy_price) / 2 * qty * FEE_RATE
                    self._log_cycle(buy_price, p, qty, round(cycle_profit, 4))
                s['pending_cycles'] = max(0, s.get('pending_cycles', 0) - 1)
                # Notifier le SELL fill
                buy_price_for_notif = levels[idx - 1] if idx > 0 else 0
                sell_profit = (p - buy_price_for_notif) * qty - (p + buy_price_for_notif) / 2 * qty * FEE_RATE if idx > 0 else 0
                notifs.append({'type':'SELL_FILL','sell':p,'buy':buy_price_for_notif,'qty':qty,'profit':round(sell_profit,4),'cycle':s.get('total_cycles',0)})
                if idx > 0 and not s.get('frozen', False):
                    bl = str(levels[idx - 1])
                    if bl not in lo:
                        cost = levels[idx - 1] * qty * 1.0015
                        buy_placed = True
                        # ── Mode réel : placer le BUY sur Binance AVANT de modifier l'état ──
                        if self.real_mode and usdt >= cost:
                            result = self._real_place_order('buy', qty, float(bl))
                            if not result:
                                buy_placed = False
                                print(f"  ⚠️ BUY {qty}@{float(bl):.4f} échoué, skip")
                        if buy_placed and usdt >= cost:
                            usdt -= cost; usdt_l += cost; lo[bl] = 'buy'
        s['level_orders'] = lo
        s['usdt_free'] = float(f"{usdt:.2f}"); s['usdt_locked'] = float(f"{usdt_l:.2f}")
        qty_dec = s.get('_qty_dec', 1)
        s['tokens_free'] = float(f"{tokens:.{qty_dec}f}"); s['tokens_locked'] = float(f"{tokens_l:.{qty_dec}f}")
        self.save_state(s)
        return s, notifs

    # ── Trailing Profit Lock ────────────────────────────────
    def _check_trailing_profit(self, s, price):
        """Vérifie si un niveau de grille a atteint le seuil de profit → verrouille.
        Retourne (s, sold_levels) où sold_levels = liste des prix des niveaux vendus."""
        if not TRAILING_ENABLED or s.get('frozen', False):
            return s, []

        levels = s.get('levels', [])
        lo = s.get('level_orders', {})
        entry = s.get('entry_price', price)
        sold = []

        # Parcourir les niveaux — on cherche les SELL ordres qui ont du profit
        for p_str, otype in list(lo.items()):
            if otype != 'sell':
                continue
            p = float(p_str)
            idx = levels.index(p) if p in levels else -1
            if idx <= 0:
                continue

            # Le BUY correspondant était à levels[idx-1]
            buy_price = levels[idx - 1]
            if buy_price <= 0:
                continue

            # Tracker le pic de prix pour ce niveau
            trail_key = f"trail_{p_str}"
            trail = s.get(trail_key, {})

            if not trail:
                trail = {'peak': price, 'buy_price': buy_price, 'active': False}
            elif price > trail.get('peak', price):
                trail['peak'] = price

            profit_from_peak = (trail['peak'] - buy_price) / buy_price
            pullback_from_peak = (trail['peak'] - price) / trail['peak']

            # Activer le trailing si le profit seuil est atteint
            if profit_from_peak >= TRAILING_PROFIT_PCT and not trail.get('active'):
                trail['active'] = True
                trail['activated_at'] = time.time()
                print(f"  🎯 Trailing activé pour niveau {p_str} (pic +{profit_from_peak*100:.2f}%)")

            # Vendre si le trailing est actif ET le pullback dépasse le seuil
            if trail.get('active') and pullback_from_peak >= TRAILING_CALLBACK_PCT:
                qty = s.get('qty', 0.1)
                if self.real_mode:
                    # Règle SPOT: ne jamais annuler un SELL existant.
                    # Si un SELL est déjà positionné sur ce niveau (grid), il sera
                    # exécuté naturellement au prix prévu. On n'intervient que si
                    # aucun SELL n'existe (position orpheline).
                    existing_sell = any(v == 'sell' for v in lo.values())
                    if existing_sell:
                        # SELL déjà en place → pas d'action, laisser la grille fonctionner
                        if trail_key in s:
                            del s[trail_key]
                        print(f"  🎯 Trailing: SELL existant préservé @ niveau(x) actif(s) — pas d'intervention")
                        continue
                    # Pas de SELL → annuler uniquement les BUY et placer un SELL trailing
                    self._real_cancel_buys_only()
                    buy_keys = [k for k, v in lo.items() if v == 'buy']
                    for k in buy_keys:
                        del lo[k]
                    # Placer un ordre LIMIT au prix actuel pour vendre
                    result = self._real_place_order('sell', qty, price)
                    if not result:
                        continue
                # Mettre à jour l'état
                lo.pop(p_str, None)
                tokens_free = s.get('tokens_free', 0) + qty
                s['tokens_free'] = float(f"{tokens_free:.{s.get('_qty_dec', 1)}f}")
                s['tokens_locked'] = max(0, s.get('tokens_locked', 0) - qty)
                profit = (price - buy_price) * qty - (price + buy_price) / 2 * qty * FEE_RATE
                s['total_profit_est'] = s.get('total_profit_est', 0) + profit
                s['total_cycles'] = s.get('total_cycles', 0) + 1
                sold.append(p)
                # 📊 Log cycle dans perf_tracker (Kelly + analyse)
                self._log_cycle(buy_price, price, qty, round(profit, 4))
                print(f"  🔒 Trailing SELL niveau {p_str} (buy ${buy_price:.4f} → sell ${price:.4f}, "
                      f"pullback {pullback_from_peak*100:.2f}%)")
                self.tg_send(
                    f"🔒 Trailing SELL niveau {p_str}\n"
                    f"Achat ${buy_price:.4f} → Vente ${price:.4f}\n"
                    f"Profit: +{(price-buy_price)/buy_price*100:.2f}%"
                )
                # Nettoyer le tracker
                if trail_key in s:
                    del s[trail_key]
                continue

            # Sauvegarder le tracker mis à jour
            s[trail_key] = trail

        s['level_orders'] = lo
        self.save_state(s)
        return s, sold

    def _check_stuck_position(self, s, price):
        """🛟 Capital Liberation — Débloque les positions bloquées en tendance baissière.
        Conditions: délai min + baisse sous le BUY + persistance"""
        if not CAPITAL_LIBERATION_ENABLED or s.get('frozen', False):
            return s, False

        buy_time = s.get('last_buy_time', 0)
        buy_price = s.get('last_buy_price', 0)
        if buy_time == 0 or buy_price == 0:
            return s, False

        # ⏱️ 1. Délai minimum depuis le BUY
        elapsed = time.time() - buy_time
        if elapsed < CAPITAL_LIB_MIN_HOLD:
            return s, False

        # 📉 2. Prix sous le seuil de baisse
        drop = (price - buy_price) / buy_price
        if drop >= -CAPITAL_LIB_DROP_PCT:
            # Prix remonté → reset du timer de persistance
            if '_stuck_since' in s:
                del s['_stuck_since']
                self.save_state(s)
            return s, False

        # ⏳ 3. Vérification persistante
        stuck_since = s.get('_stuck_since', 0)
        if stuck_since == 0:
            s['_stuck_since'] = time.time()
            self.save_state(s)
            print(f"  🛟 Position bloquée détectée — prix {drop*100:+.2f}% sous le BUY, vérification {CAPITAL_LIB_PERSIST_SEC}s")
            return s, False

        if time.time() - stuck_since < CAPITAL_LIB_PERSIST_SEC:
            return s, False

        # 🚨 Conditions remplies → libérer le capital
        lo = s.get('level_orders', {})
        qty = s.get('qty', 0.1)

        if self.real_mode:
            # Règle SPOT: vérifier si un SELL est déjà ouvert sur Binance.
            # Si oui, ne pas l'annuler — seulement annuler les BUY.
            existing_sells = {k: v for k, v in lo.items() if v == 'sell'}
            if existing_sells:
                # SELL déjà en place → annuler uniquement les BUY, alerter
                self._real_cancel_buys_only()
                for k in [k for k, v in lo.items() if v == 'buy']:
                    del lo[k]
                s['level_orders'] = lo
                s.pop('_stuck_since', None)
                self.save_state(s)
                sell_prices_str = ', '.join(f"${float(k):.4f}" for k in existing_sells)
                msg = (f"🛟 Capital lib. — {self.name}\n"
                       f"SELL existant conservé @ {sell_prices_str}\n"
                       f"BUY annulés | ⏱️ Bloqué {int(elapsed/60)} min")
                self.tg_send(msg)
                print(f"  🛟 {self.name} — BUY annulés, SELL existant préservé @ {sell_prices_str}")
                return s, True

            # Aucun SELL → annuler BUY et placer un nouveau SELL à prix + spread
            self._real_cancel_buys_only()
            for k in [k for k, v in lo.items() if v == 'buy']:
                del lo[k]

            sell_price = fmt_price(price * (1 + CAPITAL_LIB_NEW_SPREAD), self.tick_size)
            result = self._real_place_order('sell', qty, sell_price)
            if result:
                lo[str(sell_price)] = 'sell'
                s['level_orders'] = lo
                s['tokens_locked'] = qty
                s['tokens_free'] = max(0, s.get('tokens_free', 0))
                s.pop('last_buy_time', None)
                s.pop('last_buy_price', None)
                s.pop('_stuck_since', None)
                self.save_state(s)
                loss_pct = (sell_price - buy_price) / buy_price * 100
                msg = (f"🛟 Capital libéré — {self.name}\n"
                       f"Achat @ ${buy_price:.4f} → Vente @ ${sell_price:.4f} ({loss_pct:+.2f}%)\n"
                       f"⏱️ Bloqué {int(elapsed/60)} min | 📉 Baisse max {drop*100:.2f}%")
                self.tg_send(msg)
                print(f"  🛟 {self.name} — Capital libéré! SELL @ ${sell_price:.4f}")
                return s, True
            else:
                print(f"  ❌ Échec placement SELL de libération")
                return s, False

        # Mode paper
        lo.clear()
        s.pop('last_buy_time', None)
        s.pop('last_buy_price', None)
        s.pop('_stuck_since', None)
        self.save_state(s)
        print(f"  🛟 [Paper] Capital libéré! Achat ${buy_price:.4f} → Vente @ ${price*(1+CAPITAL_LIB_NEW_SPREAD):.4f}")
        return s, True

    def should_redeploy(self, s, price):
        if s.get('frozen', False):
            return False, "frozen"
        # 🚫 SELL en attente de placement → pas de redéploiement
        if s.get('_pending_sell'):
            return False, "pending sell"
        # 🛡️ Tokens possédés sans SELL → pas de redéploiement
        total_tokens = s.get('tokens_free', 0) + s.get('tokens_locked', 0)
        if total_tokens > 0:
            lo = s.get('level_orders', {})
            has_sell = any(v == 'sell' for v in lo.values())
            if not has_sell:
                # 🛟 Option B — Reconstruction orphelins
                if ORPHAN_REBUILD_ENABLED and not s.get('_orphan_rebuild_flag'):
                    if self._try_rebuild_sell(s, price):
                        return False, "orphan rebuild placed"
                return False, "tokens without sell order"
        # 🧠 P1 Filter — Bloquer le redéploiement si tendance 24h négative significative
        p1_active = P1_FILTER_ENABLED and (P1_SYMBOLS == ['ALL'] or self.name in P1_SYMBOLS)
        if p1_active:
            trend_down, price_now, price_24h = get_trend_24h(self.symbol)
            if trend_down and price_24h > 0:
                drop_pct = abs((price_now - price_24h) / price_24h * 100)
                if drop_pct >= P1_MIN_DROP_PCT:
                    print(f"  🧠 P1: {self.name} — tendance 24h négative (${price_24h:.4f} → ${price_now:.4f}, -{drop_pct:.1f}%), BUY bloqué")
                    return False, f"P1: 24h downtrend (-{drop_pct:.1f}%)"
        entry = s.get('entry_price', price)
        levels = s.get('levels', [])
        lo = s.get('level_orders', {})
        if not lo:
            # 🔇 Cooldown après un skip (0 ordres possibles)
            skip_until = s.get('_skip_until', 0)
            if self.real_mode and time.time() < skip_until:
                return False, f"cooldown ({int(skip_until - time.time())}s left)"
            return True, "no orders left"
        types = set(lo.values())
        only_buys = (len(types) == 1 and 'buy' in types)
        only_sells = (len(types) == 1 and 'sell' in types)
        # 🛑 QUE des BUY et prix au-dessus du dernier niveau → ne PAS redéployer
        # Le BUY est une bonne affaire en attente, on le laisse tranquille
        if only_buys and price > levels[-1]:
            return False, "only buys, price above grid (keep buy order)"
        # ✅ QUE des SELL et prix sous le premier niveau → normal, on attend le fill
        if only_sells and price < levels[0]:
            return False, f"only sell, price below sell level (normal)"
        # ⚠️ Redéploiement uniquement si le prix SORT de la grille
        if levels and (price < levels[0] or price > levels[-1]):
            return True, f"price outside grid"
        if len(types) == 1:
            # En mode réel, ne pas redéployer si on a des ordres ouverts (vendre ce qu'on a)
            if self.real_mode:
                return False, f"all {list(types)[0]} only (real, keeping orders)"
            return True, f"all {list(types)[0]} only"
        return False, ""

    def _try_rebuild_sell(self, s, price):
        """🛟 Option B — Reconstruction d un SELL pour tokens orphelins.
        Déclenché quand should_redeploy détecte tokens > 0 sans SELL.
        Conditions de sécurité : 6 garde-fous + vérification Binance."""
        now = time.time()

        # Garde-fou 1 : Ne pas interférer avec un freeze en cours
        if s.get('frozen', False):
            return False
        # Garde-fou 2 : Ne pas interférer avec un SELL en attente de placement
        if s.get('_pending_sell'):
            return False
        # Garde-fou 3 : Laisser le temps à recycle() de traiter un fill récent
        last_buy = s.get('last_buy_time', 0)
        if last_buy and (now - last_buy) < ORPHAN_REBUILD_MIN_WAIT:
            return False
        # Garde-fou 4 : Vérifier qu aucun ordre réel n existe sur Binance
        open_orders = self._real_get_open_orders()
        if open_orders is None or len(open_orders) > 0:
            return False
        # Garde-fou 5 : Valeur économique minimale
        tok_free = s.get('tokens_free', 0)
        if tok_free * price < self.min_notional:
            return False
        # Garde-fou 6 : Vérifier que les tokens ne viennent pas d un SELL en cours
        # (le SELL n a pas encore été ajouté à level_orders)
        # → déjà vérifié par open_orders + last_buy

        # Calculer le prix de vente : JAMAIS sous le marché
        sell_price = fmt_price(price * (1 + ORPHAN_REBUILD_SPREAD), self.tick_size)

        # Vérification perte excessive
        entry = s.get('entry_price', 0)
        loss_pct = 0.0
        if entry > 0:
            loss_pct = (sell_price - entry) / entry
            if loss_pct < -ORPHAN_MAX_LOSS_PCT:
                sell_price = fmt_price(entry * (1 - ORPHAN_MAX_LOSS_PCT), self.tick_size)
                loss_pct = (sell_price - entry) / entry
                print(f"  ⚠️ Orphelin: perte excessive évitée, prix ajusté à ${sell_price:.4f} ({loss_pct*100:+.2f}%)")

        # Placer le SELL sur Binance
        qty_dec = s.get('_qty_dec', 1)
        sell_qty = float(Decimal(str(tok_free)).quantize(Decimal(str(self.lot_step)), rounding=ROUND_DOWN))
        if sell_qty * sell_price < self.min_notional:
            print(f"  ⚠️ Orphelin: qty {sell_qty} sous MIN_NOTIONAL — skip")
            return False

        result = self._real_place_order('sell', sell_qty, sell_price)
        if result:
            lo = s.get('level_orders', {})
            lo[str(sell_price)] = 'sell'
            s['level_orders'] = lo
            s['tokens_locked'] = sell_qty
            s['tokens_free'] = max(0, float(f"{(tok_free - sell_qty):.{qty_dec}f}"))
            s['_orphan_rebuild_flag'] = True
            s['_orphan_rebuild_time'] = now
            self.save_state(s)
            pct_str = f"({loss_pct*100:+.2f}%)" if entry > 0 else ""
            log_msg = f"🔧 {self.name} — SELL reconstruction @ ${sell_price:.4f} ({sell_qty:.0f} tokens) {pct_str}"
            print(f"  {log_msg}")
            self.tg_send(log_msg)
            return True
        print(f"  ❌ Orphelin: échec placement SELL @ ${sell_price:.4f}")
        return False

    def redeploy_grid(self, s, price):
        pv = self.portfolio_value(s, price)
        cycles = s.get('total_cycles', 0)
        profit = s.get('total_profit_est', 0.0)
        # ⚠️ Toujours utiliser self.capital ($47), PAS pv !
        # pv est gonflé par usdt_free fictif → inflate le capital à chaque cycle
        new_s = self.deploy(price, capital=self.capital, prev_cycles=cycles, prev_profit=profit)
        if new_s is None:
            print(f"  ⛔ Redéploiement annulé (conditions bloquantes)")
            # Recharger le state depuis le disque (deploy() a pu sauvegarder _skip_until)
            disk_s = self.load_state()
            if disk_s:
                return disk_s
            return s
        self.tg_send(f"🔄 Redéploiement à ${price:.4f}\n💰 PV: ${pv:.2f} | 📊 {cycles} cycles")
        # ⚠️ Forcer capital et phase2_start à self.capital ($50)
        # pour éviter que redeploy() écrase avec la valeur du portefeuille
        new_s['capital'] = self.capital
        new_s['phase2_start'] = self.capital
        new_s['day_date'] = s.get('day_date', date.today().isoformat())
        new_s['day_start_value'] = s.get('day_start_value', pv)
        new_s['daily_loss'] = s.get('daily_loss', 0.0)
        new_s['frozen'] = s.get('frozen', False)
        new_s['peak_price'] = s.get('peak_price', price)
        new_s['peak_time'] = s.get('peak_time', time.time())
        new_s['freeze_price'] = s.get('freeze_price', 0.0)
        new_s['freeze_time'] = s.get('freeze_time', 0)
        self.save_state(new_s)
        return new_s

    def check_reset(self):
        reset_file = f"{BASE_DIR}/{self.name}.reset"
        if os.path.exists(reset_file):
            price = self._get_price()
            if price == 0:
                return None
            s = self.load_state()
            if not s:
                return None
            pv = self.portfolio_value(s, price)
            capital_base = s.get('initial_capital', s.get('capital', 100.0))
            profit = max(0, pv - capital_base)
            pool_file = f"{BASE_DIR}/profit_pool.json"
            pool = {}
            if os.path.exists(pool_file):
                with open(pool_file) as f:
                    pool = json.load(f)
            pool[self.name] = pool.get(self.name, 0) + round(profit, 2)
            with open(pool_file, 'w') as f:
                json.dump(pool, f, indent=2)
            self.tg_send(
                f"💰 PROFIT EXTRACTED: ${profit:.2f}\n"
                f"📦 Pool {self.name}: $<code>{pool[self.name]:.2f}</code>\n"
                f"🔄 Redémarrage avec ${self.capital:.0f}"
            )
            os.remove(reset_file)
            new_s = self.deploy(
                price,
                capital=self.capital,
                prev_cycles=s.get('total_cycles', 0),
                prev_profit=s.get('total_profit_est', 0.0)
            )
            return new_s
        return None

    def check_manual_freeze(self, s):
        freeze_file = f"{BASE_DIR}/{self.name}.freeze"
        if os.path.exists(freeze_file):
            s['frozen'] = True
            s['manual_freeze'] = True
            s['freeze_price'] = self._get_price() or s.get('entry_price', s.get('deploy_price', 0))
            if s['freeze_price'] <= 0:
                s['freeze_price'] = price  # fallback ultime
            s['freeze_time'] = time.time()
            spread = 0.05
            self._price_history = [s['freeze_price'] * (1 + spread * (i/10)) for i in range(10)]
            self.save_state(s)
            os.remove(freeze_file)
            self.tg_send(f"🧊 FREEZE MANUEL — {self.name} gelé")
            print(f"🧊 {self.name} — Freeze manuel activé")
        return s

    def run(self):
        # 🔄 XRP pilote : rebuild_state() au démarrage (source de vérité = Binance)
        if self.engine:
            print(f"  🔄 Sync Engine — Rebuild state depuis Binance...")
            report = self.engine.rebuild_state()
            report.print_report()
            if report.state == 'LONG_OPEN' or report.state == 'SELL_OPEN':
                # Position existante → état depuis Binance
                s = self._state_from_report(report)
                if report.state == 'LONG_OPEN':
                    # 🟢 LONG_OPEN → skip tout déploiement, entrer dans la boucle
                    price = self._get_price()
                    if price <= 0:
                        print(f"❌ Impossible d'obtenir le prix {self.symbol}")
                        return
                    self.tg_send(f"🔄 XRP pilote — LONG_OPEN\n"
                                 f"Tokens: {s.get('tokens_free', 0):.2f} XRP\n"
                                 f"PRU: ${s.get('last_buy_price', 0):.4f}\n"
                                 f"Prix: ${price:.4f}\n"
                                 f"Sync Engine actif — observation 24h")
                    print(f"  🟢 XRP LONG_OPEN — position préservée, skip déploiement")
                    # Timer pour le prochain rebuild_state périodique (1h)
                    self._last_rebuild_time = time.time()
                    # Aller directement dans la boucle principale (skip if/elif deploy)
                    # → goto main_loop (le flag _skip_deploy évite le bloc deploy ci-dessous)
                    s['_skip_deploy'] = True
            elif report.state == 'WAIT_BUY':
                # Pas de position → déploiement normal
                s = self.load_state()
            else:
                print(f"  ⚠️ Sync Engine — État inattendu: {report.state}, fallback state local")
                s = self.load_state()
        else:
            s = self.load_state()

        # 📦 Checkpoint au démarrage
        self._checkpoint("pre-boot")

        # ── Mode réel : sync avec Binance, ne pas forcer un redeploy ──
        if self.real_mode:
            # 🔄 XRP pilote : LONG_OPEN → skip déploiement
            skip_deploy = bool(s and s.get('_skip_deploy'))
            if skip_deploy:
                s.pop('_skip_deploy', None)
                print(f"  🟢 XRP LONG_OPEN — skip déploiement, entrée directe dans la boucle")
            
            # Si _deploy_in_progress=True mais aucun ordre ouvert sur Binance,
            # (évite 30s de fenêtre morte avec des ordres fantômes).
            if s and s.get('_deploy_in_progress', False):
                try:
                    open_orders = self._real_get_open_orders()
                    if open_orders is not None and len(open_orders) == 0:
                        s['_deploy_in_progress'] = False
                        s['_deploy_in_progress_since'] = 0
                        self.save_state(s)
                        print("  🧹 Flag orphelin — reset (aucun ordre réel sur Binance)")
                except Exception:
                    print("  ⚠️ Flag orphelin: erreur API, pas de reset")

            if not skip_deploy and s and s.get('levels') and s.get('level_orders'):
                # État existant avec des ordres → sync from Binance
                s = self._real_sync_state_from_orders(s)
                has_orders = bool(s.get('level_orders', {}))
                if has_orders:
                    print(f"  🌐 Mode RÉEL — {len(s['level_orders'])} ordres restaurés depuis Binance")
                    price = self._get_price()
                else:
                    print(f"  🌐 Mode RÉEL — état vide, déploiement depuis Binance")
                    price = self._get_price()
                    if price == 0:
                        print(f"❌ Impossible d'obtenir le prix {self.symbol}")
                        return
                    s = self.deploy(price,
                        prev_cycles=s.get('total_cycles', 0) if s else 0,
                        prev_profit=s.get('total_profit_est', 0.0) if s else 0.0)
            if not skip_deploy:
                # Pas d'état → déploiement frais
                print(f"  🌐 Mode RÉEL — premier déploiement")
                price = self._get_price()
                if price == 0:
                    print(f"❌ Impossible d'obtenir le prix {self.symbol}")
                    return
                s = self.deploy(price,
                    prev_cycles=s.get('total_cycles', 0) if s else 0,
                    prev_profit=s.get('total_profit_est', 0.0) if s else 0.0)
            if s is None:
                print(f"  ⛔ Déploiement annulé (conditions bloquantes) — attente 5min avant retry")
                self.tg_send(f"⛔ {self.name} non déployé (conditions bloquantes) — retry dans 5min")
                # Au lieu de return, entrer dans la boucle avec un state minimal pour retry
                s = self.load_state() or {}
                s['_skip_until'] = time.time() + 300  # 5 min de cooldown
                s['capital'] = self.capital
                s['frozen'] = False
                s['real_mode'] = self.real_mode
                self.save_state(s)
                # On entre dans la boucle principale qui va retenter deploy au prochain refresh
                price = last_price if 'last_price' in dir() else self._get_price()
                if price <= 0:
                    price = 0.754  # fallback prix SUI
            if not skip_deploy:
                nb = sum(1 for v in s['level_orders'].values() if v == 'buy')
                ns = sum(1 for v in s['level_orders'].values() if v == 'sell')
                if nb + ns > 0:
                    self.tg_send(f"📦 Déploiement RÉEL ${self.capital}\\n📈 ${price:.4f}\\n📋 {nb}B+{ns}S")
                    print(f"✅ {self.name} déployé en réel à ${price:.4f} ({nb}B+{ns}S)")
                else:
                    print(f"✅ {self.name} déployé en réel à ${price:.4f} (0B+0S — skip notification)")
        elif not s or 'levels' not in s or not s.get('levels'):
            price = self._get_price()
            if price == 0:
                print(f"❌ Impossible d'obtenir le prix {self.symbol}")
                return
            s = self.deploy(price)
            if s is None:
                print(f"  ⛔ Déploiement annulé (conditions bloquantes)")
                return
            nb = sum(1 for v in s['level_orders'].values() if v == 'buy')
            ns = sum(1 for v in s['level_orders'].values() if v == 'sell')
            self.tg_send(f"📦 Déploiement ${self.capital}\n📈 ${price:.4f}\n📋 {nb}B+{ns}S")
            print(f"✅ {self.name} déployé à ${price:.4f}")

        last_price_time = 0
        last_redeploy_time = 0
        last_sync_time = 0
        last_balance_sync_time = 0
        last_order_sync_time = 0
        last_kelly_refresh = 0

        while True:
            try:
                if self.check_stop():
                    self.tg_send(f"🛑 Arrêté — STOP file")
                    if self.real_mode:
                        self._real_cancel_all()
                    break

                new_s = self.check_reset()
                if new_s is not None:
                    s = new_s
                    last_price_time = 0
                    last_redeploy_time = 0
                    continue

                s = self.check_manual_freeze(s)

                now = time.time()

                # ── Retry SELL en attente (après échec dans recycle()) ──
                if s.get('_pending_sell'):
                    ps = s['_pending_sell']
                    if self.real_mode:
                        result = self._real_place_order('sell', ps['qty'], ps['price'])
                        if result:
                            print(f"  ✅ SELL retry #{ps['attempts']} réussi @ ${ps['price']:.4f}")
                            self.tg_send(f"✅ {self.name} — SELL replacé @ ${ps['price']:.4f} "
                                         f"après {ps['attempts']} tentative(s)")
                            del s['_pending_sell']
                            self.save_state(s)
                        else:
                            ps['attempts'] += 1
                            ps['last_error'] = str(result) if result is not None else "API returned None"
                            if ps['attempts'] % 5 == 0:
                                elapsed = int(time.time() - ps['since'])
                                self.tg_send(f"⏳ {self.name} — SELL toujours pas placé "
                                             f"({elapsed}s, {ps['attempts']} tentatives, "
                                             f"erreur: {ps['last_error'][:50]})")

                # ── Mode réel : sync état depuis les ordres Binance ──
                if self.real_mode and now - last_sync_time >= 5.0:
                    # 🔄 XRP pilote : rebuild_state périodique (toutes les heures)
                    if self.engine and now - getattr(self, '_last_rebuild_time', 0) >= 3600.0:
                        print(f"  🔄 Sync Engine — Rebuild périodique...")
                        report = self.engine.rebuild_state()
                        if report.state == 'LONG_OPEN':
                            if not s.get('level_orders') or not any(v == 'sell' for v in s['level_orders'].values()):
                                print(f"  ✅ LONG_OPEN confirmé (pas de changement)")
                            self._last_rebuild_time = now
                        elif report.state == 'SELL_OPEN':
                            print(f"  ✅ SELL_OPEN confirmé")
                            self._last_rebuild_time = now
                        elif report.state == 'WAIT_BUY':
                            print(f"  ⚠️ XRP position fermée (WAIT_BUY) — reset state")
                            s = self._state_from_report(report)
                            self._last_rebuild_time = now
                        elif report.state == 'ERROR_RECOVERY':
                            print(f"  🔴 XRP — Erreur API lors du rebuild")
                    
                    # 🔄 XRP : recovery après échec SELL
                    if self.engine and s.get('_sync_recovery'):
                        print(f"  🔄 Sync Engine — Recovery après échec SELL...")
                        report = self.engine.rebuild_state()
                        if report.state == 'LONG_OPEN':
                            # Toujours en LONG_OPEN, la stratégie décidera
                            print(f"  ✅ LONG_OPEN confirmé (en attente de la stratégie)")
                            s = self._state_from_report(report)
                            s['_sync_recovery'] = True  # Garder le flag
                        elif report.state == 'SELL_OPEN':
                            # SELL finalement placé (récupération)
                            print(f"  ✅ SELL récupéré — recovery terminé")
                            s = self._state_from_report(report)
                            del s['_sync_recovery']
                        self.save_state(s)
                    
                    s = self._real_sync_state_from_orders(s)
                    # ⚠️ Drainer le buffer OrderStream (WebSocket) pour éviter fuite mémoire
                    # Les fills instantanés ne sont pas encore intégrés à recycle(),
                    # mais au moins on empêche le buffer de grossir indéfiniment.
                    if HFT_USE_ORDER_STREAM and self._order_stream:
                        ws_fills = self._order_stream.get_fills()
                        if ws_fills:
                            print(f"  ⚡ OrderStream: {len(ws_fills)} fills drainés")
                    last_sync_time = now

                # ── Mode réel : sync soldes réels (toutes les 60s) ──
                if self.real_mode and now - last_balance_sync_time >= 60.0:
                    s = self._real_sync_balances(s)
                    last_balance_sync_time = now

                # ── Mode réel : sync ordres ouverts (toutes les 5min) ──
                if self.real_mode and now - last_order_sync_time >= 300.0:
                    lo_before = len(s.get('level_orders', {}))
                    s = self._real_sync_state_from_orders(s)
                    lo_after = len(s.get('level_orders', {}))
                    if lo_before != lo_after:
                        print(f"  🔄 Order sync: {lo_before}→{lo_after} ordres depuis Binance")
                        self.save_state(s)
                    # 🔍 Désynchronisation : si level_orders vide après sync mais attendu
                    if lo_after == 0 and lo_before > 0 and not s.get('frozen', False):
                        self.tg_send(f"⚠️ Désynchronisation détectée | {lo_before} ordres perdus après sync")
                    last_order_sync_time = now

                # 🔄 BUY_REPOSITIONED — Vérification des BUY vieillissants (toutes les 5min)
                if (self.real_mode and not self.engine  # Pas sur XRP (SyncEngine)
                        and now - getattr(self, '_last_aging_check', 0) >= 300.0):
                    self._check_aging_buys(s)
                    self._last_aging_check = now

                # 📊 Rafraîchir l'allocation Kelly toutes les 30 min
                if self.kelly and now - last_kelly_refresh >= 1800:
                    new_cap = perf_tracker.calc_kelly_capital(self.name)
                    if abs(new_cap - self.capital) > 1.0:
                        diff = new_cap - self.capital
                        self.capital = new_cap
                        print(f"  📊 Kelly refresh: ${new_cap:.0f} ({diff:+.1f})")
                        self.tg_send(f"📊 Kelly refresh: ${new_cap:.0f} ({diff:+.1f})")
                    last_kelly_refresh = now

                # 📊 SR — Rafraîchir l'analyse S/R
                if SR_ENABLED and self._sr and now - self._last_sr_refresh >= SR_REFRESH_SEC:
                    self._sr.get(self.symbol, force=True)
                    self._last_sr_refresh = now
                    if self._sr._cache.get(self.symbol):
                        cached = self._sr._cache[self.symbol]['result']
                        print(f"  📊 SR: {len(cached['support_levels'])}S/{len(cached['resistance_levels'])}R "
                              f"POC=${cached['poc']:.0f} "
                              f"TVP={'✅' if cached.get('trendline_support') else '❌'}/"
                              f"TVD={'✅' if cached.get('trendline_resistance') else '❌'}")

                # 🚦 MG — Market Gate refresh (toutes les 30 min)
                if MG_ENABLED and self._mg and now - self._last_mg_refresh >= MG_REFRESH_SEC:
                    self._mg_result = self._mg.evaluate(self.symbol)
                    self._last_mg_refresh = now
                    mg = self._mg_result
                    print(f"  🚦 MG: Score {mg['score']} (A={mg['system_a']['score']} B={mg['system_b']['score']}) → {mg['action']}")
                    if mg['action'] != 'ok':
                        self.tg_send(mg['alert'])

                # 📡 NF — NewsFeed refresh (toutes les 15 min)
                if NF_ENABLED and self._nf and now - self._last_nf_refresh >= NF_REFRESH_SEC:
                    self._nf_result = self._nf.refresh()
                    self._last_nf_refresh = now
                    nf = self._nf_result
                    print(f"  📡 NF: sentiment {nf['sentiment']:+.2f} ({nf['score_status']}) | "
                          f"breaking={len(nf['breaking'])} | sources={', '.join(nf['sources_used'])}")
                    if nf['breaking'] and nf['sentiment'] < -0.1:
                        self.tg_send(self._nf.format_report())

                if now - last_price_time >= 2.0:
                    price = self._get_price()
                    if price == 0:
                        time.sleep(0.5)
                        continue
                    last_price = price
                    last_price_time = now

                    entry = s.get('entry_price', price)

                    # Nuclear floor
                    if (entry - price) / entry > NUCLEAR_FLOOR:
                        self.tg_send(f"💀 NUCLEAR FLOOR — ${price:.4f}")
                        if self.real_mode:
                            self._real_cancel_all()
                        break

                    # Daily loss — ignorer si pas encore de cycle (faux positif au démarrage)
                    # 🛡️ Protection race condition fill : si un ordre vient de se remplir,
                    #    forcer un sync balances AVANT le calcul PV.
                    #    Pour les SELL : réintégrer la valeur économique du fill dans la PV
                    #    (les tokens ont disparu mais l'USDT n'est pas encore dans le state).
                    #    Corrige bug ADA 09/06 : fill SELL → tokens_locked=0 → PV fausse → faux STOP.
                    last_fill = s.get('_recent_fill_ts', 0)
                    if last_fill and time.time() - last_fill < 30:
                        print(f"  🛡️ Fill récent détecté ({time.time()-last_fill:.0f}s ago) — sync balances forcé")
                        s = self._real_sync_balances(s)
                        fill_value = s.pop('_recent_fill_value_usdt', 0)
                        if fill_value > 0:
                            # PV avant sync perdu les tokens, ajouter la valeur économique
                            pv = self.portfolio_value(s, price) + fill_value
                            print(f"  🛡️ PV daily loss ajustée: ${pv:.2f} (+${fill_value:.2f} fill SELL réintégré)")
                        else:
                            pv = self.portfolio_value(s, price)
                    else:
                        pv = self.portfolio_value(s, price)
                    # 📋 DL_DEBUG — Temporaire (20 cycles) : vérifier le fix A usdt_locked
                    _dl_left = s.get('_dl_debug_remaining', 20)
                    if _dl_left > 0:
                        _has_preserved = sum(1 for v in s.get('level_orders', {}).values() if v == 'buy') > 0
                        print(f"  📋 DL_DEBUG: ts={datetime.now().isoformat()[:19]} "
                              f"price=${price:.4f} DSV=${s.get('day_start_value', pv):.2f} "
                              f"PV=${pv:.2f} free=${s.get('usdt_free', 0):.2f} "
                              f"locked=${s.get('usdt_locked', 0):.2f} "
                              f"tf={s.get('tokens_free', 0):.1f} tl={s.get('tokens_locked', 0):.1f} "
                              f"preserved={'Y' if _has_preserved else 'N'} "
                              f"remaining={_dl_left-1}")
                        s['_dl_debug_remaining'] = _dl_left - 1
                    today = date.today().isoformat()
                    if s.get('day_date') != today:
                        s['day_date'] = today
                        s['day_start_value'] = pv
                        s['daily_loss'] = 0.0
                    loss = s.get('day_start_value', pv) - pv
                    s['daily_loss'] = loss
                    # 📊 Logging détaillé Daily Loss (collecte observation)
                    log_fields = {
                        'ts': datetime.now().isoformat(),
                        'symbol': self.symbol,
                        'price': price,
                        'day_start_value': round(s.get('day_start_value', pv), 2),
                        'pv': round(pv, 2),
                        'loss': round(loss, 2),
                        'loss_pct': round(round(loss, 2) / max(s.get('day_start_value', pv), 0.01) * 100, 2),
                        'usdt_free': s.get('usdt_free', 0),
                        'usdt_locked': s.get('usdt_locked', 0),
                        'tokens_free': s.get('tokens_free', 0),
                        'tokens_locked': s.get('tokens_locked', 0),
                        'total_cycles': s.get('total_cycles', 0),
                        'capital': s.get('capital', 0),
                        'phase2_start': s.get('phase2_start', 0),
                        'frozen': s.get('frozen', False),
                        'triggered': loss > s.get('day_start_value', pv) * MAX_DAILY_LOSS_PCT,
                    }
                    log_line = f"  📊 DL: DSV=${log_fields['day_start_value']} PV=${log_fields['pv']} loss=${log_fields['loss']} ({log_fields['loss_pct']}%) free=${log_fields['usdt_free']} locked=${log_fields['usdt_locked']} tf={log_fields['tokens_free']} tl={log_fields['tokens_locked']}"
                    print(log_line)
                    if log_fields['triggered'] and self.real_mode:
                        # Sauvegarder le snapshot dans un fichier dédié
                        import json as _json
                        snapshot_file = f"{BASE_DIR}/daily_loss_snapshots.json"
                        snaps = []
                        if os.path.exists(snapshot_file):
                            try:
                                with open(snapshot_file) as _f:
                                    snaps = _json.load(_f)
                            except:
                                snaps = []
                        snaps.append(log_fields)
                        # Garder max 50 snapshots
                        with open(snapshot_file, 'w') as _f:
                            _json.dump(snaps[-50:], _f, indent=2, default=str)
                        print(f"  📝 Snapshot sauvegardé dans {snapshot_file}")
                    if s.get('total_cycles', 0) > 0 and loss > s.get('day_start_value', pv) * MAX_DAILY_LOSS_PCT:
                        self.tg_send(f"🚨 DAILY LOSS 3% | PV=${pv:.2f} ({log_fields['loss_pct']}%) | cycle #{s.get('total_cycles',0)}")
                        if self.real_mode:
                            # Préserver les SELL: la position peut encore récupérer
                            _, sells_kept = self._real_cancel_buys_only()
                            if sells_kept:
                                self.tg_send(f"🛡️ {sells_kept} SELL préservé(s) — resteront actifs")
                        break

                    # Phase 2 stop — ignorer si pas encore de cycle ou si position active
                    if s.get('total_cycles', 0) > 0:
                        if s.get('tokens_locked', 0) > 0:
                            # Position ouverte protégée par un SELL → ne pas interrompre
                            pass
                        else:
                            p2start = s.get('phase2_start', 100.0)
                            p2loss = (p2start - pv) / p2start
                            if p2loss > PHASE2_STOP_PCT:
                                self.tg_send(f"🛑 PHASE 2 STOP -2%\nPortefeuille: ${pv:.2f}")
                                if self.real_mode:
                                    self._real_cancel_buys_only()
                                break

                    # 🛡️ Price Circuit Breaker
                    drop_from_entry = (entry - price) / entry
                    drop_level = s.get('drop_level', 0)

                    if drop_from_entry >= PRICE_CB_L2 and drop_level < 2:
                        s['drop_level'] = 2
                        s['frozen'] = True
                        self._buy_fill_times.clear()  # cleanup mémoires de cycles orphelins
                        # ⚠️ Sauvegarder les métadonnées de gel pour le dégel adaptatif
                        s['freeze_price'] = price
                        s['freeze_time'] = time.time()
                        s['freeze_drop_pct'] = drop_from_entry
                        lo = s.get('level_orders', {})
                        buy_removed = sum(1 for v in lo.values() if v == 'buy')
                        new_lo = {k: v for k, v in lo.items() if v != 'buy'}
                        s['level_orders'] = new_lo
                        s['usdt_free'] = round(s.get('usdt_free', 0) + s.get('usdt_locked', 0), 2)
                        s['usdt_locked'] = 0
                        if self.real_mode:
                            # CB L2: annuler uniquement les BUY — les SELL restent actifs
                            self._real_cancel_buys_only()
                        self.save_state(s)
                        self.tg_send(f"⚠️ PRICE CB L2 🟡 — -{drop_from_entry*100:.1f}% depuis entry\n"
                                     f"📋 {buy_removed} BUY supprimés → Sell-only")
                        print(f"🟡 {self.name} — PRICE CB L2: {buy_removed} BUY retirés")
                        continue

                    if drop_from_entry >= PRICE_CB_L1 and drop_level < 1:
                        s['drop_level'] = 1
                        s['frozen'] = True
                        self._buy_fill_times.clear()  # cleanup mémoires de cycles orphelins
                        # ⚠️ Sauvegarder les métadonnées de gel pour le dégel adaptatif
                        s['freeze_price'] = price
                        s['freeze_time'] = time.time()
                        s['freeze_drop_pct'] = drop_from_entry
                        self.save_state(s)
                        if self.real_mode:
                            # CB L1: geler les BUY uniquement — les SELL restent actifs
                            self._real_cancel_buys_only()
                        self.tg_send(f"⚠️ PRICE CB L1 🟢 — -{drop_from_entry*100:.1f}% depuis entry ${entry:.4f}\n"
                                     f"🧊 BUY gelés. Surveillance en cours...")
                        print(f"🟢 {self.name} — PRICE CB L1 à ${price:.4f} ({drop_from_entry*100:.1f}% drop)")
                        continue

                    # 🧠 Mode GEL Multi-Agents (MAF)
                    peak = s.get('peak_price', price)
                    frozen = s.get('frozen', False)

                    # Mettre à jour l'orchestrateur avec le tick actuel
                    if MAF_ENABLED and hasattr(self, '_freeze_orch') and self._freeze_orch is not None:
                        # Récupérer sigma depuis le QuantEngine
                        ma_sigma = getattr(self._quant.vol, 'sigma', 0.0) if hasattr(self, '_quant') else 0.0
                        self._freeze_orch.tick(price, ma_sigma)

                    if not frozen:
                        if price > peak:
                            s['peak_price'] = price
                            s['peak_time'] = time.time()
                            peak = price
                        drop_pct = (peak - price) / peak if peak > 0 else 0.0
                        time_since_peak = time.time() - s.get('peak_time', 0)

                        # Décision MAF: seuil adaptatif ou seuil fixe
                        if MAF_ENABLED and self._freeze_orch.momentum.is_ready:
                            maf = self._freeze_orch.evaluate(
                                drop_pct, time_since_peak,
                                price=price, sigma=ma_sigma
                            )
                            should_freeze = maf['should_freeze']
                            freeze_level = maf['freeze_level']
                            freeze_drop_now = maf['freeze_drop_now']
                            threshold_label = self._freeze_orch.get_label(maf)
                            print(f"  🧠 MAF: {maf['momentum_state']}/{maf['volatility_state']} "
                                  f"drop={drop_pct*100:.1f}% {threshold_label}")

                            # 📊 SR — Override freeze si prix près d'un support fort
                            if SR_ENABLED and should_freeze and self._sr:
                                sr_check = self._sr.set_price(self.symbol, price)
                                if sr_check and sr_check['near_support']:
                                    # Prix près d'un support → le marché peut rebondir
                                    # On annule le gel si la baisse est < 2× le buffer S/R
                                    if drop_pct <= max(abs(maf['freeze_threshold']), 0.02) * SR_FREEZE_DIV_NEAR_SUPPORT:
                                        should_freeze = False
                                        freeze_level = 0
                                        print(f"    📊 SR: près du support ${sr_check['nearest_support']:.2f} "
                                              f"→ freeze annulé (support buffer)")

                            # 📊 SR — Freeze PLUS tôt si prix près d'une résistance (résistance tient)
                            if SR_ENABLED and not should_freeze and self._sr:
                                sr_check = self._sr.set_price(self.symbol, price)
                                if sr_check and sr_check['near_resistance']:
                                    near_rs = sr_check['nearest_resistance']
                                    prox = sr_check['resistance_proximity_pct']
                                    # Prix bloqué sous résistance → risque de rejet brutal
                                    if prox <= SR_FREEZE_BUFFER_PCT and maf.get('momentum_state') == 'bearish':
                                        should_freeze = True
                                        freeze_level = 1
                                        freeze_drop_now = True
                                        print(f"    📊 SR: près résistance ${near_rs:.2f} + bearish "
                                              f"→ freeze préventif (résistance tient)")
                        else:
                            # Fallback: seuil fixe classique
                            should_freeze = (drop_pct >= FREEZE_DROP_PCT
                                             and time_since_peak <= FREEZE_WINDOW_H * 3600)
                            freeze_level = 1
                            freeze_drop_now = False
                            threshold_label = f"seuil fixe -{FREEZE_DROP_PCT*100:.0f}%"

                        if should_freeze:
                            s['frozen'] = True
                            self._buy_fill_times.clear()  # cleanup mémoires de cycles orphelins
                            s['freeze_price'] = price
                            s['freeze_time'] = time.time()
                            s['freeze_drop_pct'] = drop_pct
                            s['maf_freeze_level'] = freeze_level

                            # Niveau 2 (sell-only): retirer les BUY
                            if freeze_level >= 2:
                                lo = s.get('level_orders', {})
                                buy_removed = sum(1 for v in lo.values() if v == 'buy')
                                new_lo = {k: v for k, v in lo.items() if v != 'buy'}
                                s['level_orders'] = new_lo
                                s['usdt_free'] = round(s.get('usdt_free', 0) + s.get('usdt_locked', 0), 2)
                                s['usdt_locked'] = 0

                            if self.real_mode:
                                # MAF GEL: annuler uniquement les BUY — les SELL restent actifs
                                self._real_cancel_buys_only()
                            self.save_state(s)

                            freeze_icon = "⚡" if freeze_drop_now else "🧊"
                            level_tag = f" (level {freeze_level})" if freeze_level >= 2 else ""
                            self.tg_send(
                                f"{freeze_icon} MAF GEL{level_tag}\n"
                                f"Prix: ${price:.4f} ({drop_pct*100:.1f}% depuis pic)\n"
                                f"📊 {threshold_label}"
                            )
                            continue
                    else:
                        # 🧊 DÉGEL — conditions adaptatives avec MAF
                        freeze_low = s.get('freeze_price', price)
                        freeze_drop = s.get('freeze_drop_pct', 0.03)
                        freeze_time_sec = time.time() - s.get('freeze_time', 0)

                        # Timer de base (amplitude-dépendant, existant)
                        min_time = _get_unfreeze_min_time(freeze_drop)
                        min_time_min = (min_time // 60) if min_time else '—'

                        # Multiplicateur MAF du timer
                        if MAF_ENABLED and self._freeze_orch.momentum.is_ready:
                            timer_mult = self._freeze_orch.get_timer_multiplier()
                            min_time_adj = int(min_time * timer_mult) if min_time is not None else None
                        else:
                            timer_mult = 1.0
                            min_time_adj = min_time

                        # L2 (-8%+) → pas d'auto-dégel
                        if min_time is None and not s.get('manual_freeze', False):
                            pass  # skip auto-unfreeze
                        else:
                            bounce_pct = (price - freeze_low) / freeze_low if freeze_low > 0 else 0.0
                            # Track first time bounce threshold was reached
                            if bounce_pct >= FREEZE_UNFREEZE_BOUNCE and not s.get('manual_freeze', False):
                                first_bounce = s.get('first_bounce_time', 0)
                                if first_bounce == 0:
                                    s['first_bounce_time'] = time.time()
                                    self.save_state(s)
                                elif min_time_adj is not None and (time.time() - first_bounce) >= min_time_adj:
                                    s['frozen'] = False
                                    if MAF_ENABLED:
                                        self._freeze_orch.reset()
                                    s['manual_freeze'] = False
                                    s['first_bounce_time'] = 0
                                    s['peak_price'] = price
                                    s['peak_time'] = time.time()
                                    self.save_state(s)
                                    self.tg_send(
                                        f"🔥 Dégel +{bounce_pct*100:.1f}% "
                                        f"(confirmé {min_time_min}min×{timer_mult:.1f} — "
                                        f"amplitude -{freeze_drop*100:.0f}%)"
                                    )
                                    s = self.redeploy_grid(s, price)
                                    continue
                            else:
                                if s.get('first_bounce_time', 0) != 0:
                                    s['first_bounce_time'] = 0
                                    self.save_state(s)
                            self._price_history.append(price)
                            if len(self._price_history) > 10: self._price_history.pop(0)
                            if len(self._price_history) >= 5:
                                ph = self._price_history
                                total_price = sum(ph)
                                if total_price <= 0:
                                    continue
                                range_pct = (max(ph) - min(ph)) / total_price * len(ph)
                                if range_pct < FREEZE_UNFREEZE_RANGE and not s.get('manual_freeze', False):
                                    first_stable = s.get('first_stable_time', 0)
                                    if first_stable == 0:
                                        s['first_stable_time'] = time.time()
                                        self.save_state(s)
                                    elif min_time_adj is not None and (time.time() - first_stable) >= min_time_adj:
                                        s['frozen'] = False
                                        if MAF_ENABLED:
                                            self._freeze_orch.reset()
                                        s['manual_freeze'] = False
                                        s['first_stable_time'] = 0
                                        s['first_bounce_time'] = 0
                                        s['peak_price'] = price
                                        s['peak_time'] = time.time()
                                        self.save_state(s)
                                        self.tg_send(
                                            f"🔥 Dégel (range {range_pct*100:.1f}% — "
                                            f"confirmé {min_time_min}min×{timer_mult:.1f} — "
                                            f"amplitude -{freeze_drop*100:.0f}%)"
                                        )
                                        s = self.redeploy_grid(s, price)
                                        continue
                                else:
                                    if s.get('first_stable_time', 0) != 0:
                                        s['first_stable_time'] = 0
                                        self.save_state(s)

                        unfreeze_file = f"{BASE_DIR}/{self.name}.unfreeze"
                        if os.path.exists(unfreeze_file):
                            s['frozen'] = False
                            if MAF_ENABLED:
                                self._freeze_orch.reset()
                            s['manual_freeze'] = False
                            s['peak_price'] = price
                            s['peak_time'] = time.time()
                            self._price_history = [price]
                            self.save_state(s)
                            os.remove(unfreeze_file)
                            self.tg_send(f"🔓 DÉGEL MANUEL — {self.name}")
                            print(f"🔓 {self.name} — Dégel manuel")
                            # Redéploiement immédiat pour replacer les ordres
                            s = self.redeploy_grid(s, price)
                            continue

                    # Redéploiement — stratégie chirurgicale, pas de race condition
                    should, reason = self.should_redeploy(s, price)
                    if should and now - last_redeploy_time > 60:
                        last_redeploy_time = now
                        self.tg_send(f"🔁 Redéploiement | {reason} | ${price:.4f}")
                        s = self.redeploy_grid(s, price)
                        continue

                    # 🎯 Trailing Profit Lock — vendre si pic + pullback
                    if TRAILING_ENABLED:
                        s, trailing_sold = self._check_trailing_profit(s, price)

                    # 🛟 Capital Liberation — débloquer les positions bloquées
                    if CAPITAL_LIBERATION_ENABLED:
                        s, liberated = self._check_stuck_position(s, price)
                        if liberated:
                            continue

                    # Recycle
                    s, notifs = self.recycle(s, price)
                    for n in notifs:
                        self.log_fill(n)
                        if self.real_mode and n['type'] in ('SELL_FILL', 'BUY_FILL'):
                            cycle_n = n.get('cycle', s.get('total_cycles', '?'))
                            if n['type'] == 'SELL_FILL':
                                self.tg_send(f"✅ SELL #{cycle_n} @ ${n['sell']:.4f} × {n['qty']} = +${n['profit']:.2f}")
                                # Cycle COMPLETED
                                self.tg_send(f"🏁 Cycle #{cycle_n} COMPLETED | BUY {n['buy']:.4f} → SELL {n['sell']:.4f} | +${n['profit']:.2f}")
                            else:
                                self.tg_send(f"🔵 BUY #{cycle_n} @ ${n['buy']:.4f} × {n['qty']} → SELL @ ${n['sell']:.4f}")

                time.sleep(0.1)
            except KeyboardInterrupt:
                self.tg_send(f"👋 Arrêté")
                if self.real_mode:
                    self._real_cancel_all()
                break
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"⚠️ Erreur: {e} — réessai dans 10s")
                # Limiter les erreurs consécutives
                s['_consecutive_errors'] = s.get('_consecutive_errors', 0) + 1
                if s.get('_consecutive_errors', 0) > 10:
                    self.tg_send(f"🔥 Trop d'erreurs consécutives — arrêt")
                    if self.real_mode:
                        # Préserver les SELL même en cas d'arrêt sur erreur
                        self._real_cancel_buys_only()
                    break
                time.sleep(10)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', required=True, help='Ex: BTCUSDT')
    parser.add_argument('--capital', type=float, default=100.0, help='Capital en USDT (ignoré si --kelly)')
    parser.add_argument('--name', default='', help='Nom du bot')
    parser.add_argument('--real', action='store_true', help='Mode réel (place les ordres sur Binance)')
    parser.add_argument('--dryrun', action='store_true', help='Dry-run : simule les appels API sans exécuter')
    parser.add_argument('--kelly', action='store_true', help='Allocation Kelly au lieu du capital fixe')
    args = parser.parse_args()

    name = args.name or args.symbol.replace('USDT', '')
    bot = BotTrader(args.symbol, args.capital, name, args.real, args.dryrun, args.kelly)
    mode = "🌐 RÉEL" if args.real else "📄 Paper"
    if args.dryrun: mode += " (DRY-RUN)"
    if args.kelly:
        mode += " 📊 KELLY"
    print(f"🤖 {name} — {args.symbol} — ${bot.capital:.0f} — {mode}")
    bot.run()
