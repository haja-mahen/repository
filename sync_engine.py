#!/usr/bin/env python3
"""
🔄 Sync Engine — Couche de synchronisation universelle
======================================================
Module indépendant, réutilisable par tous les bots.
Binance = source de vérité absolue.
State local = cache jetable, reconstructible.

Périmètre P1 :
  • place_and_verify()   — place + vérifie (openOrders|allOrders|myTrades)
  • rebuild_state()       — reconstruction complète depuis Binance (balances first)
  • StateMachine          — 6 états + transitions + ERROR_RECOVERY
  • clientOrderId         — unique par ordre
  • Log échecs            — JSON Lines
"""
import json, os, hmac, hashlib, time, requests
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass, field, asdict
from typing import Optional

# ─── Constantes ──────────────────────────────────────────
MAX_RETRIES = 3
RETRY_DELAY_SEC = 2
LOG_DIR = os.path.expanduser("~/.hermes/profiles/immo/scripts/logs")
ORDER_FAILURES_LOG = f"{LOG_DIR}/order_failures.jsonl"

# ─── Types ───────────────────────────────────────────────

@dataclass
class RebuildReport:
    """Rapport de rebuild_state()"""
    symbol: str
    timestamp: str
    state: str                    # WAIT_BUY | BUY_PENDING | LONG_OPEN | SELL_OPEN | ERROR_RECOVERY
    capital: float
    tokens_free: float
    tokens_locked: float
    usdt_free: float
    usdt_locked: float
    pru: float                    # Prix de revient moyen (0 si pas de position)
    level_orders: dict            # {price_str: side}
    source: str                   # "balances" | "balances+openOrders" | "balances+myTrades"
    confidence: str               # "HIGH" | "MEDIUM" | "LOW"
    details: dict = field(default_factory=dict)

    def print_report(self):
        c = self.confidence
        emoji = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🔴"}.get(c, "❓")
        print(f"  ┌─ rebuild_state() — {self.symbol}")
        print(f"  │ state:      {self.state}")
        print(f"  │ capital:    ${self.capital:.2f}")
        print(f"  │ tokens:     {self.tokens_free:.4f} free / {self.tokens_locked:.4f} locked")
        print(f"  │ USDT:       ${self.usdt_free:.2f} free / ${self.usdt_locked:.2f} locked")
        print(f"  │ PRU:        ${self.pru:.4f}")
        print(f"  │ source:     {self.source}")
        print(f"  │ confidence: {emoji} {c}")
        if self.details:
            for k, v in self.details.items():
                print(f"  │ {k}: {v}")
        print(f"  └────────────────────────────────────")


@dataclass
class OrderResult:
    """Résultat de place_and_verify()"""
    success: bool
    order_id: Optional[int] = None
    status: Optional[str] = None
    client_order_id: Optional[str] = None
    side: Optional[str] = None
    price: Optional[float] = None
    qty: Optional[float] = None
    error: Optional[str] = None
    attempts: list = field(default_factory=list)


# ─── API Binance ─────────────────────────────────────────

class BinanceAPI:
    """Wrapper minimal pour l'API REST Binance"""
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.base = "https://api.binance.com"

    def _sign(self, params: dict) -> dict:
        params['timestamp'] = int(time.time() * 1000)
        params = {k: v for k, v in sorted(params.items())}
        query = '&'.join(f"{k}={v}" for k, v in params.items())
        params['signature'] = hmac.new(
            self.secret_key.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        return params

    def _get(self, path: str, params: dict = None, signed: bool = False, timeout: int = 10):
        if params is None:
            params = {}
        if signed:
            params = self._sign(params)
        try:
            # Use raw query string to avoid urllib encoding differences
            query = '&'.join(f"{k}={v}" for k, v in params.items())
            url = f"{self.base}{path}?{query}"
            r = requests.get(url, headers={"X-MBX-APIKEY": self.api_key}, timeout=timeout)
            return r
        except requests.exceptions.Timeout:
            return None
        except requests.exceptions.ConnectionError:
            return None
        except Exception:
            return None

    def _post(self, path: str, params: dict, timeout: int = 10):
        params = self._sign(params)
        try:
            query = '&'.join(f"{k}={v}" for k, v in params.items())
            url = f"{self.base}{path}?{query}"
            r = requests.post(url, headers={"X-MBX-APIKEY": self.api_key}, timeout=timeout)
            return r
        except:
            return None

    def _delete(self, path: str, params: dict, timeout: int = 10):
        params = self._sign(params)
        try:
            query = '&'.join(f"{k}={v}" for k, v in params.items())
            url = f"{self.base}{path}?{query}"
            r = requests.delete(url, headers={"X-MBX-APIKEY": self.api_key}, timeout=timeout)
            return r
        except:
            return None

    def get_account(self):
        r = self._get('/api/v3/account', signed=True)
        if r and r.status_code == 200:
            return r.json()
        return None

    def get_open_orders(self, symbol: str):
        r = self._get('/api/v3/openOrders', {'symbol': symbol}, signed=True)
        if r and r.status_code == 200:
            return r.json()
        return None  # None = erreur, pas "pas d'ordres"

    def get_all_orders(self, symbol: str, limit: int = 50):
        r = self._get('/api/v3/allOrders', {'symbol': symbol, 'limit': limit}, signed=True)
        if r and r.status_code == 200:
            return r.json()
        return None

    def get_my_trades(self, symbol: str, limit: int = 100):
        r = self._get('/api/v3/myTrades', {'symbol': symbol, 'limit': limit}, signed=True)
        if r and r.status_code == 200:
            return r.json()
        return None

    def get_ticker(self, symbol: str):
        r = self._get('/api/v3/ticker/price', {'symbol': symbol})
        if r and r.status_code == 200:
            return float(r.json()['price'])
        return 0.0

    def place_order(self, symbol: str, side: str, qty: str, price: str,
                    client_order_id: str = None):
        params = {
            'symbol': symbol,
            'side': side.upper(),
            'type': 'LIMIT',
            'timeInForce': 'GTC',
            'quantity': qty,
            'price': price,
        }
        if client_order_id:
            params['newClientOrderId'] = client_order_id
        return self._post('/api/v3/order', params)

    def get_order(self, symbol: str, order_id: int):
        return self._get('/api/v3/order', {'symbol': symbol, 'orderId': order_id}, signed=True)

    def cancel_all(self, symbol: str):
        r = self._delete('/api/v3/openOrders', {'symbol': symbol})
        if r and r.status_code == 200:
            return r.json()
        return None

    def cancel_order(self, symbol: str, order_id: int):
        r = self._delete('/api/v3/order', {'symbol': symbol, 'orderId': order_id})
        if r and r.status_code == 200:
            return r.json()
        return None


# ─── Machine à états ─────────────────────────────────────

class StateMachine:
    """
    États :
      WAIT_BUY       — Capital libre, pas de position
      BUY_PENDING    — BUY envoyé, en attente de fill
      LONG_OPEN      — Tokens détenus, pas de SELL
      SELL_OPEN      — SELL sur Binance, en attente de fill
      COMPLETED      — Cycle terminé
      ERROR_RECOVERY — Échec irrécupérable, attend rebuild
    """
    STATES = ['WAIT_BUY', 'BUY_PENDING', 'LONG_OPEN', 'SELL_OPEN', 'COMPLETED', 'ERROR_RECOVERY']

    TRANSITIONS = {
        # (from, to) → autorisé
        ('WAIT_BUY', 'BUY_PENDING'): True,
        ('BUY_PENDING', 'LONG_OPEN'): True,
        ('BUY_PENDING', 'WAIT_BUY'): True,      # BUY canceled
        ('LONG_OPEN', 'SELL_OPEN'): True,
        ('LONG_OPEN', 'ERROR_RECOVERY'): True,  # 3 retries échoués
        ('SELL_OPEN', 'COMPLETED'): True,
        ('COMPLETED', 'WAIT_BUY'): True,
        ('ERROR_RECOVERY', 'WAIT_BUY'): True,
        ('ERROR_RECOVERY', 'LONG_OPEN'): True,
        ('ERROR_RECOVERY', 'BUY_PENDING'): True,
        ('ERROR_RECOVERY', 'SELL_OPEN'): True,
        # Tout vers ERROR_RECOVERY est autorisé
    }

    @staticmethod
    def is_valid_transition(from_state: str, to_state: str) -> bool:
        if from_state == to_state:
            return True
        # ERROR_RECOVERY est absorbant depuis n'importe où
        if to_state == 'ERROR_RECOVERY':
            return True
        return StateMachine.TRANSITIONS.get((from_state, to_state), False)

    @staticmethod
    def transition(current: str, target: str) -> str:
        if not StateMachine.is_valid_transition(current, target):
            raise ValueError(f"Transition interdite: {current} → {target}")
        return target

    @staticmethod
    def get_state_from_binance(api: BinanceAPI, symbol: str,
                                balance_asset: str, balance_quote: str = 'USDT',
                                tick_size: float = 0.0001) -> tuple:
        """
        Détermine l'état depuis Binance uniquement.
        Retourne (state, tokens_free, tokens_locked, usdt_free, usdt_locked, pru, source, confidence)
        """
        # 1. Soldes (source primaire)
        account = api.get_account()
        if not account:
            return ('ERROR_RECOVERY', 0, 0, 0, 0, 0, 'api_error', 'LOW')

        tokens_free = tokens_locked = 0.0
        usdt_free = usdt_locked = 0.0
        for b in account.get('balances', []):
            if b['asset'] == balance_asset:
                tokens_free = float(b['free'])
                tokens_locked = float(b['locked'])
            elif b['asset'] == balance_quote:
                usdt_free = float(b['free'])
                usdt_locked = float(b['locked'])

        # 2. Ordres ouverts
        open_orders = api.get_open_orders(symbol)
        if open_orders is None:
            open_orders = []  # Erreur API → on continue sans

        has_buy_order = any(o['side'].upper() == 'BUY' for o in open_orders)
        has_sell_order = any(o['side'].upper() == 'SELL' for o in open_orders)

        # 3. Trades pour PRU
        trades = api.get_my_trades(symbol)
        pru = 0.0
        source = 'balances'
        confidence = 'HIGH'

        # Balances first : si tokens > 0, la position EXISTE
        if tokens_free > 0 or tokens_locked > 0:
            if has_sell_order:
                state = 'SELL_OPEN'
                source = 'balances+openOrders'
            else:
                state = 'LONG_OPEN'
                # Chercher PRU dans myTrades
                if trades:
                    buys = sorted([t for t in trades if t['isBuyer']],
                                  key=lambda x: x['time'])
                    sells = sorted([t for t in trades if not t['isBuyer']],
                                   key=lambda x: x['time'])
                    # FIFO : matcher BUY → SELL en traçant le coût restant
                    buy_q = [(float(b['qty']), float(b['quoteQty'])) for b in buys]
                    for s in sells:
                        remaining = float(s['qty'])
                        while remaining > 0.000001 and buy_q:
                            bq, bcost = buy_q[0]
                            take = min(bq, remaining)
                            # Réduire la qty restante de ce BUY
                            remaining -= take
                            new_bq = round(bq - take, 8)
                            if new_bq < 0.000001:
                                buy_q.pop(0)
                            else:
                                # Ajuster proportionnellement le coût
                                new_cost = round(bcost * (bq - take) / bq, 8)
                                buy_q[0] = (new_bq, new_cost)
                    if buy_q:
                        total_cost = sum(c for _, c in buy_q)
                        total_qty = sum(q for q, _ in buy_q)
                        pru = total_cost / total_qty if total_qty > 0 else 0.0
                        source = 'balances+myTrades'
                    else:
                        # tokens > 0 mais tous les BUY matched → situation anormale
                        confidence = 'MEDIUM'
                        source = 'balances only'
                else:
                    confidence = 'MEDIUM'
                    source = 'balances only'
        elif usdt_locked > 0:
            if has_buy_order:
                state = 'BUY_PENDING'
                source = 'balances+openOrders'
            else:
                state = 'BUY_PENDING'
                source = 'balances only'
                confidence = 'MEDIUM'
        else:
            state = 'WAIT_BUY'
            source = 'balances'

        return (state, tokens_free, tokens_locked, usdt_free, usdt_locked, pru, source, confidence)


# ─── SyncEngine ──────────────────────────────────────────

class SyncEngine:
    """Moteur de synchronisation universel"""

    def __init__(self, api_key: str, secret_key: str, symbol: str,
                 bot_name: str, tick_size: float = 0.0001,
                 lot_step: float = 0.1, balance_asset: str = None):
        self.api = BinanceAPI(api_key, secret_key)
        self.symbol = symbol
        self.bot_name = bot_name
        self.tick_size = tick_size
        self.lot_step = lot_step
        self.balance_asset = balance_asset or symbol.replace('USDT', '')
        self.sm = StateMachine()

        # Assurer le dossier de logs
        os.makedirs(LOG_DIR, exist_ok=True)

    def _fmt_price(self, price: float) -> str:
        return f"{price:.{max(0, -int(Decimal(str(self.tick_size)).as_tuple().exponent))}f}"

    def _fmt_qty(self, qty: float) -> str:
        dec = max(0, -int(Decimal(str(self.lot_step)).as_tuple().exponent))
        return f"{qty:.{dec}f}"

    def _get_client_order_id(self, side: str) -> str:
        """Génère un clientOrderId unique"""
        ts = int(time.time() * 1000)
        return f"{self.bot_name}_{side}_{ts}"

    def _log_failure(self, entry: dict):
        """Journalise un échec d'ordre en JSON Lines"""
        os.makedirs(os.path.dirname(ORDER_FAILURES_LOG), exist_ok=True)
        with open(ORDER_FAILURES_LOG, 'a') as f:
            f.write(json.dumps(entry) + '\n')

    # ── rebuild_state() ──────────────────────────────────

    def rebuild_state(self, current_state: Optional[dict] = None) -> RebuildReport:
        """
        Reconstruit l'état depuis Binance.
        current_state : state local optionnel (pour enrichir mais ne prévaut jamais sur Binance)
        Retourne : RebuildReport
        """
        ts = datetime.now(timezone.utc).isoformat()
        price = self.api.get_ticker(self.symbol)

        (state, tokens_free, tokens_locked, usdt_free, usdt_locked,
         pru, source, confidence) = self.sm.get_state_from_binance(
            self.api, self.symbol, self.balance_asset, 'USDT', self.tick_size
        )

        # Capital
        capital = usdt_free + usdt_locked + (tokens_free + tokens_locked) * price

        # Level orders depuis openOrders
        open_orders = self.api.get_open_orders(self.symbol) or []
        level_orders = {}
        for o in open_orders:
            p = float(o['price'])
            level_orders[self._fmt_price(p)] = o['side'].lower()

        details = {}
        if current_state:
            diff = []
            if current_state.get('state') != state:
                diff.append(f"state: {current_state.get('state')} → {state}")
            if abs(current_state.get('tokens_free', 0) - tokens_free) > 0.01:
                diff.append(f"tokens: {current_state.get('tokens_free')} → {tokens_free}")
            if diff:
                details['divergences'] = diff

        details['balance_token'] = tokens_free + tokens_locked
        details['openOrders_count'] = len(open_orders)
        details['current_price'] = price
        details['reasoning'] = (
            f"Balances: {tokens_free + tokens_locked:.2f} {self.balance_asset}, "
            f"${usdt_free:.2f} USDT free / ${usdt_locked:.2f} locked. "
            f"Ordres: {len(open_orders)} ouvert(s). "
            f"{'myTrades utilisés' if 'myTrades' in source else 'myTrades non utilisés'}."
        )

        report = RebuildReport(
            symbol=self.symbol,
            timestamp=ts,
            state=state,
            capital=round(capital, 2),
            tokens_free=round(tokens_free, 4),
            tokens_locked=round(tokens_locked, 4),
            usdt_free=round(usdt_free, 2),
            usdt_locked=round(usdt_locked, 2),
            pru=round(pru, 4),
            level_orders=level_orders,
            source=source,
            confidence=confidence,
            details=details,
        )
        return report

    # ── place_and_verify() ───────────────────────────────

    def place_and_verify(self, side: str, qty: float, price: float,
                         dry_run: bool = False) -> OrderResult:
        """
        Place un ordre LIMIT, vérifie sa création.
        openOrders | allOrders | myTrades : 3 sources pour confirmer.
        Retries : MAX_RETRIES max → ERROR_RECOVERY.
        """
        cid = self._get_client_order_id(side)
        qty_str = self._fmt_qty(qty)
        price_str = self._fmt_price(price)

        result = OrderResult(
            success=False,
            client_order_id=cid,
            side=side,
            price=price,
            qty=qty,
        )

        if dry_run:
            result.success = True
            result.order_id = -1
            result.status = 'DRY_RUN'
            return result

        attempts = []
        final_error = None

        for attempt in range(1, MAX_RETRIES + 1):
            attempt_info = {
                'n': attempt,
                'method': 'POST',
                'response': None,
                'sources_checked': {'openOrders': False, 'allOrders': False, 'myTrades': False},
                'error': None,
            }

            # ÉTAPE 1 : POST l'ordre
            r = self.api.place_order(self.symbol, side, qty_str, price_str, cid)

            if r and r.status_code == 200:
                data = r.json()
                oid = data.get('orderId')
                status = data.get('status')
                # Vérifier que l'ordre n'est pas rejeté
                if oid and oid > 0 and status not in ('REJECTED', 'EXPIRED', -1):
                    result.success = True
                    result.order_id = oid
                    result.status = status
                    attempt_info['response'] = 'SUCCESS'
                    attempts.append(attempt_info)
                    result.attempts = attempts
                    return result
                elif status in ('REJECTED', 'EXPIRED'):
                    attempt_info['response'] = f'REJECTED: {data.get("code", "")}'
                    attempt_info['error'] = data.get('msg', '')
                    final_error = f"Ordre rejeté: {data.get('msg', '')}"
                    # Ne pas retry un rejet explicite
                    attempts.append(attempt_info)
                    result.attempts = attempts
                    result.error = final_error
                    return result

            # POST a échoué → vérifier maintenant si l'ordre existe malgré tout
            attempt_info['response'] = 'API_ERROR' if not r else 'INVALID_RESPONSE'

            # VÉRIFICATION 3 SOURCES
            # Source 1 : openOrders
            open_orders = self.api.get_open_orders(self.symbol)
            if open_orders:
                for o in open_orders:
                    if (o['side'].upper() == side.upper()
                            and self._fmt_price(float(o['price'])) == price_str
                            and abs(float(o['origQty']) - qty) < 0.01 * qty):
                        result.success = True
                        result.order_id = o['orderId']
                        result.status = o['status']
                        attempt_info['sources_checked']['openOrders'] = True
                        attempts.append(attempt_info)
                        result.attempts = attempts
                        return result

            # Source 2 : allOrders
            all_orders = self.api.get_all_orders(self.symbol, limit=20)
            if all_orders:
                for o in all_orders:
                    if (o.get('clientOrderId') == cid
                            or (o['side'].upper() == side.upper()
                                and self._fmt_price(float(o['price'])) == price_str
                                and abs(float(o['origQty']) - qty) < 0.01 * qty)):
                        result.success = True
                        result.order_id = o['orderId']
                        result.status = o['status']
                        attempt_info['sources_checked']['allOrders'] = True
                        attempts.append(attempt_info)
                        result.attempts = attempts
                        return result

            # Source 3 : myTrades (pour SELL déjà exécuté)
            trades = self.api.get_my_trades(self.symbol, limit=20)
            if trades and side.upper() == 'SELL':
                for t in trades:
                    if (not t['isBuyer']
                            and abs(float(t['price']) - price) / price < 0.01
                            and abs(float(t['qty']) - qty) < 0.05 * qty):
                        # SELL déjà exécuté (possible si marché rapide)
                        result.success = True  # Pas d'orderId mais le trade existe
                        result.status = 'FILLED'
                        attempt_info['sources_checked']['myTrades'] = True
                        attempts.append(attempt_info)
                        result.attempts = attempts
                        return result

            # Aucune source ne confirme → échec de cette tentative
            attempt_info['error'] = 'Non trouvé dans openOrders/allOrders/myTrades'
            attempts.append(attempt_info)

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SEC)

        # 3 retries échoués → ERROR_RECOVERY
        result.attempts = attempts
        result.error = f"3 retries échoués. Dernier: {final_error or 'ordre non confirmé'}"
        result.success = False

        # Journaliser l'échec
        self._log_failure({
            'ts': datetime.now(timezone.utc).isoformat(),
            'cid': cid,
            'symbol': self.symbol,
            'side': side,
            'qty': qty,
            'price': price,
            'retries': len(attempts),
            'final': 'ERROR_RECOVERY',
            'error': result.error,
        })

        return result

    # ── cancel_and_verify() ──────────────────────────────

    def cancel_and_verify(self, order_id: int) -> bool:
        """Annule un ordre et vérifie sa disparition des openOrders"""
        r = self.api.cancel_order(self.symbol, order_id)
        if not r or r.status_code != 200:
            return False
        # Vérifier que l'ordre a bien disparu
        time.sleep(0.5)
        open_orders = self.api.get_open_orders(self.symbol)
        if open_orders is None:
            return False  # Erreur API
        for o in open_orders:
            if o['orderId'] == order_id:
                return False  # Toujours présent
        return True
