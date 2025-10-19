import os
import ccxt
from typing import Dict, Any

import database
import notifier

# ==============================================================================
# VARIABLE DE MODE DE TRADING
# ==============================================================================
# Ce paramètre active ou désactive le trading réel.
# Si True, aucun ordre n'est envoyé à l'exchange.
PAPER_TRADING_MODE = os.getenv("PAPER_TRADING_MODE", "true").lower() in ("1", "true", "yes")

# --- Constantes (inchangées) ---
RISK_PER_TRADE_PERCENT = 1.0
LEVERAGE = 2

# ... (les fonctions get_usdt_balance et calculate_position_size restent identiques) ...
def get_usdt_balance(ex: ccxt.Exchange) -> float:
    try:
        return float(ex.fetch_balance()['total'].get('USDT', 0.0))
    except Exception as e:
        notifier.tg_send(f"⚠️ Erreur de solde: {e}")
        return 0.0

def calculate_position_size(balance: float, entry_price: float, sl_price: float) -> float:
    if balance <= 0 or entry_price == sl_price: return 0.0
    risk_amount_usdt = balance * (RISK_PER_TRADE_PERCENT / 100.0)
    price_diff_per_unit = abs(entry_price - sl_price)
    return risk_amount_usdt / price_diff_per_unit
# ==============================================================================

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any]) -> bool:
    """
    Fonction principale pour ouvrir une position (réelle ou simulée).
    """
    if database.is_position_open(symbol):
        return False

    balance = get_usdt_balance(ex)
    if balance <= 10:
        return False

    quantity = calculate_position_size(balance, signal['entry'], signal['sl'])
    if quantity == 0.0:
        return False

    # --- LOGIQUE DE COMMUTATION PAPIER/RÉEL ---
    side = signal['side']
    mode_text = "MODE PAPIER" if PAPER_TRADING_MODE else "MODE RÉEL"

    if not PAPER_TRADING_MODE:
        # --- BLOC DE TRADING RÉEL ---
        try:
            ex.set_leverage(LEVERAGE, symbol)
            params = {
                'stopLoss': {'triggerPrice': signal['sl']},
                'takeProfit': {'triggerPrice': signal['tp']}
            }
            order = ex.create_market_order(symbol, side, quantity, params=params)
            print(f"ORDRE RÉEL EXÉCUTÉ: {order}")
            notifier.tg_send(f"✅ [{mode_text}] Ordre ouvert sur {symbol} | {side.upper()} | Qte: {quantity:.4f}")
        except Exception as e:
            print(f"ERREUR D'ORDRE sur {symbol}: {e}")
            notifier.tg_send(f"❌ [{mode_text}] Erreur d'ordre sur {symbol}: {e}")
            return False
    else:
        # --- BLOC DE TRADING PAPIER ---
        print(f"[{symbol}] SIMULATION D'ORDRE (Paper Mode): {side.upper()} Qte: {quantity:.4f}")
        notifier.tg_send(f"📝 [{mode_text}] Ordre simulé sur {symbol} | {side.upper()} | Qte: {quantity:.4f}")

    # L'enregistrement en DB se fait dans les DEUX modes pour le suivi
    database.create_trade(
        symbol=symbol,
        side=side,
        regime=signal['regime'],
        entry_price=signal['entry'],
        sl_price=signal['sl'],
        tp_price=signal['tp'],
        quantity=quantity,
        bb20_mid_at_entry=signal.get('bb20_mid')
    )
    return True

# La fonction manage_open_positions reste la même pour l'instant
def manage_open_positions(ex: ccxt.Exchange):
    pass
