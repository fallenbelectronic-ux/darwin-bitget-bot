import os
import ccxt
from typing import Dict, Any

import database
import notifier

PAPER_TRADING_MODE = os.getenv("PAPER_TRADING_MODE", "true").lower() in ("1", "true", "yes")
RISK_PER_TRADE_PERCENT = 1.0
LEVERAGE = 2

def get_usdt_balance(ex: ccxt.Exchange) -> float:
    try:
        return float(ex.fetch_balance()['total'].get('USDT', 0.0))
    except Exception as e:
        notifier.tg_send_error("Récupération du solde", e)
        return 0.0

def calculate_position_size(balance: float, entry_price: float, sl_price: float) -> float:
    if balance <= 0 or entry_price == sl_price: return 0.0
    risk_amount_usdt = balance * (RISK_PER_TRADE_PERCENT / 100.0)
    price_diff_per_unit = abs(entry_price - sl_price)
    return risk_amount_usdt / price_diff_per_unit

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any]) -> bool:
    if database.is_position_open(symbol): return False
    balance = get_usdt_balance(ex)
    if balance <= 10: return False
    quantity = calculate_position_size(balance, signal['entry'], signal['sl'])
    if quantity <= 0: return False

    mode_text = "PAPIER" if PAPER_TRADING_MODE else "RÉEL"
    
    # Formater le message AVANT d'exécuter l'ordre
    trade_message = notifier.tg_format_trade(symbol, signal, quantity, mode_text)

    if not PAPER_TRADING_MODE:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            params = {'stopLoss': {'triggerPrice': signal['sl']}, 'takeProfit': {'triggerPrice': signal['tp']}}
            ex.create_market_order(symbol, signal['side'], quantity, params=params)
        except Exception as e:
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            return False

    # Envoyer la notification et enregistrer en DB
    notifier.tg_send(trade_message)
    database.create_trade(
        symbol=symbol, side=signal['side'], regime=signal['regime'],
        entry_price=signal['entry'], sl_price=signal['sl'], tp_price=signal['tp'],
        quantity=quantity, bb20_mid_at_entry=signal.get('bb20_mid')
    )
    return True

def manage_open_positions(ex: ccxt.Exchange):
    # La logique de gestion (BE, etc.) reste ici
    pass
