# Fichier: trader.py
import os
import time
import ccxt
import pandas as pd
from typing import Dict, Any, Optional

import database
import notifier
import charting
# CORRECTION: Le nom de la fonction importée est maintenant 'fetch_and_prepare_df'
import utils

PAPER_TRADING_MODE = os.getenv("PAPER_TRADING_MODE", "true").lower() in ("1", "true", "yes")
RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
LEVERAGE = int(os.getenv("LEVERAGE", "2"))
TIMEFRAME = os.getenv("TIMEFRAME", "1h")

def manage_open_positions(ex: ccxt.Exchange):
    # Logique à implémenter pour le BE ou le TP dynamique
    pass

def get_usdt_balance(ex: ccxt.Exchange) -> float:
    try:
        balance = ex.fetch_balance(params={'type': 'swap', 'code': 'USDT'})
        return float(balance['total'].get('USDT', 0.0))
    except Exception as e:
        notifier.tg_send_error("Récupération du solde", e)
        return 0.0

def calculate_position_size(balance: float, risk: float, entry: float, sl: float) -> float:
    if balance <= 0 or entry == sl: return 0.0
    risk_amount = balance * (risk / 100.0)
    price_diff = abs(entry - sl)
    return risk_amount / price_diff if price_diff > 0 else 0.0

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict, df: pd.DataFrame):
    if len(database.get_open_positions()) >= int(os.getenv('MAX_OPEN_POSITIONS', 3)):
        return
    if database.is_position_open(symbol):
        return
    
    balance = get_usdt_balance(ex)
    if balance <= 10:
        return
    
    quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, signal['entry'], signal['sl'])
    if quantity <= 0:
        return
    
    mode = "PAPIER" if PAPER_TRADING_MODE else "RÉEL"
    msg = notifier.format_trade_message(symbol, signal, quantity, mode, RISK_PER_TRADE_PERCENT)
    chart = charting.generate_trade_chart(symbol, df, signal)

    if not PAPER_TRADING_MODE:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            params = {'stopLoss': {'triggerPrice': signal['sl']}, 'takeProfit': {'triggerPrice': signal['tp']}}
            ex.create_market_order(symbol, signal['side'], quantity, params=params)
        except Exception as e:
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            return
            
    notifier.tg_send_with_photo(chart, msg)
    database.create_trade(
        symbol=symbol, side=signal['side'], regime=signal['regime'], status='OPEN',
        entry_price=signal['entry'], sl_price=signal['sl'], tp_price=signal['tp'],
        quantity=quantity, risk_percent=RISK_PER_TRADE_PERCENT, open_timestamp=int(time.time()),
        bb20_mid_at_entry=signal.get('bb20_mid')
    )

def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    trade = database.get_trade_by_id(trade_id)
    if not trade or trade.get('status') != 'OPEN':
        return notifier.tg_send(f"Trade #{trade_id} déjà fermé ou invalide.")
        
    symbol, side, quantity = trade['symbol'], trade['side'], trade['quantity']
    try:
        if not PAPER_TRADING_MODE:
            ex.create_market_order(symbol, 'sell' if side == 'buy' else 'buy', quantity, params={'reduceOnly': True})
        
        database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
        notifier.tg_send(f"✅ Position sur {symbol} (Trade #{trade_id}) fermée manuellement.")
    except Exception as e:
        notifier.tg_send_error(f"Fermeture manuelle de {symbol}", e)
