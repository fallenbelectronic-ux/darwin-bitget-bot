# Fichier: trader.py
import os
import time
import ccxt
import pandas as pd
from ta.volatility import BollingerBands
from typing import Dict, Any, Optional

import database
import notifier
import charting
# CORRECTION: Importation depuis le nouveau fichier utils.py
from utils import fetch_ohlcv_df 

# --- PARAMÈTRES ---
PAPER_TRADING_MODE = os.getenv("PAPER_TRADING_MODE", "true").lower() in ("1", "true", "yes")
RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
LEVERAGE = int(os.getenv("LEVERAGE", "2"))
TIMEFRAME = os.getenv("TIMEFRAME", "1h")
TP_UPDATE_THRESHOLD_PERCENT = 0.05 

def _calculate_bb_mid(df: pd.DataFrame) -> Optional[float]:
    if df is None or len(df) < 20: return None
    bb = BollingerBands(close=df['close'], window=20, window_dev=2)
    return bb.bollinger_mavg().iloc[-1]

def manage_open_positions(ex: ccxt.Exchange):
    if PAPER_TRADING_MODE: return
    open_positions = database.get_open_positions()
    if not open_positions: return

    for pos in open_positions:
        symbol, trade_id, current_tp = pos['symbol'], pos['id'], pos['tp_price']
        try:
            df = fetch_ohlcv_df(ex, symbol, TIMEFRAME)
            if df is None: continue
            new_dynamic_tp = _calculate_bb_mid(df)
            if new_dynamic_tp is None: continue
            change_percent = (abs(new_dynamic_tp - current_tp) / current_tp) * 100
            if change_percent < TP_UPDATE_THRESHOLD_PERCENT: continue
            print(f"Mise à jour du TP requise pour {symbol} (Trade #{trade_id}): {current_tp:.5f} -> {new_dynamic_tp:.5f}")
            params = {'symbol': symbol, 'takeProfitPrice': f"{new_dynamic_tp:.5f}", 'stopLossPrice': f"{pos['sl_price']:.5f}"}
            ex.private_post_mix_position_modify_position(params)
            database.update_trade_tp(trade_id, new_dynamic_tp)
            notifier.tg_send(f"✅ TP pour <b>{symbol}</b> mis à jour dynamiquement à <code>{new_dynamic_tp:.5f}</code>.")
        except ccxt.NetworkError as e: print(f"Erreur réseau (gestion {symbol}): {e}")
        except ccxt.ExchangeError as e: print(f"Erreur d'exchange pour {symbol} (trade peut-être déjà fermé): {e}")
        except Exception as e:
            print(f"Erreur inattendue (gestion {symbol}): {e}")
            notifier.tg_send_error(f"Gestion de {symbol}", e)

def get_usdt_balance(ex: ccxt.Exchange) -> float:
    try:
        balance = ex.fetch_balance(params={'type': 'swap', 'code': 'USDT'})
        return float(balance['total'].get('USDT', 0.0))
    except Exception as e:
        notifier.tg_send_error("Récupération du solde", e)
        return 0.0

def calculate_position_size(balance: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    if balance <= 0 or entry_price == sl_price: return 0.0
    risk_amount_usdt = balance * (risk_percent / 100.0)
    price_diff_per_unit = abs(entry_price - sl_price)
    return risk_amount_usdt / price_diff_per_unit if price_diff_per_unit > 0 else 0.0

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame):
    max_pos = int(os.getenv('MAX_OPEN_POSITIONS', 3))
    if len(database.get_open_positions()) >= max_pos: return
    if database.is_position_open(symbol): return
    current_risk = RISK_PER_TRADE_PERCENT
    balance = get_usdt_balance(ex)
    if balance <= 10: return
    quantity = calculate_position_size(balance, current_risk, signal['entry'], signal['sl'])
    if quantity <= 0: return
    mode_text = "PAPIER" if PAPER_TRADING_MODE else "RÉEL"
    trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, current_risk)
    chart_image = charting.generate_trade_chart(symbol, df, signal)
    if not PAPER_TRADING_MODE:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            params = {'stopLoss': {'triggerPrice': signal['sl']}, 'takeProfit': {'triggerPrice': signal['tp']}}
            ex.create_market_order(symbol, signal['side'], quantity, params=params)
        except Exception as e:
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            return
    notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message)
    database.create_trade(symbol=symbol, side=signal['side'], regime=signal['regime'], status='OPEN', entry_price=signal['entry'], sl_price=signal['sl'], tp_price=signal['tp'], quantity=quantity, risk_percent=current_risk, open_timestamp=int(time.time()), bb20_mid_at_entry=signal.get('bb20_mid'))

def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    trade = database.get_trade_by_id(trade_id)
    if not trade or trade.get('status') != 'OPEN':
        return notifier.tg_send(f"Trade #{trade_id} déjà fermé ou invalide.")
    symbol, side, quantity = trade['symbol'], trade['side'], trade['quantity']
    try:
        if not PAPER_TRADING_MODE:
            close_side = 'sell' if side == 'buy' else 'buy'
            ex.create_market_order(symbol, close_side, quantity, params={'reduceOnly': True})
        database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
        notifier.tg_send(f"✅ Position sur {symbol} (Trade #{trade_id}) fermée manuellement.")
    except Exception as e:
        notifier.tg_send_error(f"Fermeture manuelle de {symbol}", e)
