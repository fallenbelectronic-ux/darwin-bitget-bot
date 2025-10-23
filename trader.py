# Fichier: trader.py
import os, time, ccxt
import pandas as pd
from typing import Dict, Any, Optional, Tuple

import database
import notifier
import charting
import utils

# --- PARAMÈTRES ---
RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
LEVERAGE = int(os.getenv("LEVERAGE", "2"))
TIMEFRAME = os.getenv("TIMEFRAME", "1h")
MIN_RR = float(os.getenv("MIN_RR", "3.0"))
TP_UPDATE_THRESHOLD = 0.05
MIN_NOTIONAL = 5.0
TICK_RATIO = 0.0005

def detect_signal(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Analyse les bougies pour détecter un signal DARWIN."""
    if df is None or len(df) < 83: return None
    last = df.iloc[-1]
    
    for i in range(2, 4):
        prev = df.iloc[-i]
        signal = None
        
        is_long = last['close'] > prev['bb80_mid']
        # Conditions de contact simplifiées pour la lisibilité
        touch_low = prev['low'] <= prev['bb20_lo']
        touch_high = prev['high'] >= prev['bb20_up']
        touch_dbl_low = touch_low and prev['low'] <= prev['bb80_lo']
        touch_dbl_high = touch_high and prev['high'] >= prev['bb80_up']

        # --- SCÉNARIOS ---
        if is_long and touch_low: # Tendance Long
             signal = build_signal("buy", "Tendance", last, prev)
        elif not is_long and touch_high: # Tendance Short
             signal = build_signal("sell", "Tendance", last, prev)
        elif touch_dbl_low: # Contre-Tendance Long
             signal = build_signal("buy", "Contre-tendance", last, prev)
        elif touch_dbl_high: # Contre-Tendance Short
             signal = build_signal("sell", "Contre-tendance", last, prev)

        if signal: return signal
    return None

def build_signal(side, regime, last, prev):
    """Fonction utilitaire pour construire l'objet signal."""
    tick = last['close'] * TICK_RATIO
    entry = last['close']
    
    if side == 'buy':
        sl = prev['low'] - (2 * tick)
        tp = last['bb80_up'] if regime == 'Tendance' else last['bb20_up']
        rr = (tp - entry) / (entry - sl)
    else:
        sl = prev['high'] + (2 * tick)
        tp = last['bb80_lo'] if regime == 'Tendance' else last['bb20_lo']
        rr = (entry - tp) / (sl - entry)
        
    if rr >= MIN_RR and entry > 0 and sl > 0:
         return {"side": side, "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}
    return None

def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict, df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
    """Exécute le trade après validation."""
    is_paper = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', 3))

    if len(database.get_open_positions()) >= max_pos: return False, "Max positions atteint."
    if database.is_position_open(symbol): return False, "Position déjà ouverte."

    balance = get_usdt_balance(ex)
    if balance <= 10: return False, "Solde insuffisant."
    
    quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, signal['sl'])
    if (quantity * entry_price) < MIN_NOTIONAL: return False, "Valeur trade trop faible."

    if not is_paper:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            # Ordre marché simple pour commencer, plus robuste sur Bitget
            ex.create_market_order(symbol, signal['side'], quantity)
            # Placement SL/TP séparés pour plus de sécurité
            sl_side = 'sell' if signal['side'] == 'buy' else 'buy'
            ex.create_order(symbol, 'limit', sl_side, quantity, signal['sl'], params={'stopPrice': signal['sl'], 'reduceOnly': True})
            ex.create_order(symbol, 'limit', sl_side, quantity, signal['tp'], params={'stopPrice': signal['tp'], 'reduceOnly': True})
        except Exception as e:
            notifier.tg_send_error(f"Erreur ordre {symbol}", e); return False, str(e)

    strat = "SPLIT" if (database.get_setting('STRATEGY_MODE') == 'SPLIT' and signal['regime'] == 'Contre-tendance') else "NORMAL"
    database.create_trade(symbol, signal['side'], signal['regime'], entry_price, signal['sl'], signal['tp'], quantity, RISK_PER_TRADE_PERCENT, strat)

    msg = notifier.format_trade_message(symbol, signal, quantity, "PAPIER" if is_paper else "RÉEL", RISK_PER_TRADE_PERCENT)
    chart = charting.generate_trade_chart(symbol, df, signal)
    notifier.tg_send_with_photo(chart, msg)
    return True, "OK"

def manage_open_positions(ex: ccxt.Exchange):
    """Gestion dynamique des positions (SPLIT, Breakeven, TP)."""
    if database.get_setting('PAPER_TRADING_MODE', 'true') == 'true': return

    for pos in database.get_open_positions():
        try:
            ticker = ex.fetch_ticker(pos['symbol'])
            curr_price = ticker['last']
            df = utils.fetch_and_prepare_df(ex, pos['symbol'], TIMEFRAME)
            if df is None: continue
            
            mm20 = df.iloc[-1]['bb20_mid']
            is_long = pos['side'] == 'buy'

            # Gestion SPLIT : Prise de profit à la MM20
            if pos['management_strategy'] == 'SPLIT' and pos['breakeven_status'] == 'PENDING':
                if (is_long and curr_price >= mm20) or (not is_long and curr_price <= mm20):
                     # ... (Logique de fermeture partielle ici) ...
                     pass

            # Mise à jour dynamique du TP
              else:
            new_dynamic_tp = last_indicators['bb80_mid'] if pos['regime'] == 'Tendance' else \
                             last_indicators['bb20_up'] if is_long else last_indicators['bb20_lo']
            if new_dynamic_tp and (abs(new_dynamic_tp - pos['tp_price']) / pos['tp_price']) * 100 >= TP_UPDATE_THRESHOLD_PERCENT:
                try:
                    print(f"Gestion Dynamique: Mise à jour du TP pour {pos['symbol']} -> {new_dynamic_tp:.5f}")
                    params = {'symbol': pos['symbol'], 'takeProfitPrice': f"{new_dynamic_tp:.5f}", 'stopLossPrice': f"{pos['sl_price']:.5f}"}
                    ex.private_post_mix_position_modify_position(params)
                    database.update_trade_tp(pos['id'], new_dynamic_tp)
                except Exception as e:
                    print(f"Erreur de mise à jour TP (Dynamique) pour {pos['symbol']}: {e}")
            
        except Exception as e:
             print(f"Erreur gestion {pos['symbol']}: {e}")

def get_usdt_balance(ex) -> float:
    try: return float(ex.fetch_balance({'type':'swap','code':'USDT'})['total']['USDT'])
    except: return 0.0

def calculate_position_size(bal, risk, entry, sl) -> float:
    if bal <= 0 or entry == sl: return 0.0
    return (bal * (risk/100)) / abs(entry - sl)

def close_position_manually(ex, trade_id):
    # ... (Logique inchangée, correcte) ...
    pass
