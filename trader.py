# Fichier: trader.py
import os
import time
import ccxt
import pandas as pd
from typing import Dict, Any, Optional, Tuple

import database
import notifier
import charting
import utils

# --- Paramètres de Trading ---
RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
LEVERAGE = int(os.getenv("LEVERAGE", "2"))
TIMEFRAME = os.getenv("TIMEFRAME", "1h")
MIN_RR = float(os.getenv("MIN_RR", "3.0"))
MM_DEAD_ZONE_PERCENT = float(os.getenv("MM_DEAD_ZONE_PERCENT", "0.1"))

# ==============================================================================
# ANALYSE DE LA BOUGIE (Nouvelle Section)
# ==============================================================================
def is_valid_reaction_candle(candle: pd.Series, side: str) -> bool:
    """Analyse la forme de la bougie de réaction pour valider le signal."""
    body = abs(candle['close'] - candle['open'])
    total_range = candle['high'] - candle['low']
    if total_range == 0 or body < total_range * 0.15: # Ignorer les Dojis / corps trop petits
        return False

    wick_high = candle['high'] - max(candle['open'], candle['close'])
    wick_low = min(candle['open'], candle['close']) - candle['low']

    if side == 'buy':
        # Bougie verte, petite mèche haute (pas un "pinbar inversé")
        return candle['close'] > candle['open'] and wick_high < body * 1.5
    
    if side == 'sell':
        # Bougie rouge, petite mèche basse
        return candle['close'] < candle['open'] and wick_low < body * 1.5
        
    return False

def detect_signal(df: pd.DataFrame, sym: str) -> Optional[Dict[str, Any]]:
    """Logique de détection complète avec les règles avancées."""
    if df is None or len(df) < 81: return None
    
    last, prev = df.iloc[-1], df.iloc[-2] # last=réaction, prev=contact

    # --- Filtre 0: Analyse de la bougie de réaction ---
    side_guess = 'buy' if last['close'] > last['open'] else 'sell'
    if not is_valid_reaction_candle(last, side_guess):
        return None

    # --- Filtre 1: Réintégration BB20 ---
    # Note: Vous devez avoir une fonction close_inside_bb20 dans utils.py
    if not utils.close_inside_bb20(last['close'], last['bb20_lo'], last['bb20_up']):
        return None
    
    # --- Filtre 2: Zone neutre MM80 ---
    dead_zone = last['bb80_mid'] * (MM_DEAD_ZONE_PERCENT / 100.0)
    if abs(last['close'] - last['bb80_mid']) < dead_zone:
        return None

    signal = None
    
    # --- Détection des Patterns ---
    is_above_mm80 = last['close'] > last['bb80_mid']
    # Note: Vous devez avoir une fonction touched_or_crossed dans utils.py
    touched_bb20_low = utils.touched_or_crossed(prev['low'], prev['high'], prev['bb20_lo'], "buy")
    touched_bb20_high = utils.touched_or_crossed(prev['low'], prev['high'], prev['bb20_up'], "sell")

    # Pattern 1: Tendance (Extrême Correction)
    if is_above_mm80 and touched_bb20_low:
        regime = "Tendance"
        entry = last['close']
        sl = prev['low'] - (prev['atr'] * 0.25)
        tp = last['bb80_up']
        if (entry - sl) > 0:
            rr = (tp - entry) / (entry - sl)
            if rr >= MIN_RR:
                signal = {"side": "buy", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}
                
    elif not is_above_mm80 and touched_bb20_high:
        regime = "Tendance"
        entry = last['close']
        sl = prev['high'] + (prev['atr'] * 0.25)
        tp = last['bb80_lo']
        if (sl - entry) > 0:
            rr = (entry - tp) / (sl - entry)
            if rr >= MIN_RR:
                signal = {"side": "sell", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}

    # Pattern 2: Contre-Tendance (Double Extrême)
    if not signal:
        touched_double_low = prev['low'] <= min(prev['bb20_lo'], prev['bb80_lo'])
        touched_double_high = prev['high'] >= max(prev['bb20_up'], prev['bb80_up'])

        if touched_double_low:
            regime = "Contre-tendance"
            entry = last['close']
            sl = prev['low'] - (prev['atr'] * 0.25)
            tp = last['bb20_mid']
            if (entry - sl) > 0:
                rr = (tp - entry) / (entry - sl)
                if rr >= MIN_RR:
                    signal = {"side": "buy", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}
        elif touched_double_high:
            regime = "Contre-tendance"
            entry = last['close']
            sl = prev['high'] + (prev['atr'] * 0.25)
            tp = last['bb20_mid']
            if (sl - entry) > 0:
                rr = (entry - tp) / (sl - entry)
                if rr >= MIN_RR:
                    signal = {"side": "sell", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}
    
    if signal:
        signal['bb20_mid'] = last['bb20_mid']
        signal['entry_atr'] = prev.get('atr', 0.0) # Utiliser .get pour plus de sécurité
        signal['entry_rsi'] = 0.0 # Placeholder
        return signal
        
    return None

# ==============================================================================
# LOGIQUE D'EXÉCUTION (Améliorée)
# ==============================================================================
def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
    """Tente d'exécuter un trade avec toutes les vérifications de sécurité."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('MAX_OPEN_POSITIONS', 3)))

    if len(database.get_open_positions()) >= max_pos:
        return False, f"Rejeté: Max positions ({max_pos}) atteint."
    if database.is_position_open(symbol):
        return False, "Rejeté: Position déjà ouverte (DB)."
    
    balance = get_usdt_balance(ex)
    if balance is None or balance <= 10:
        return False, f"Rejeté: Solde insuffisant ({balance or 0:.2f} USDT) ou erreur API."
    
    quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, signal['sl'])
    if quantity <= 0:
        return False, f"Rejeté: Quantité calculée nulle ({quantity})."
        
    notional_value = quantity * entry_price
    if notional_value < MIN_NOTIONAL_VALUE:
        return False, f"Rejeté: Valeur du trade ({notional_value:.2f} USDT) < min requis ({MIN_NOTIONAL_VALUE} USDT)."
    
    final_entry_price = entry_price
    if not is_paper_mode:
        try:
            ex.set_leverage(LEVERAGE, symbol)
            params = {'stopLoss': {'triggerPrice': signal['sl']}, 'takeProfit': {'triggerPrice': signal['tp']}}
            order = ex.create_market_order(symbol, signal['side'], quantity, params=params)
            
            time.sleep(3)
            position = ex.fetch_position(symbol)
            if not position or float(position.get('stopLossPrice', 0)) == 0:
                print("🚨 ALERTE SÉCURITÉ : SL non détecté ! Clôture d'urgence.")
                ex.create_market_order(symbol, 'sell' if signal['side'] == 'buy' else 'buy', quantity, params={'reduceOnly': True})
                return False, "ERREUR CRITIQUE: Stop Loss non placé. Position clôturée."
            
            if order and order.get('price'):
                final_entry_price = float(order['price'])

        except Exception as e:
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            return False, f"Erreur d'exécution: {e}"

    signal['entry'] = final_entry_price
    
    management_strategy = "NORMAL"
    if database.get_setting('STRATEGY_MODE', 'NORMAL').upper() == 'SPLIT' and signal['regime'] == 'Contre-tendance':
        management_strategy = "SPLIT"
        
    database.create_trade(
        symbol=symbol,
        side=signal['side'],
        regime=signal['regime'],
        entry_price=final_entry_price,
        sl_price=signal['sl'],
        tp_price=signal['tp'],
        quantity=quantity,
        risk_percent=RISK_PER_TRADE_PERCENT,
        management_strategy=management_strategy,
        entry_atr=signal.get('entry_atr', 0.0) or 0.0,
        entry_rsi=signal.get('entry_rsi', 0.0) or 0.0,
    )
    
    chart_image = charting.generate_trade_chart(symbol, df, signal)
    mode_text = "PAPIER" if is_paper_mode else "RÉEL"
    trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, RISK_PER_TRADE_PERCENT)
    notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message)
    
    return True,"Position ouverte avec succès."

def manage_open_positions(ex: ccxt.Exchange):
    # (Votre logique avancée de gestion SPLIT et TP Dynamique sera implémentée ici)
    # Pour l'instant, on se concentre sur la qualité de l'entrée.
    pass

def get_usdt_balance(ex: ccxt.Exchange) -> Optional[float]:
    """Récupère le solde USDT."""
    try:
        ex.options['recvWindow'] = 10000
        balance = ex.fetch_balance(params={'type': 'swap', 'code': 'USDT'})
        return float(balance['total'].get('USDT', 0.0))
    except Exception as e:
        notifier.tg_send_error("Récupération du solde", e)
        return None

def calculate_position_size(balance: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    """Calcule la quantité d'actifs à trader."""
    if balance <= 0 or entry_price == sl_price: return 0.0
    risk_amount_usdt = balance * (risk_percent / 100.0)
    price_diff_per_unit = abs(entry_price - sl_price)
    return risk_amount_usdt / price_diff_per_unit if price_diff_per_unit > 0 else 0.0

def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    """Clôture manuellement une position."""
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    trade = database.get_trade_by_id(trade_id)
    if not trade or trade.get('status') != 'OPEN':
        return notifier.tg_send(f"Trade #{trade_id} déjà fermé ou invalide.")
    
    try:
        if not is_paper_mode:
            ex.create_market_order(trade['symbol'], 'sell' if trade['side'] == 'buy' else 'buy', trade['quantity'], params={'reduceOnly': True})
        
        # Calcule un PNL approximatif, mais l'idéal serait d'avoir le prix de sortie réel
        database.close_trade(trade_id, status='CLOSED_MANUAL', exit_price=trade['entry_price'])
        notifier.tg_send(f"✅ Position sur {trade['symbol']} (Trade #{trade_id}) fermée manuellement.")
    except Exception as e:
        notifier.tg_send_error(f"Fermeture manuelle de {trade['symbol']}", e)
