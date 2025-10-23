diff --git a/trader.py b/trader.py
index 0ab79b55f279be9f640343fefcb02402e7a2e3fb..9c8330219a5d98252375efe43f19a03fca9bf7c6 100644
--- a/trader.py
+++ b/trader.py
@@ -8,76 +8,134 @@ from typing import Dict, Any, Optional, Tuple
 
 import database
 import notifier
 import charting
 from utils import fetch_ohlcv_df
 
 # --- PARAMÈTRES ---
 RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
 LEVERAGE = int(os.getenv("LEVERAGE", "2"))
 TIMEFRAME = os.getenv("TIMEFRAME", "1h")
 TP_UPDATE_THRESHOLD_PERCENT = 0.05
 MIN_NOTIONAL_VALUE = 5.0
 MIN_RR = float(os.getenv("MIN_RR", "3.0"))
 ATR_TRAILING_MULTIPLIER = 2.0
 
 def _get_indicators(df: pd.DataFrame) -> Optional[pd.DataFrame]:
     """Calcule et ajoute tous les indicateurs techniques nécessaires."""
     if df is None or len(df) < 81:
         return None
     
     bb_20 = BollingerBands(close=df['close'], window=20, window_dev=2)
     df['bb20_up'], df['bb20_mid'], df['bb20_lo'] = bb_20.bollinger_hband(), bb_20.bollinger_mavg(), bb_20.bollinger_lband()
     
     bb_80 = BollingerBands(close=df['close'], window=80, window_dev=2)
     df['bb80_up'], df['bb80_mid'], df['bb80_lo'] = bb_80.bollinger_hband(), bb_80.bollinger_mavg(), bb_80.bollinger_lband()
-    
+
     df['atr'] = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14).average_true_range()
     return df
 
 def is_valid_reaction_candle(candle: pd.Series, side: str) -> bool:
     """Vérifie si la bougie de réaction est une bougie de décision valide."""
     body = abs(candle['close'] - candle['open'])
     if body == 0: return False
     
     wick_high = candle['high'] - max(candle['open'], candle['close'])
     wick_low = min(candle['open'], candle['close']) - candle['low']
     total_size = candle['high'] - candle['low']
 
     if body < total_size * 0.15:
         return False
 
     if side == 'buy':
         if candle['close'] <= candle['open']: return False
         if wick_high > body * 2.0: return False
             
     if side == 'sell':
         if candle['close'] >= candle['open']: return False
         if wick_low > body * 2.0: return False
             
     return True
 
+def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
+    """Analyse les dernières bougies pour détecter un signal de trading valide."""
+    if df is None or len(df) < 83:
+        return None
+
+    df_with_indicators = _get_indicators(df.copy())
+    if df_with_indicators is None:
+        return None
+
+    last_candle = df_with_indicators.iloc[-1]
+
+    for i in range(2, 4):
+        contact_candle = df_with_indicators.iloc[-i]
+        signal: Optional[Dict[str, Any]] = None
+
+        is_uptrend = contact_candle['close'] > contact_candle['bb80_mid']
+        is_downtrend = contact_candle['close'] < contact_candle['bb80_mid']
+
+        buy_tendance = is_uptrend and contact_candle['low'] <= contact_candle['bb20_lo']
+        buy_ct = contact_candle['low'] <= contact_candle['bb20_lo'] and contact_candle['low'] <= contact_candle['bb80_lo']
+
+        if buy_tendance or buy_ct:
+            if is_valid_reaction_candle(last_candle, 'buy'):
+                reintegration_ok = last_candle['close'] > contact_candle['bb20_lo']
+                if buy_ct:
+                    reintegration_ok = reintegration_ok and last_candle['close'] > contact_candle['bb80_lo']
+                if reintegration_ok:
+                    regime = "Tendance" if buy_tendance else "Contre-tendance"
+                    entry = (last_candle['open'] + last_candle['close']) / 2
+                    sl = contact_candle['low'] - (contact_candle['atr'] * 0.25)
+                    tp = last_candle['bb80_up'] if regime == 'Tendance' else last_candle['bb20_up']
+                    rr = (tp - entry) / (entry - sl) if (entry - sl) > 0 else 0
+                    if rr >= MIN_RR:
+                        signal = {"side": "buy", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}
+
+        sell_tendance = is_downtrend and contact_candle['high'] >= contact_candle['bb20_up']
+        sell_ct = contact_candle['high'] >= contact_candle['bb20_up'] and contact_candle['high'] >= contact_candle['bb80_up']
+
+        if signal is None and (sell_tendance or sell_ct):
+            if is_valid_reaction_candle(last_candle, 'sell'):
+                reintegration_ok = last_candle['close'] < contact_candle['bb20_up']
+                if sell_ct:
+                    reintegration_ok = reintegration_ok and last_candle['close'] < contact_candle['bb80_up']
+                if reintegration_ok:
+                    regime = "Tendance" if sell_tendance else "Contre-tendance"
+                    entry = (last_candle['open'] + last_candle['close']) / 2
+                    sl = contact_candle['high'] + (contact_candle['atr'] * 0.25)
+                    tp = last_candle['bb80_lo'] if regime == 'Tendance' else last_candle['bb20_lo']
+                    rr = (entry - tp) / (sl - entry) if (sl - entry) > 0 else 0
+                    if rr >= MIN_RR:
+                        signal = {"side": "sell", "regime": regime, "entry": entry, "sl": sl, "tp": tp, "rr": rr}
+
+        if signal:
+            signal['bb20_mid'] = last_candle['bb20_mid']
+            return signal
+
+    return None
+
 def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd.DataFrame, entry_price: float) -> Tuple[bool, str]:
     """Tente d'exécuter un trade avec toutes les vérifications de sécurité."""
     is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
     max_pos = int(database.get_setting('MAX_OPEN_POSITIONS', os.getenv('MAX_OPEN_POSITIONS', 3)))
     
     if len(database.get_open_positions()) >= max_pos: return False, f"Rejeté: Max positions ({max_pos}) atteint."
     if database.is_position_open(symbol): return False, "Rejeté: Position déjà ouverte (DB)."
     try:
         positions = ex.fetch_positions([symbol])
         if any(p for p in positions if p.get('contracts') and float(p['contracts']) > 0):
             return False, "Rejeté: Position déjà ouverte (vérifié sur l'exchange)."
     except Exception as e:
         return False, f"Rejeté: Erreur de vérification de position ({e})."
 
     balance = get_usdt_balance(ex)
     if balance is None: return False, "Rejeté: Erreur de solde (Clés API?)."
     if balance <= 10: return False, f"Rejeté: Solde insuffisant ({balance:.2f} USDT)."
     
     quantity = calculate_position_size(balance, RISK_PER_TRADE_PERCENT, entry_price, signal['sl'])
     if quantity <= 0: return False, f"Rejeté: Quantité calculée nulle ({quantity})."
     
     notional_value = quantity * entry_price
     if notional_value < MIN_NOTIONAL_VALUE: return False, f"Rejeté: Valeur du trade ({notional_value:.2f} USDT) < minimum requis ({MIN_NOTIONAL_VALUE} USDT)."
     
     final_entry_price = entry_price
@@ -85,51 +143,63 @@ def execute_trade(ex: ccxt.Exchange, symbol: str, signal: Dict[str, Any], df: pd
         try:
             ex.set_leverage(LEVERAGE, symbol)
             params = {'stopLoss': {'triggerPrice': signal['sl']}, 'takeProfit': {'triggerPrice': signal['tp']}, 'tradeSide': 'open'}
             order = ex.create_market_order(symbol, signal['side'], quantity, params=params)
             
             time.sleep(3)
             position = ex.fetch_position(symbol)
             if not position or float(position.get('stopLossPrice', 0)) == 0:
                 print("🚨 ALERTE SÉCURITÉ : SL non détecté ! Clôture d'urgence.")
                 ex.create_market_order(symbol, 'sell' if signal['side'] == 'buy' else 'buy', quantity, params={'reduceOnly': True})
                 return False, "ERREUR CRITIQUE: Stop Loss non placé. Position clôturée."
             
             if order and 'price' in order and order['price']:
                 final_entry_price = float(order['price'])
         except Exception as e:
             notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
             return False, f"Erreur d'exécution: {e}"
 
     signal['entry'] = final_entry_price
     
     current_strategy_mode = database.get_setting('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'NORMAL').upper())
     management_strategy = "NORMAL"
     if current_strategy_mode == 'SPLIT' and signal['regime'] == 'Contre-tendance':
         management_strategy = "SPLIT"
         
-    database.create_trade(symbol, signal['side'], signal['regime'], final_entry_price, signal['sl'], signal['tp'], quantity, RISK_PER_TRADE_PERCENT, int(time.time()), signal.get('bb20_mid'), management_strategy)
+    database.create_trade(
+        symbol,
+        signal['side'],
+        signal['regime'],
+        final_entry_price,
+        signal['sl'],
+        signal['tp'],
+        quantity,
+        RISK_PER_TRADE_PERCENT,
+        management_strategy,
+        signal.get('entry_atr', 0.0) or 0.0,
+        signal.get('entry_rsi', 0.0) or 0.0,
+    )
     
     chart_image = charting.generate_trade_chart(symbol, df, signal)
     mode_text = "PAPIER" if is_paper_mode else "RÉEL"
     trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, RISK_PER_TRADE_PERCENT)
     notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message, chat_id=notifier.TG_ALERTS_CHAT_ID or notifier.TG_CHAT_ID)
     
     return True, "Position ouverte avec succès."
 
 def manage_open_positions(ex: ccxt.Exchange):
     """Gère les positions ouvertes selon leur stratégie."""
     is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
     if is_paper_mode: return
 
     for pos in database.get_open_positions():
         df = _get_indicators(fetch_ohlcv_df(ex, pos['symbol'], TIMEFRAME))
         if df is None: continue
         
         last_indicators = df.iloc[-1]; is_long = pos['side'] == 'buy'
 
         if pos['management_strategy'] == 'SPLIT' and pos['breakeven_status'] == 'PENDING':
             try:
                 current_price = ex.fetch_ticker(pos['symbol'])['last']
                 management_trigger_price = last_indicators['bb20_mid']
                 if (is_long and current_price >= management_trigger_price) or (not is_long and current_price <= management_trigger_price):
                     
