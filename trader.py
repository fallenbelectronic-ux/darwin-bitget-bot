# Fichier: trader.py
import os
import time
import ccxt
import pandas as pd
from typing import Dict, Any, Optional, Tuple, List
import database
import notifier
import charting
import utils

# --- Paramètres de Trading ---
try:
    RISK_PER_TRADE_PERCENT = float(database.get_setting('RISK_PER_TRADE_PERCENT', os.getenv("RISK_PER_TRADE_PERCENT", "1.0")))
except Exception:
    RISK_PER_TRADE_PERCENT = float(os.getenv("RISK_PER_TRADE_PERCENT", "1.0"))
LEVERAGE = int(os.getenv("LEVERAGE", "2"))
TIMEFRAME = os.getenv("TIMEFRAME", "1h")
MIN_RR = float(os.getenv("MIN_RR", "3.0"))
MM_DEAD_ZONE_PERCENT = float(os.getenv("MM_DEAD_ZONE_PERCENT", "0.1"))
MIN_NOTIONAL_VALUE = float(os.getenv("MIN_NOTIONAL_VALUE", "5"))

# --- Filtres de réaction (paternes) ---
REACTION_WINDOW_BARS = 3
PINBAR_MAX_BODY = 0.30      # ≤ 30% du range
IMPULSE_MIN_BODY = 0.35     # ≥ 35% du range
SIMPLE_WICK_MIN = 0.35      # ≥ 35% du range

# Confirmation obligatoire après une pinbar-contact
PINBAR_CONFIRM_MAX_BARS = 2     # nb de bougies max pour voir la réaction 
PINBAR_CONFIRM_MIN_BODY = 0.20  # corps min de la bougie de réaction

# --- Frais & BE ---
FEE_ENTRY_PCT   = float(os.getenv("FEE_ENTRY_PCT", "0.0010"))  # 0.1% typique taker
FEE_EXIT_PCT    = float(os.getenv("FEE_EXIT_PCT",  "0.0010"))  # 0.1% typique taker
BE_BUFFER_PCT   = float(os.getenv("BE_BUFFER_PCT", "0.0020"))  # +0.2% au-dessus du VRAI BE
BE_BUFFER_USDT  = float(os.getenv("BE_BUFFER_USDT","0.0"))     # buffer absolu optionnel (USDT). Laisse 0 si tu n’en veux pas.

# ==============================================================================
# ANALYSE DE LA BOUGIE (Nouvelle Section)
# ==============================================================================

def create_exchange():
    ex = ccxt.bitget({
        'apiKey': os.getenv('BITGET_API_KEY'),
        'secret': os.getenv('BITGET_API_SECRET'),
        'password': os.getenv('BITGET_PASSPHRASSE'),
        'options': {
            'defaultType': 'swap',
            'defaultSubType': 'linear',
        },
        'timeout': 15000,  # 15 secondes
        'enableRateLimit': True,  # ← IMPORTANT
    })
    return ex

def get_universe_size() -> int:
    """
    Lit UNIVERSE_SIZE depuis la base (fallback sur l'env, défaut 500).
    À appeler à chaque itération de scan pour prise en compte immédiate.
    """
    try:
        val = database.get_setting('UNIVERSE_SIZE', os.getenv("UNIVERSE_SIZE", "500"))
        return max(1, int(val))
    except Exception:
        try:
            return max(1, int(os.getenv("UNIVERSE_SIZE", "500")))
        except Exception:
            return 500

def is_tradeable_symbol(ex, symbol: str) -> bool:
    """
    Filtre minimaliste et robuste.
    
    CRITÈRES :
    1. Dans l'univers Top market cap (déjà filtré en amont)
    2. OU dans whitelist manuelle
    3. Spread bid/ask acceptable (< 0.2%)
    
    PAS de check volume (manipulable, instable).
    
    Args:
        ex: Exchange
        symbol: Paire à vérifier (ex: 'SOL/USDT:USDT')
    
    Returns:
        True si tradeable
    """
    try:
        base = symbol.split('/')[0].upper()
        
        # ====== WHITELIST (bypass tous les checks) ======
        try:
            whitelist_str = database.get_setting('SYMBOL_WHITELIST', '')
            if whitelist_str:
                whitelist = [s.strip().upper() for s in whitelist_str.split(',')]
                if base in whitelist:
                    print(f"✅ {base} dans whitelist")
                    return True
        except Exception:
            pass
        
        # ====== SPREAD BID/ASK (indicateur liquidité RÉEL) ======
        try:
            ticker = ex.fetch_ticker(symbol)
            
            bid = float(ticker.get('bid', 0))
            ask = float(ticker.get('ask', 0))
            
            if bid <= 0 or ask <= 0:
                print(f"❌ {base} : Pas de bid/ask valide")
                return False
            
            spread_pct = ((ask - bid) / bid) * 100
            
            # Seuil configurable
            try:
                max_spread = float(database.get_setting('MAX_SPREAD_PCT', '0.2'))
            except Exception:
                max_spread = 0.2  # 0.2% par défaut
            
            if spread_pct > max_spread:
                print(f"❌ {base} spread trop large : {spread_pct:.3f}% > {max_spread}%")
                return False
            
            print(f"✅ {base} spread OK : {spread_pct:.3f}%")
            return True
        
        except Exception as e:
            print(f"❌ {base} erreur spread check : {e}")
            return False
    
    except Exception as e:
        print(f"❌ Erreur is_tradeable_symbol : {e}")
        return False

def check_correlation_risk(ex, new_symbol: str, new_side: str) -> bool:
    """
    Évite sur-exposition même sur paires "décorrélées".
    
    RÈGLE PRO :
    - Max 3 positions LONG en même temps (toutes paires confondues)
    - Max 3 positions SHORT en même temps
    - Max 2 dans le même secteur (L1, DeFi, Meme...)
    
    POURQUOI ?
    - Lors d'un crash BTC -10%, TOUT dump ensemble
    - SOL "décorrélée" peut dumper -15% quand même
    - 3 LONGS = 3x l'exposition au risque systémique
    
    Args:
        ex: Exchange
        new_symbol: Symbole du nouveau trade
        new_side: 'buy' ou 'sell'
    
    Returns:
        True si risque acceptable, False si rejet
    """
    try:
        open_positions = database.get_open_positions()
        
        # ====== LIMITE GLOBALE PAR DIRECTION ======
        same_direction_count = sum(
            1 for pos in open_positions 
            if pos.get('side') == new_side
        )
        
        try:
            max_same_direction = int(database.get_setting('MAX_SAME_DIRECTION', '3'))
        except Exception:
            max_same_direction = 3
        
        if same_direction_count >= max_same_direction:
            notifier.tg_send(
                f"⚠️ Trade {new_symbol} {new_side.upper()} rejeté\n"
                f"Déjà {same_direction_count} positions {new_side.upper()} ouvertes\n"
                f"Max autorisé : {max_same_direction}\n"
                f"➡️ Risque systémique trop élevé"
            )
            return False
        
        # ====== LIMITE PAR SECTEUR (bonus) ======
        correlated_groups = {
            'L1_ALTS': ['SOL', 'AVAX', 'NEAR', 'FTM', 'ATOM', 'DOT', 'ADA', 'ALGO', 'TIA'],
            'DEFI': ['UNI', 'AAVE', 'SNX', 'COMP', 'MKR', 'CRV', 'SUSHI', 'BAL', 'YFI'],
            'MEME': ['DOGE', 'SHIB', 'PEPE', 'FLOKI', 'WIF', 'BONK'],
            'GAMING': ['AXS', 'SAND', 'MANA', 'ENJ', 'GALA', 'IMX', 'BEAM'],
            'LAYER2': ['ARB', 'OP', 'MATIC', 'LRC', 'METIS', 'STRK'],
            'AI': ['FET', 'AGIX', 'RNDR', 'GRT', 'OCEAN'],
        }
        
        new_base = new_symbol.split('/')[0].upper()
        
        # Trouver groupe du nouveau trade
        new_group = None
        for group_name, symbols in correlated_groups.items():
            if new_base in symbols:
                new_group = group_name
                break
        
        if new_group:
            # Compter positions dans le même groupe + même direction
            same_sector_count = sum(
                1 for pos in open_positions
                if pos.get('side') == new_side and 
                   pos.get('symbol', '').split('/')[0].upper() in correlated_groups[new_group]
            )
            
            try:
                max_per_sector = int(database.get_setting('MAX_PER_SECTOR', '2'))
            except Exception:
                max_per_sector = 2
            
            if same_sector_count >= max_per_sector:
                notifier.tg_send(
                    f"⚠️ Trade {new_symbol} rejeté\n"
                    f"Déjà {same_sector_count} positions dans secteur {new_group}\n"
                    f"➡️ Diversification insuffisante"
                )
                return False
        
        return True
    
    except Exception as e:
        print(f"Erreur check_correlation_risk: {e}")
        return True

def get_universe_by_market_cap(ex: ccxt.Exchange, size: int) -> List[str]:
    """
    Retourne la liste des paires futures USDT Bitget (format CCXT) triées par market cap (CoinGecko),
    sans limite artificielle à 100. Supporte jusqu'à 500 via pagination (250/par page).
    On retourne strictement les 'size' premières paires disponibles sur Bitget.

    Cache léger en RAM pour la journée courante + taille demandée.
    """
    import time
    import requests

    # --- cache process-local (clé = (jour_utc, size)) ---
    now_day = time.gmtime().tm_yday
    key = (now_day, int(size))
    if not hasattr(get_universe_by_market_cap, "_cache"):
        get_universe_by_market_cap._cache = {}
    cache = get_universe_by_market_cap._cache
    if key in cache:
        return cache[key][:size]

    # Charger les marchés Bitget une fois
    try:
        ex.load_markets()
    except Exception:
        pass
    markets = getattr(ex, "markets", {}) or {}
    symbols_set = set(markets.keys()) if isinstance(markets, dict) else set()

    def _to_ccxt_candidates(base: str) -> List[str]:
        base = (base or "").upper().replace(" ", "").replace("-", "")
        # variantes les plus fréquentes côté Bitget futures USDT
        return [f"{base}/USDT:USDT", f"{base}/USDT"]

    # --- Pagination CoinGecko: 250 par page, autant de pages que nécessaire ---
    per_page = 250
    pages = (int(size) + per_page - 1) // per_page
    picked: List[str] = []

    for page in range(1, pages + 1):
        try:
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": per_page,
                "page": page,
                "price_change_percentage": "24h",
                "sparkline": "false",
            }
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            items = r.json() or []
        except Exception:
            break  # en cas d'erreur réseau, on sort proprement (build_universe gère le fallback)

        for it in items:
            # On prend le 'symbol' CoinGecko (ex: "btc", "eth", "sol")
            base = str(it.get("symbol", "")).upper()
            if not base:
                continue
            for cand in _to_ccxt_candidates(base):
                if cand in symbols_set:
                    picked.append(cand)
                    break  # on a mappé cette base => passe à la suivante
            if len(picked) >= size:
                break
        if len(picked) >= size:
            break

    # Mémorise dans le cache du jour (même si incomplet, on laisse le fallback du caller gérer)
    cache[key] = picked[:]
    return picked[:size]



def _coingecko_coin_list_cached() -> list:
    """
    Retourne la liste CoinGecko (id, symbol, name) avec cache 1×/jour
    dans settings.COINGECKO_COIN_LIST_JSON et settings.COINGECKO_COIN_LIST_TS.
    """
    import time, json, requests
    try:
        ts = float(database.get_setting('COINGECKO_COIN_LIST_TS', '0') or '0')
    except Exception:
        ts = 0.0
    now = time.time()
    if now - ts < 23 * 3600:
        try:
            raw = database.get_setting('COINGECKO_COIN_LIST_JSON', '[]') or '[]'
            data = json.loads(raw)
            if isinstance(data, list) and data:
                return data
        except Exception:
            pass

    url = "https://api.coingecko.com/api/v3/coins/list"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json() if r.content else []
        if isinstance(data, list):
            database.set_setting('COINGECKO_COIN_LIST_JSON', json.dumps(data))
            database.set_setting('COINGECKO_COIN_LIST_TS', str(now))
            return data
    except Exception:
        pass
    return []


def _coingecko_market_caps_for_symbols(bases: list, sym_to_ids: dict) -> dict:
    """
    Pour chaque base (ex: 'BTC','ETH'), interroge CoinGecko /coins/markets
    sur tous les ids possibles du symbole et retient la market cap max.
    Retourne { 'BTC': mcap_usd, ... }.
    """
    import math, json, requests
    result = {}
    if not bases:
        return result

    # Construire la liste des ids à interroger à partir des symboles
    ids = []
    for base in bases:
        ids.extend(sym_to_ids.get(base.upper(), []))
    # Dédupliquer
    ids = list(dict.fromkeys([i for i in ids if i]))

    if not ids:
        return {b: 0.0 for b in bases}

    # CoinGecko limite per_page à 250
    per_page = 200
    pages = int(math.ceil(len(ids) / per_page))

    id_to_mcap = {}
    for p in range(pages):
        chunk = ids[p * per_page:(p + 1) * per_page]
        if not chunk:
            continue
        try:
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                "vs_currency": "usd",
                "ids": ",".join(chunk),
                "order": "market_cap_desc",
                "per_page": len(chunk),
                "page": 1,
                "sparkline": "false",
                "price_change_percentage": "24h"
            }
            r = requests.get(url, params=params, timeout=25)
            r.raise_for_status()
            data = r.json() if r.content else []
            for item in data or []:
                cid = str(item.get('id') or '')
                mcap = float(item.get('market_cap') or 0.0)
                if cid and mcap > 0:
                    id_to_mcap[cid] = max(id_to_mcap.get(cid, 0.0), mcap)
        except Exception:
            continue

    # Pour chaque base, prendre la meilleure mcap parmi ses ids
    for base in bases:
        best = 0.0
        for cid in sym_to_ids.get(base.upper(), []):
            mc = id_to_mcap.get(cid, 0.0)
            if mc > best:
                best = mc
        result[base] = best

    return result

def detect_market_regime(ex) -> str:
    """
    Détecte le régime macro du marché crypto.
    
    Analyse BTC (leader du marché) sur timeframe 1d pour déterminer :
    - Tendance (BULL/BEAR/RANGE)
    - Volatilité (NORMAL/HIGH)
    
    RÉGIMES :
    - BULL_STABLE : Tendance haussière + volatilité normale (meilleur)
    - BULL_VOLATILE : Tendance haussière + forte volatilité (prudence)
    - BEAR : Tendance baissière (réduire exposition)
    - RANGE : Sideways (scalp uniquement)
    - NEUTRAL : Impossible déterminer (défaut safe)
    
    Returns:
        String du régime détecté
    """
    try:
        # Feature activée ?
        try:
            enable_regime = str(database.get_setting('ENABLE_REGIME_DETECTION', 'true')).lower() == 'true'
        except Exception:
            enable_regime = True
        
        if not enable_regime:
            return 'NEUTRAL'
        
        # Analyser BTC sur 1d
        try:
            btc_df = utils.fetch_and_prepare_df(ex, 'BTC/USDT:USDT', '1d')
        except Exception:
            btc_df = None
        
        if btc_df is None or len(btc_df) < 50:
            print("⚠️ Impossible analyser BTC pour régime marché")
            return 'NEUTRAL'
        
        last = btc_df.iloc[-1]
        close = float(last['close'])
        
        # Calculer SMA20 et SMA50
        try:
            sma20 = float(btc_df['close'].rolling(20).mean().iloc[-1])
            sma50 = float(btc_df['close'].rolling(50).mean().iloc[-1])
        except Exception:
            return 'NEUTRAL'
        
        # ====== DÉTERMINER TENDANCE BTC ======
        if close > sma20 > sma50:
            btc_trend = 'BULL'
        elif close < sma20 < sma50:
            btc_trend = 'BEAR'
        else:
            btc_trend = 'RANGE'
        
        # ====== DÉTERMINER VOLATILITÉ ======
        try:
            atr_current = float(last.get('atr', 0))
            atr_mean = float(btc_df['atr'].tail(20).mean())
            
            if atr_current > atr_mean * 1.5:
                volatility = 'HIGH'
            else:
                volatility = 'NORMAL'
        except Exception:
            volatility = 'NORMAL'
        
        # ====== COMBINER RÉGIME FINAL ======
        if btc_trend == 'BULL' and volatility == 'NORMAL':
            regime = 'BULL_STABLE'
        elif btc_trend == 'BULL' and volatility == 'HIGH':
            regime = 'BULL_VOLATILE'
        elif btc_trend == 'BEAR':
            regime = 'BEAR'
        elif btc_trend == 'RANGE':
            regime = 'RANGE'
        else:
            regime = 'NEUTRAL'
        
        # Mémoriser en cache
        try:
            database.set_setting('CURRENT_MARKET_REGIME', regime)
        except Exception:
            pass
        
        print(f"📊 Régime marché détecté : {regime} (BTC: {btc_trend}, Vol: {volatility})")
        
        return regime
    
    except Exception as e:
        print(f"Erreur detect_market_regime: {e}")
        return 'NEUTRAL'


def adapt_strategy_to_regime(regime: str) -> dict:
    """
    Adapte les paramètres de trading selon le régime de marché.
    
    Args:
        regime: Régime détecté par detect_market_regime()
    
    Returns:
        Dict avec paramètres adaptés (max_pos, min_rr, etc.)
    """
    try:
        params = {
            'max_positions': 3,
            'min_rr': 3.0,
            'risk_pct': 2.0,
            'enable_ct': True,
        }
        
        if regime == 'BEAR':
            # Mode défensif
            params['max_positions'] = 2
            params['min_rr'] = 4.0
            params['risk_pct'] = 1.5
            params['enable_ct'] = False  # Pas de contre-tendance en bear
            print("🛡️ Mode BEAR : Paramètres défensifs activés")
        
        elif regime == 'BULL_STABLE':
            # Mode agressif
            params['max_positions'] = 5
            params['min_rr'] = 2.5
            params['risk_pct'] = 2.0
            params['enable_ct'] = True
            print("🚀 Mode BULL_STABLE : Paramètres agressifs activés")
        
        elif regime == 'BULL_VOLATILE':
            # Mode prudent
            params['max_positions'] = 3
            params['min_rr'] = 3.5
            params['risk_pct'] = 1.5
            params['enable_ct'] = True
            print("⚠️ Mode BULL_VOLATILE : Paramètres prudents activés")
        
        elif regime == 'RANGE':
            # Mode scalp
            params['max_positions'] = 2
            params['min_rr'] = 3.0
            params['risk_pct'] = 1.0
            params['enable_ct'] = True  # CT meilleur en range
            print("📊 Mode RANGE : Paramètres scalp activés")
        
        else:  # NEUTRAL
            # Paramètres standards (défaut)
            print("🔄 Mode NEUTRAL : Paramètres standards")
        
        return params
    
    except Exception as e:
        print(f"Erreur adapt_strategy_to_regime: {e}")
        return {
            'max_positions': 3,
            'min_rr': 3.0,
            'risk_pct': 2.0,
            'enable_ct': True,
        }

def is_good_trading_session() -> bool:
    """
    Filtre les sessions de trading optimales.
    
    ÉVITE :
    - Weekend (volume -60%, spreads x3)
    - Asia solo 2h-7h UTC (faible liquidité, manipulations)
    
    PRÉFÈRE :
    - London 8h-12h UTC
    - US 13h-17h UTC
    - Europe/US overlap 13h-16h UTC (meilleur)
    
    POURQUOI ÉVITER ASIA/WEEKEND :
    - Volume -60% → Whales manipulent facilement
    - Spreads x3 → Slippage énorme
    - Faux breakouts +45%
    - Stop hunts agressifs
    - Winrate -20% mesuré
    
    Returns:
        True si bonne session, False si pause recommandée
    """
    import datetime
    
    try:
        # Feature activée ?
        try:
            enable_filter = str(database.get_setting('ENABLE_SESSION_FILTER', 'true')).lower() == 'true'
        except Exception:
            enable_filter = True
        
        if not enable_filter:
            return True
        
        now = datetime.datetime.utcnow()
        hour = now.hour
        weekday = now.weekday()  # 0=Monday, 6=Sunday
        
        # ====== WEEKEND FILTER ======
        if weekday >= 5:  # Saturday=5, Sunday=6
            print(f"⏸️ Weekend détecté ({weekday}) → Pause trading")
            return False
        
        # ====== ASIA SOLO FILTER (2h-7h UTC) ======
        try:
            avoid_start = int(database.get_setting('AVOID_HOURS_START', '2'))
            avoid_end = int(database.get_setting('AVOID_HOURS_END', '8'))
        except Exception:
            avoid_start = 2
            avoid_end = 8
        
        if avoid_start <= hour < avoid_end:
            print(f"⏸️ Asia session solo ({hour}h UTC) → Pause trading")
            return False
        
        # ====== SESSIONS PREMIUM ======
        if 13 <= hour < 17:
            print(f"✅ US session ({hour}h UTC) → Trading actif")
            return True
        
        if 8 <= hour < 12:
            print(f"✅ London session ({hour}h UTC) → Trading actif")
            return True
        
        # Autres heures : OK mais moins optimal
        print(f"✅ Session acceptable ({hour}h UTC)")
        return True
    
    except Exception as e:
        print(f"Erreur is_good_trading_session: {e}")
        return True  # Fail-safe : ne pas bloquer le trading

def _inside(val: float, lo: float, up: float) -> bool:
    return float(lo) <= float(val) <= float(up)
    
def _near_bb80_with_tolerance(extreme: float, bb80_band: float, side: str, tol_pct: float, atr: float, atr_k: float) -> bool:
    """
    Tolérance SEULEMENT pour la BOUJIE DE CONTACT (pas SL/TP):
      - BUY  : low <= bb80_lo  OU low <= bb80_lo*(1+tol_pct)  OU low <= bb80_lo + ATR*atr_k
      - SELL : high >= bb80_up OU high >= bb80_up*(1-tol_pct) OU high >= bb80_up - ATR*atr_k
    """
    extreme = float(extreme); bb80_band = float(bb80_band)
    tol_pct = float(tol_pct); atr = float(atr); atr_k = float(atr_k)
    if side == 'buy':
        return (extreme <= bb80_band) or (extreme <= bb80_band * (1.0 + tol_pct)) or (extreme <= bb80_band + atr * atr_k)
    else:
        return (extreme >= bb80_band) or (extreme >= bb80_band * (1.0 - tol_pct)) or (extreme >= bb80_band - atr * atr_k)

def _touched_with_tol(price: float, band: float, side: str, tol_pct: float) -> bool:
    """
    Tolérance de contact sur la BB80 (jaune).
    - Long : on accepte si low <= bb80_lo * (1 + tol_pct)
    - Short: on accepte si high >= bb80_up * (1 - tol_pct)
    """
    band = float(band); price = float(price); tol = float(tol_pct)
    if side == 'buy':
        return price <= band * (1.0 + tol)
    else:
        return price >= band * (1.0 - tol)

def _maybe_improve_rr_with_cut_wick(prev: pd.Series, entry: float, sl: float, tp: float, side: str) -> Tuple[float, float]:
    """
    Si CUT_WICK_FOR_RR est ON (DB) et si RR initial < MIN_RR mais >= 2.8,
    recalcule un RR en 'coupant la mèche' (SL basé sur le corps de la bougie de déclenchement).
    Retourne (rr_alternatif, sl_original_ignoré). Le SL réel n’est pas modifié ici.
    """
    enabled = str(database.get_setting('CUT_WICK_FOR_RR', 'false')).lower() == 'true'
    if not enabled:
        if side == 'buy':
            return ((tp - entry) / (entry - sl), sl)
        else:
            return ((entry - tp) / (sl - entry), sl)

    open_, close_ = float(prev['open']), float(prev['close'])
    body_high, body_low = (max(open_, close_), min(open_, close_))
    if side == 'buy':
        sl_body = body_low  # coupe la mèche basse
        rr_alt = (tp - entry) / (entry - sl_body) if (entry - sl_body) > 0 else 0.0
        return rr_alt, sl
    else:
        sl_body = body_high # coupe la mèche haute
        rr_alt = (entry - tp) / (sl_body - entry) if (sl_body - entry) > 0 else 0.0
        return rr_alt, sl

def is_valid_reaction_candle(candle: pd.Series, side: str, prev: Optional[pd.Series] = None) -> bool:
    """Validation stricte de la bougie de réaction.
    Règles DB:
      - DOJI_BODY_MAX (def 0.10)
      - PINBAR_MAX_BODY (def 0.30)
      - SIMPLE_WICK_MIN (def 0.30)   # >= 30% du range pour la grande mèche
      - PINBAR_OPP_WICK_MAX (def 0.20)
      - MARUBOZU_MIN_BODY (def 0.30)
      - WICK_HUGE_MAX (def 0.70)
    Spécifique:
      - PINBAR: couleur indifférente ; on exige seulement la bonne mèche (opposée au trade) et les ratios ci-dessus.
      - Autres patterns (impulsion/marubozu/gap/double): direction dans le sens du trade.
    """
    try:
        doji_max            = float(database.get_setting('DOJI_BODY_MAX', 0.10))
        pinbar_max_body     = float(database.get_setting('PINBAR_MAX_BODY', 0.30))
        simple_wick_min     = float(database.get_setting('SIMPLE_WICK_MIN', 0.30))
        pinbar_opp_wick_max = float(database.get_setting('PINBAR_OPP_WICK_MAX', 0.20))
        marubozu_min_body   = float(database.get_setting('MARUBOZU_MIN_BODY', 0.30))
        wick_huge_max       = float(database.get_setting('WICK_HUGE_MAX', 0.70))
    except Exception:
        doji_max, pinbar_max_body, simple_wick_min = 0.10, 0.30, 0.30
        pinbar_opp_wick_max, marubozu_min_body, wick_huge_max = 0.20, 0.30, 0.70

    o, c = float(candle['open']), float(candle['close'])
    h, l = float(candle['high']), float(candle['low'])
    rng  = max(1e-12, h - l)
    body = abs(c - o)
    w_up = max(0.0, h - max(o, c))
    w_dn = max(0.0, min(o, c) - l)

    body_r = body / rng
    w_up_r = w_up / rng
    w_dn_r = w_dn / rng

    # (A) Rejet sécurité : mèche énorme du côté trade
    if side == 'buy' and w_dn_r > wick_huge_max:
        return False
    if side == 'sell' and w_up_r > wick_huge_max:
        return False

    # (B) PINBAR (couleur indifférente) — uniquement des conditions de mèches/corps
    if body_r <= pinbar_max_body:
        if side == 'buy':
            # Long ⇒ grande mèche basse (>= 30%), petite mèche haute (<= opp max)
            if w_dn_r >= simple_wick_min and w_up_r <= pinbar_opp_wick_max:
                return True
        else:
            # Short ⇒ grande mèche haute, petite mèche basse
            if w_up_r >= simple_wick_min and w_dn_r <= pinbar_opp_wick_max:
                return True
        # si pinbar non valide, on continue aux autres patterns

    # (C) Éliminer les dojis trop faibles pour le reste
    if body_r < doji_max:
        return False

    # (D) Pour les autres patterns, on exige la direction dans le sens du trade
    is_bull = c > o
    if (side == 'buy' and not is_bull) or (side == 'sell' and is_bull):
        return False

    # (E) Impulsion / marubozu directionnels
    if body_r >= marubozu_min_body:
        return True

    # (F) Motifs inter-bougies (avec direction du cur déjà validée ci-dessus)
    if prev is not None:
        if _is_gap_impulse(prev, candle, side):
            return True
        if _is_double_marubozu(prev, candle, side):
            return True

    return False



def _compute_body_wicks(candle: pd.Series) -> Tuple[float, float, float, float]:
    """
    Retourne (body_ratio, wick_up_ratio, wick_down_ratio, range) par rapport au range de la bougie.
    """
    o, c = float(candle['open']), float(candle['close'])
    h, l = float(candle['high']), float(candle['low'])
    rng = max(1e-12, h - l)
    body = abs(c - o)
    w_up = max(0.0, h - max(o, c))
    w_dn = max(0.0, min(o, c) - l)
    return body / rng, w_up / rng, w_dn / rng, rng

def _body_ratio(candle: pd.Series) -> float:
    o, c = float(candle['open']), float(candle['close'])
    h, l = float(candle['high']), float(candle['low'])
    rng = max(1e-12, h - l)
    return abs(c - o) / rng


def _is_gap_impulse(prev: pd.Series, cur: pd.Series, side: str) -> bool:
    """
    Détection 'Gap + Impulsion' entre prev et cur (agnostique à la couleur).
      - GAP_MIN_PCT (def 0.001 = 0.1%)
      - IMPULSE_MIN_BODY (def 0.35 = 35% du range)
    """
    if prev is None or cur is None:
        return False
    try:
        gap_min_pct      = float(database.get_setting('GAP_MIN_PCT', 0.001))
        impulse_min_body = float(database.get_setting('IMPULSE_MIN_BODY', 0.35))
    except Exception:
        gap_min_pct, impulse_min_body = 0.001, 0.35

    prev_close = float(prev['close'])
    cur_open   = float(cur['open'])
    if prev_close <= 0:
        return False

    gap_ratio = abs(cur_open - prev_close) / prev_close
    if gap_ratio < gap_min_pct:
        return False

    body_r, _, _, _ = _compute_body_wicks(cur)
    if body_r < impulse_min_body:
        return False

    return True

def _is_double_marubozu(prev: pd.Series, cur: pd.Series, side: str) -> bool:
    """
    Détection 'Double marubozu 30%' (agnostique à la couleur) :
      - body_ratio(prev) ≥ DOUBLE_MARUBOZU_MIN (def 0.30)
      - body_ratio(cur)  ≥ DOUBLE_MARUBOZU_MIN (def 0.30)
      - mèches de chaque bougie ≤ DOUBLE_MARUBOZU_WICK_MAX (def 0.10)
    """
    if prev is None or cur is None:
        return False
    try:
        min_body_ratio = float(database.get_setting('DOUBLE_MARUBOZU_MIN', 0.30))
        max_wick_ratio = float(database.get_setting('DOUBLE_MARUBOZU_WICK_MAX', 0.10))
    except Exception:
        min_body_ratio, max_wick_ratio = 0.30, 0.10

    b1, u1, d1, _ = _compute_body_wicks(prev)
    b2, u2, d2, _ = _compute_body_wicks(cur)

    if not (b1 >= min_body_ratio and b2 >= min_body_ratio):
        return False
    if any(x > max_wick_ratio for x in (u1, d1, u2, d2)):
        return False

    return True

def _is_valid_reaction(df: pd.DataFrame, i: int, direction: str) -> bool:
    """
    Valide la 'bougie de réaction' selon le preset Balanced, pour LONG et SHORT.
    Exigences :
      1) Close À L’INTÉRIEUR de la BB20 (entre bb20_low et bb20_high).
      2) Structure directionnelle OBLIGATOIRE :
         - impulse (corps >= IMPULSE_MIN_BODY) dans le sens du trade
           OU
         - pin-bar (mèche dominante >= SIMPLE_WICK_MIN, corps <= PINBAR_MAX_BODY)
           avec la mèche opposée <= PINBAR_OPP_WICK_MAX dans le sens du trade.
    Args:
        df: DataFrame avec colonnes ['open','high','low','close','bb20_low','bb20_high'].
        i: index (int) de la bougie candidate.
        direction: 'long' ou 'short'.
    Returns:
        bool
    """
    row = df.iloc[i]
    o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
    bb_low = float(row["bb20_low"])
    bb_high = float(row["bb20_high"])

    # 1) Réintégration BB20 obligatoire
    if not (bb_low <= c <= bb_high):
        return False

    # Mesures de bougie
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    body_pct = body / rng
    up_wick = h - max(o, c)
    dn_wick = min(o, c) - l
    up_wick_pct = up_wick / rng
    dn_wick_pct = dn_wick / rng

    # Tolérances (fallback si non définies plus haut dans le fichier)
    pinbar_opp_max = globals().get("PINBAR_OPP_WICK_MAX", 0.24)

    # 2) Structure directionnelle
    if direction == "long":
        impulse_ok = (c > o) and (body_pct >= IMPULSE_MIN_BODY)
        pinbar_ok = (dn_wick_pct >= SIMPLE_WICK_MIN) and (body_pct <= PINBAR_MAX_BODY) and (up_wick_pct <= pinbar_opp_max)
        return bool(impulse_ok or pinbar_ok)

    if direction == "short":
        impulse_ok = (c < o) and (body_pct >= IMPULSE_MIN_BODY)
        pinbar_ok = (up_wick_pct >= SIMPLE_WICK_MIN) and (body_pct <= PINBAR_MAX_BODY) and (dn_wick_pct <= pinbar_opp_max)
        return bool(impulse_ok or pinbar_ok)

    return False

def _reaction_trigger_levels(df: pd.DataFrame, i: int, direction: str) -> Optional[Dict[str, float]]:
    """
    Donne les niveaux d'EXÉCUTION issus de la bougie de réaction validée :
      - entrée uniquement sur CASSURE du high (long) / low (short) de la réaction
      - SL de référence = low (long) / high (short) de la réaction (les coussins %/ATR sont ajoutés ailleurs)
    Args:
        df: DataFrame OHLC avec index i existant.
        i: index (int) de la bougie de réaction (déjà validée par _is_valid_reaction).
        direction: 'long' ou 'short'.
    Returns:
        dict avec {'entry_trigger','sl_ref'} ou None si direction invalide.
    """
    row = df.iloc[i]
    h, l = float(row["high"]), float(row["low"])

    if direction == "long":
        return {"entry_trigger": h, "sl_ref": l}
    if direction == "short":
        return {"entry_trigger": l, "sl_ref": h}
    return None


def _sl_from_contact_candle(contact: pd.Series, side: str, atr_contact: Optional[float] = None) -> float:
    """
    Calcule le SL à partir de la bougie de CONTACT (bougie 1) avec offset hybride.
      - Long  (buy)  : ancre = low(contact)  → SL = low * (1 - max(pct, ATR*k/low))
      - Short (sell) : ancre = high(contact) → SL = high * (1 + max(pct, ATR*k/high))

    Args:
        contact: pd.Series contenant au minimum open/close/high/low (et idéalement 'atr').
        side: 'buy' | 'sell'
        atr_contact: ATR de la bougie contact si déjà calculé (optionnel). Sinon on tente contact['atr'].

    Returns:
        float: prix de stop-loss calculé (>= 0). En cas de données manquantes, renvoie l’ancre brute.
    """
    try:
        s = str(side or "").strip().lower()
        if s not in ("buy", "sell"):
            return 0.0

        # Récupération robuste des champs
        h = float(contact.get("high")) if "high" in contact else float(contact["high"])
        l = float(contact.get("low"))  if "low"  in contact else float(contact["low"])
        if not (h > 0 and l > 0):
            # Données insuffisantes : on renvoie un fallback neutre
            return max(0.0, l if s == "buy" else h)

        # ATR de référence (si fourni, sinon depuis la bougie)
        atr_val = None
        if atr_contact is not None:
            try:
                atr_val = float(atr_contact)
            except Exception:
                atr_val = None
        if atr_val is None:
            try:
                atr_val = float(contact.get("atr", 0.0))
            except Exception:
                atr_val = 0.0

        # Ancre = extrême de la bougie de contact dans le sens opposé à l'entrée
        anchor = l if s == "buy" else h
        if anchor <= 0:
            return max(0.0, anchor)

        # Offset hybride (pourcentage vs ATR*k converti en %) appliqué sur l’ancre
        sl = adjust_sl_for_offset(
            raw_sl=float(anchor),
            side=s,
            atr=float(atr_val or 0.0),
            ref_price=float(anchor)  # conversion ATR→% par rapport à l’ancre
        )

        # Garde-fous numériques
        if not (sl > 0):
            return max(0.0, anchor)
        return float(sl)

    except Exception:
        # En cas d’imprévu, on renvoie l’ancre (comportement conservateur)
        try:
            if str(side).lower() == "buy":
                return float(contact.get("low", 0.0))
            else:
                return float(contact.get("high", 0.0))
        except Exception:
            return 0.0

def _find_contact_index(df: pd.DataFrame, base_exclude_last: bool = True, max_lookback: int = 5) -> Optional[int]:
    """
    Retourne l'index de la dernière bougie de CONTACT (avant la réaction si base_exclude_last=True),
    c.-à-d. une bougie qui touche la BB20_lo (pour un long) ou la BB20_up (pour un short).
    On ne déduit PAS le sens ici : on cherche juste la bougie la plus récente qui touche au moins une borne BB20.
    """
    if df is None or len(df) < 3:
        return None

    start = -2 if base_exclude_last else -1
    base_idx = len(df) + start
    lb = max(1, int(max_lookback))

    for back in range(1, lb + 1):
        idx = base_idx - back
        if idx < 0:
            break
        row = df.iloc[idx]
        # "touch" générique : low <= bb20_lo OU high >= bb20_up
        try:
            touched_lo = float(row['low'])  <= float(row['bb20_lo'])
            touched_up = float(row['high']) >= float(row['bb20_up'])
        except Exception:
            continue
        if touched_lo or touched_up:
            return idx
    return None

def _previous_wave_by_bb80(df: pd.DataFrame, idx: int, dead_zone_pct: float) -> Optional[str]:
    """
    Détermine le sens de la VAGUE PRÉCÉDENTE uniquement via CLOSE vs bb80_mid,
    en sortant de la dead-zone autour de bb80_mid.
    Retourne 'up' | 'down' | None.
    """
    if df is None or len(df) == 0:
        return None
    i = max(0, min(idx, len(df) - 1))
    for k in range(i - 1, -1, -1):
        try:
            close_k = float(df.iloc[k]['close'])
            mid_k   = float(df.iloc[k]['bb80_mid'])
            dz_k    = mid_k * (dead_zone_pct / 100.0)
        except Exception:
            continue
        if abs(close_k - mid_k) >= dz_k:
            return 'up' if close_k > mid_k else 'down'
    return None

def _is_inside_both_bb(row, is_long: bool) -> bool:
    """
    Retourne True si la clôture est à l'intérieur des DEUX bandes :
      - BB20 (bb20_lo / bb20_up)
      - BB80 (bb80_lo / bb80_up)
    C'est notre définition de "réintégration claire" pour la CT.
    """
    try:
        c = float(row['close'])
        bb20_up = float(row['bb20_up'])
        bb20_lo = float(row['bb20_lo'])
        bb80_up = float(row['bb80_up'])
        bb80_lo = float(row['bb80_lo'])
    except Exception:
        return False

    return (bb20_lo <= c <= bb20_up) and (bb80_lo <= c <= bb80_up)


def _check_ct_reintegration_window(df: pd.DataFrame, is_long: bool, max_window: int = 3) -> bool:
    """
    Gate spécifique CONTRE-TENDANCE.

    Vérifie, dans les (max_window+1) dernières bougies clôturées avant l'entrée :
      1) présence d'un CONTACT BB80 (haut pour un short, bas pour un long),
      2) une RÉINTÉGRATION en clôture à l'intérieur des DEUX BB (20 et 80),
      3) aucune "ressortie" en clôture des BB20/BB80 entre cette réintégration
         et la bougie de réaction.

    Si la séquence n'est pas respectée → False (on rejette le signal CT).
    """
    try:
        if df is None:
            return False
        # Besoin d'au moins (max_window + 2) bougies :
        #  - 1 bougie courante (open d'entrée)
        #  - 1 bougie de réaction
        #  - max_window bougies de contexte.
        if len(df) < max_window + 2:
            return False

        # On travaille sur les bougies clôturées : on exclut la dernière,
        # qui sert d'open d'entrée (CT_ENTRY_ON_NEXT_BAR).
        # Exemple max_window=3 → on regarde les 4 dernières bougies clôturées
        # (dont la bougie de réaction).
        window = df.iloc[-(max_window + 1):-1]
    except Exception:
        return False

    if window is None or len(window) == 0:
        return False

    # 1) Recherche du contact BB80 dans la fenêtre (on prend le DERNIER contact trouvé).
    contact_idx = None
    for idx, row in window.iterrows():
        try:
            if is_long:
                touched = float(row['low']) <= float(row['bb80_lo'])
            else:
                touched = float(row['high']) >= float(row['bb80_up'])
        except Exception:
            touched = False

        if touched:
            contact_idx = idx

    if contact_idx is None:
        # Pas de contact BB80 → séquence CT invalide
        return False

    # 2) Recherche de la première bougie de réintégration (clôture dans BB20 ET BB80)
    reintegration_idx = None
    idxs = list(window.index)
    start_found = False
    for idx in idxs:
        if idx == contact_idx:
            start_found = True
        if not start_found:
            continue

        row = window.loc[idx]
        if _is_inside_both_bb(row, is_long):
            reintegration_idx = idx
            break

    if reintegration_idx is None:
        # Pas de réintégration claire après le contact
        return False

    # 3) Vérifier qu'entre la réintégration et la bougie de réaction
    #    on ne ressort pas des BB20/BB80 en clôture.
    start_pos = idxs.index(reintegration_idx)
    for idx in idxs[start_pos:]:
        row = window.loc[idx]
        if not _is_inside_both_bb(row, is_long):
            # Ressortie → séquence CT invalide
            return False

    return True


# ============================================================================
# VALIDATION PATTERNS DARWIN (selon slides)
# ============================================================================

def is_pinbar_30pct(bar: pd.Series, setup_type: str) -> bool:
    """
    Valide un Pinbar 30% selon slides Darwin.
    
    Args:
        bar: Bougie à analyser
        setup_type: 'long' ou 'short'
    
    Returns:
        True si pinbar valide
    """
    total_range = bar['high'] - bar['low']
    if total_range == 0:
        return False
    
    body = abs(bar['close'] - bar['open'])
    
    if setup_type == 'long':
        # Pour LONG : mèche BASSE doit être >= 30% du range total
        lower_wick = min(bar['open'], bar['close']) - bar['low']
        wick_pct = lower_wick / total_range
        return wick_pct >= 0.30
    
    else:  # short
        # Pour SHORT : mèche HAUTE doit être >= 30% du range total
        upper_wick = bar['high'] - max(bar['open'], bar['close'])
        wick_pct = upper_wick / total_range
        return wick_pct >= 0.30


def is_simple_wick_30pct(bar: pd.Series, setup_type: str) -> bool:
    """
    Valide un Méchage simple 30% selon slides Darwin.
    
    Args:
        bar: Bougie à analyser
        setup_type: 'long' ou 'short'
    
    Returns:
        True si méchage simple valide
    """
    total_range = bar['high'] - bar['low']
    if total_range == 0:
        return False
    
    if setup_type == 'long':
        # Mèche basse >= 30%
        lower_wick = min(bar['open'], bar['close']) - bar['low']
        return (lower_wick / total_range) >= 0.30
    else:
        # Mèche haute >= 30%
        upper_wick = bar['high'] - max(bar['open'], bar['close'])
        return (upper_wick / total_range) >= 0.30


def is_double_marubozu_30pct(bar1: pd.Series, bar2: pd.Series, setup_type: str) -> bool:
    """
    Valide un Double marubozu 30% selon slides Darwin.
    
    Args:
        bar1: Première bougie (contact)
        bar2: Deuxième bougie (réaction)
        setup_type: 'long' ou 'short'
    
    Returns:
        True si double marubozu valide
    """
    # Vérifier que les deux bougies ont un body >= 30% de leur range
    for bar in [bar1, bar2]:
        total_range = bar['high'] - bar['low']
        if total_range == 0:
            return False
        
        body = abs(bar['close'] - bar['open'])
        body_pct = body / total_range
        
        if body_pct < 0.30:
            return False
    
    # Vérifier que les couleurs sont cohérentes avec le setup
    if setup_type == 'long':
        # bar1 rouge (bearish), bar2 verte (bullish)
        bar1_bearish = bar1['close'] < bar1['open']
        bar2_bullish = bar2['close'] > bar2['open']
        return bar1_bearish and bar2_bullish
    else:
        # bar1 verte (bullish), bar2 rouge (bearish)
        bar1_bullish = bar1['close'] > bar1['open']
        bar2_bearish = bar2['close'] < bar2['open']
        return bar1_bullish and bar2_bearish


def is_gap_impulse(bar1: pd.Series, bar2: pd.Series, setup_type: str) -> bool:
    """
    Valide un Gap + Impulsion selon slides Darwin.
    
    Args:
        bar1: Bougie précédente
        bar2: Bougie de réaction (avec gap)
        setup_type: 'long' ou 'short'
    
    Returns:
        True si gap + impulsion valide
    """
    # Détecter le gap
    if setup_type == 'long':
        # Gap haussier : low de bar2 > high de bar1
        gap_exists = bar2['low'] > bar1['high']
        # Impulsion : grosse bougie verte
        is_bullish = bar2['close'] > bar2['open']
        body = bar2['close'] - bar2['open']
        total_range = bar2['high'] - bar2['low']
        strong_body = (body / total_range) > 0.60 if total_range > 0 else False
        
        return gap_exists and is_bullish and strong_body
    
    else:  # short
        # Gap baissier : high de bar2 < low de bar1
        gap_exists = bar2['high'] < bar1['low']
        # Impulsion : grosse bougie rouge
        is_bearish = bar2['close'] < bar2['open']
        body = bar2['open'] - bar2['close']
        total_range = bar2['high'] - bar2['low']
        strong_body = (body / total_range) > 0.60 if total_range > 0 else False
        
        return gap_exists and is_bearish and strong_body


def find_reaction_pattern(df: pd.DataFrame, contact_idx: int, setup_type: str) -> dict:
    """
    Cherche un pattern de réaction valide dans les 1-2 bougies après contact.
    
    Patterns valides (slides Darwin) :
    - Pinbar 30%
    - Méchage simple 30%
    - Double marubozu 30%
    - Gap + Impulsion
    
    Args:
        df: DataFrame avec les données
        contact_idx: Index de la bougie de contact
        setup_type: 'long' ou 'short'
    
    Returns:
        {
            'valid': bool,
            'reaction_idx': int ou None,
            'pattern': str ou None,
            'reason': str
        }
    """
    # Chercher dans les 2 bougies suivantes
    for i in range(contact_idx + 1, min(contact_idx + 3, len(df))):
        bar = df.iloc[i]
        
        # Test PINBAR 30%
        if is_pinbar_30pct(bar, setup_type):
            return {
                'valid': True,
                'reaction_idx': i,
                'pattern': 'pinbar',
                'reason': "Pinbar 30% détecté"
            }
        
        # Test MÉCHAGE 30%
        if is_simple_wick_30pct(bar, setup_type):
            return {
                'valid': True,
                'reaction_idx': i,
                'pattern': 'wick',
                'reason': "Méchage simple 30% détecté"
            }
        
        # Test DOUBLE MARUBOZU 30% (besoin de 2 bougies)
        if i >= contact_idx + 1:
            prev_bar = df.iloc[i - 1]
            if is_double_marubozu_30pct(prev_bar, bar, setup_type):
                return {
                    'valid': True,
                    'reaction_idx': i,
                    'pattern': 'marubozu',
                    'reason': "Double marubozu 30% détecté"
                }
        
        # Test GAP + IMPULSION
        if i > 0:
            prev_bar = df.iloc[i - 1]
            if is_gap_impulse(prev_bar, bar, setup_type):
                return {
                    'valid': True,
                    'reaction_idx': i,
                    'pattern': 'gap',
                    'reason': "Gap + Impulsion détecté"
                }
    
    return {
        'valid': False,
        'reaction_idx': None,
        'pattern': None,
        'reason': "Aucun pattern 30% trouvé dans les 2 bougies"
    }


def validate_reintegration_bb20(df: pd.DataFrame, reaction_idx: int, setup_type: str) -> dict:
    """
    CRITIQUE : Vérifie que le prix RÉINTÈGRE la BB20 après la réaction.
    
    Selon slides Darwin :
    Contact → Réaction → RÉINTÉGRATION → Entrée
    
    Args:
        df: DataFrame
        reaction_idx: Index de la bougie de réaction
        setup_type: 'long' ou 'short'
    
    Returns:
        {
            'valid': bool,
            'reintegration_idx': int ou None,
            'reason': str
        }
    """
    # Chercher dans les 1-2 bougies APRÈS la réaction
    for i in range(reaction_idx + 1, min(reaction_idx + 3, len(df))):
        bar = df.iloc[i]
        bb20_up = bar['bb20_up']
        bb20_lo = bar['bb20_lo']
        
        # Le prix doit être DANS la BB20
        # close doit être entre bb20_lo et bb20_up
        if bb20_lo <= bar['close'] <= bb20_up:
            return {
                'valid': True,
                'reintegration_idx': i,
                'reason': f"Réintégration BB20 valide à la bougie {i}"
            }
    
    # Aucune réintégration trouvée
    return {
        'valid': False,
        'reintegration_idx': None,
        'reason': "Pas de réintégration BB20 après la réaction (prix reste dehors)"
    }


def validate_double_extreme_ct(df: pd.DataFrame, contact_idx: int) -> bool:
    """
    Pour CT : vérifie que BB20 ET BB80 ont été touchées.
    
    Slides Darwin CT : "Zone contact/traversement avec borne basse BB blanche ET jaune"
    
    Args:
        df: DataFrame
        contact_idx: Index de contact
    
    Returns:
        True si double extrême détecté
    """
    # Vérifier dans les 5 dernières bougies avant/pendant contact
    start_idx = max(0, contact_idx - 5)
    window = df.iloc[start_idx:contact_idx + 1]
    
    bb20_touched = False
    bb80_touched = False
    
    for _, row in window.iterrows():
        # BB20 touchée
        if row['low'] <= row['bb20_lo'] or row['high'] >= row['bb20_up']:
            bb20_touched = True
        
        # BB80 touchée
        if row['low'] <= row['bb80_lo'] or row['high'] >= row['bb80_up']:
            bb80_touched = True
    
    return bb20_touched and bb80_touched

def _is_first_after_prolonged_bb80_exit(df: pd.DataFrame, is_long: bool, min_streak: int = 5, lookback: int = 50) -> bool:
    """
    Détecte si on est juste après une sortie PROLONGÉE de la BB80 (GATE 3).
    
    RÈGLE DARWIN :
    - Si on a eu 5+ bougies CONSÉCUTIVES hors BB80 récemment
    - On REJETTE le premier signal qui apparaît après
    - Raison : Excès de volatilité, faux signaux fréquents
    
    Args:
        df: DataFrame marché
        is_long: True pour LONG, False pour SHORT
        min_streak: Nombre minimum de bougies consécutives hors BB80 (défaut 5)
        lookback: Fenêtre de recherche en nombre de bougies (défaut 50)
    
    Returns:
        True si on doit REJETER le signal (premier après excès)
        False si signal acceptable
    
    Exemples:
        LONG : Si 5+ bougies avec close < bb80_lo → REJETER prochain signal LONG
        SHORT : Si 5+ bougies avec close > bb80_up → REJETER prochain signal SHORT
    """
    try:
        if df is None or len(df) < min_streak + 2:
            return False
        
        # Limiter la fenêtre d'analyse
        start_idx = max(0, len(df) - lookback)
        window = df.iloc[start_idx:]
        
        if len(window) < min_streak + 2:
            return False
        
        # ====== DÉTECTION EXCÈS BB80 ======
        
        streak_count = 0
        max_streak = 0
        last_streak_end = -999  # Index de fin du dernier streak détecté
        
        for i, row in window.iterrows():
            try:
                close = float(row['close'])
                bb80_up = float(row['bb80_up'])
                bb80_lo = float(row['bb80_lo'])
            except Exception:
                continue
            
            # Vérifier si hors BB80
            is_outside = False
            
            if is_long:
                # LONG : chercher excès vers le BAS (close < bb80_lo)
                is_outside = (close < bb80_lo)
            else:
                # SHORT : chercher excès vers le HAUT (close > bb80_up)
                is_outside = (close > bb80_up)
            
            if is_outside:
                streak_count += 1
                max_streak = max(max_streak, streak_count)
            else:
                # Fin du streak
                if streak_count >= min_streak:
                    # Enregistrer position de fin du streak
                    last_streak_end = window.index.get_loc(i) - 1
                
                streak_count = 0
        
        # Vérifier si dernier streak était prolongé
        if streak_count >= min_streak:
            # On est ENCORE dans l'excès → rejeter
            return True
        
        # ====== VÉRIFIER SI ON EST JUSTE APRÈS L'EXCÈS ======
        
        if max_streak >= min_streak:
            # Un excès a été détecté
            # Vérifier si on est dans les 1-3 bougies APRÈS la fin de l'excès
            
            current_pos = len(window) - 1
            distance_from_end = current_pos - last_streak_end
            
            # Si on est dans les 1-3 bougies après l'excès → REJETER
            if 1 <= distance_from_end <= 3:
                return True
        
        return False
    
    except Exception as e:
        print(f"Erreur _is_first_after_prolonged_bb80_exit: {e}")
        return False  # En cas d'erreur, ne pas rejeter le signal    

def detect_signal(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Détecte un signal TREND ou COUNTER-TREND avec GATES strictes Darwin.
    
    🔧 GATES TREND (TF/CT) :
    - GATE 1 : Prix doit être à l'extérieur de BB20 (TF) ou BB80 (CT)
    - GATE 2 : Validation séquence stricte de ré-intégration (fenêtre 1-3 bougies)
    - GATE 3 : Rejet signaux après excès BB prolongé (min 5 bougies hors BB80)
    - GATE 4 : Validation sens de la mèche (must be in direction of signal)
    
    Returns:
        Dict avec clés: side, regime, entry, sl, tp, rr, ts, reason
        None si aucun signal valide
    """
    if df is None or df.empty or len(df) < 85:
        return None

    # ============================================================================
    # EXTRACTION DONNÉES ACTUELLES (ligne -1 = bougie fermée)
    # ============================================================================
    
    current = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else current

    close_price = float(current['close'])
    open_price = float(current['open'])
    high_price = float(current['high'])
    low_price = float(current['low'])
    
    mm20 = float(current.get('mm20', close_price))
    mm80 = float(current.get('mm80', close_price))
    
    bb20_up = float(current.get('bb20_up', close_price))
    bb20_lo = float(current.get('bb20_lo', close_price))
    bb80_up = float(current.get('bb80_up', close_price))  # ✅ UNIFORMISÉ
    bb80_lo = float(current.get('bb80_lo', close_price))  # ✅ UNIFORMISÉ

    # ============================================================================
    # DÉTECTION POSITION PAR RAPPORT AUX BANDES
    # ============================================================================
    
    # BB20
    is_above_bb20 = close_price > bb20_up
    is_below_bb20 = close_price < bb20_lo
    is_inside_bb20 = bb20_lo <= close_price <= bb20_up

    # BB80
    is_above_bb80 = close_price > bb80_up
    is_below_bb80 = close_price < bb80_lo
    is_inside_bb80 = bb80_lo <= close_price <= bb80_up

    # Position "claire" (marge 0.05%)
    threshold = 0.0005
    is_clearly_above_mm80 = close_price > mm80 * (1 + threshold)
    is_clearly_below_mm80 = close_price < mm80 * (1 - threshold)

    # Dead zone (entre BB20 et BB80)
    is_in_dead_zone = is_inside_bb80 and not is_inside_bb20
    
    # Trend bias (basé sur MM80)
    trend_bias = 'up' if close_price > mm80 else 'down'

    # ============================================================================
    # VALIDATION DIRECTION DE LA MÈCHE (GATE 4)
    # ============================================================================
    
    def _validate_wick_direction(df_local: pd.DataFrame, is_long: bool, lookback: int = 3) -> bool:
        """
        Vérifie que la mèche de rejet va dans le sens du signal.
        
        Pour un LONG : mèche basse doit être significative (rejet vers le bas)
        Pour un SHORT : mèche haute doit être significative (rejet vers le haut)
        """
        if df_local is None or df_local.empty:
            return False
        
        try:
            recent = df_local.iloc[-lookback:] if len(df_local) >= lookback else df_local
            
            for _, candle in recent.iterrows():
                c = float(candle['close'])
                o = float(candle['open'])
                h = float(candle['high'])
                l = float(candle['low'])
                
                body = abs(c - o)
                if body < 1e-10:
                    continue
                
                if is_long:
                    # LONG : mèche basse significative
                    lower_wick = min(c, o) - l
                    if lower_wick > body * 0.5:  # Mèche > 50% du corps
                        return True
                else:
                    # SHORT : mèche haute significative
                    upper_wick = h - max(c, o)
                    if upper_wick > body * 0.5:
                        return True
            
            return False
        except Exception:
            return False

    # ============================================================================
    # HELPERS VALIDATION SÉQUENCE CT
    # ============================================================================
    
    def _check_ct_reintegration_window(df_local: pd.DataFrame, is_long: bool, max_window: int = 3) -> bool:
        """
        Valide la séquence stricte CT (GATE 2) :
        1. Contact avec BB80 (sortie)
        2. Ré-intégration dans les 1-3 bougies
        3. Pas de ressortie après ré-intégration
        
        Returns:
            True si séquence valide, False sinon
        """
        if df_local is None or df_local.empty or len(df_local) < max_window + 2:
            return False
        
        try:
            recent = df_local.iloc[-(max_window + 5):].copy()
            if len(recent) < max_window + 2:
                return False
            
            bb80_u = recent['bb80_up'].values
            bb80_l = recent['bb80_lo'].values
            closes = recent['close'].values
            
            # Chercher le contact BB80
            contact_idx = None
            for i in range(len(recent) - max_window - 1):
                if is_long:
                    if closes[i] < bb80_l[i]:  # En dessous BB80
                        contact_idx = i
                        break
                else:
                    if closes[i] > bb80_u[i]:  # Au dessus BB80
                        contact_idx = i
                        break
            
            if contact_idx is None:
                return False
            
            # Vérifier ré-intégration dans la fenêtre
            reintegrated = False
            reintegration_idx = None
            
            for i in range(contact_idx + 1, min(contact_idx + max_window + 1, len(recent))):
                if is_long:
                    if bb80_l[i] <= closes[i] <= bb80_u[i]:
                        reintegrated = True
                        reintegration_idx = i
                        break
                else:
                    if bb80_l[i] <= closes[i] <= bb80_u[i]:
                        reintegrated = True
                        reintegration_idx = i
                        break
            
            if not reintegrated:
                return False
            
            # Vérifier pas de ressortie après ré-intégration
            for i in range(reintegration_idx + 1, len(recent)):
                if is_long:
                    if closes[i] < bb80_l[i]:
                        return False
                else:
                    if closes[i] > bb80_u[i]:
                        return False
            
            return True
            
        except Exception:
            return False

    # ✅ DOUBLON SUPPRIMÉ - On utilise la fonction globale définie ligne ~1200

    # ============================================================================
    # DÉTECTION TREND-FOLLOWING LONG (prix au-dessus MM80, sortie BB20 haut)
    # ============================================================================
    
    if is_clearly_above_mm80 or (is_in_dead_zone and trend_bias == 'up'):
        # ✅ GATE 1 : Prix doit avoir été au-dessus de BB20
        if not is_above_bb20:
            return None
        
        # ✅ GATE 2 : Vérifier séquence de ré-intégration BB20
        if not _check_ct_reintegration_window(df, is_long=True, max_window=3):
            return None
        
        # ✅ GATE 3 : Rejeter premier signal après excès BB80
        if _is_first_after_prolonged_bb80_exit(df, is_long=True, min_streak=5, lookback=50):
            return None
        
        # ✅ GATE 4 : Validation mèche
        if not _validate_wick_direction(df, is_long=True, lookback=3):
            return None
        
        # Calcul niveaux
        entry = close_price
        sl = bb20_lo
        tp = bb80_up
        
        # Vérification RR basique
        distance_to_sl = abs(entry - sl)
        distance_to_tp = abs(tp - entry)
        
        if distance_to_sl < 1e-10:
            return None
        
        rr_raw = distance_to_tp / distance_to_sl
        
        return {
            'side': 'buy',
            'regime': 'Trend-Following',
            'entry': entry,
            'sl': sl,
            'tp': tp,
            'rr': rr_raw,
            'ts': int(current.name.timestamp() * 1000) if hasattr(current.name, 'timestamp') else int(time.time() * 1000),
            'reason': 'TF Long validé (4 gates)'
        }
    
    # ============================================================================
    # DÉTECTION TREND-FOLLOWING SHORT (prix sous MM80, sortie BB20 bas)
    # ============================================================================
    
    if is_clearly_below_mm80 or (is_in_dead_zone and trend_bias == 'down'):
        # ✅ GATE 1 : Prix doit avoir été en dessous de BB20
        if not is_below_bb20:
            return None
        
        # ✅ GATE 2 : Vérifier séquence de ré-intégration BB20
        if not _check_ct_reintegration_window(df, is_long=False, max_window=3):
            return None
        
        # ✅ GATE 3 : Rejeter premier signal après excès BB80
        if _is_first_after_prolonged_bb80_exit(df, is_long=False, min_streak=5, lookback=50):
            return None
        
        # ✅ GATE 4 : Validation mèche
        if not _validate_wick_direction(df, is_long=False, lookback=3):
            return None
        
        # Calcul niveaux
        entry = close_price
        sl = bb20_up
        tp = bb80_lo
        
        # Vérification RR basique
        distance_to_sl = abs(sl - entry)
        distance_to_tp = abs(entry - tp)
        
        if distance_to_sl < 1e-10:
            return None
        
        rr_raw = distance_to_tp / distance_to_sl
        
        return {
            'side': 'sell',
            'regime': 'Trend-Following',
            'entry': entry,
            'sl': sl,
            'tp': tp,
            'rr': rr_raw,
            'ts': int(current.name.timestamp() * 1000) if hasattr(current.name, 'timestamp') else int(time.time() * 1000),
            'reason': 'TF Short validé (4 gates)'
        }
    
    # ============================================================================
    # DÉTECTION CONTRE-TENDANCE LONG (prix sous MM80, rebond vers le haut)
    # ============================================================================
    
    if is_clearly_below_mm80 or (is_in_dead_zone and trend_bias == 'down'):
        # ✅ GATE 1 : Contact avec BB80 (sortie vers le bas)
        if not is_below_bb80:
            return None
        
        # ✅ GATE 2 : Validation séquence stricte
        if not _check_ct_reintegration_window(df, is_long=True, max_window=3):
            return None
        
        # ✅ GATE 3 : Excès prolongé BB80
        if _is_first_after_prolonged_bb80_exit(df, is_long=True, min_streak=5, lookback=50):
            return None
        
        # ✅ GATE 4 : Validation mèche
        if not _validate_wick_direction(df, is_long=True, lookback=3):
            return None
        
        # Calcul niveaux
        entry = close_price
        sl = bb80_lo
        tp = mm20
        
        # Vérification RR basique
        distance_to_sl = abs(entry - sl)
        distance_to_tp = abs(tp - entry)
        
        if distance_to_sl < 1e-10:
            return None
        
        rr_raw = distance_to_tp / distance_to_sl
        
        return {
            'side': 'buy',
            'regime': 'Counter-Trend',
            'entry': entry,
            'sl': sl,
            'tp': tp,
            'rr': rr_raw,
            'ts': int(current.name.timestamp() * 1000) if hasattr(current.name, 'timestamp') else int(time.time() * 1000),
            'reason': 'CT Long validé (4 gates)'
        }
    
    # ============================================================================
    # DÉTECTION CONTRE-TENDANCE SHORT (prix au-dessus MM80, rebond vers le bas)
    # ============================================================================
    
    if is_clearly_above_mm80 or (is_in_dead_zone and trend_bias == 'up'):
        # ✅ GATE 1 : Contact avec BB80 (sortie vers le haut)
        if not is_above_bb80:
            return None
        
        # ✅ GATE 2 : Validation séquence stricte
        if not _check_ct_reintegration_window(df, is_long=False, max_window=3):
            return None
        
        # ✅ GATE 3 : Excès prolongé BB80
        if _is_first_after_prolonged_bb80_exit(df, is_long=False, min_streak=5, lookback=50):
            return None
        
        # ✅ GATE 4 : Validation mèche
        if not _validate_wick_direction(df, is_long=False, lookback=3):
            return None
        
        # Calcul niveaux
        entry = close_price
        sl = bb80_up
        tp = mm20
        
        # Vérification RR basique
        distance_to_sl = abs(sl - entry)
        distance_to_tp = abs(entry - tp)
        
        if distance_to_sl < 1e-10:
            return None
        
        rr_raw = distance_to_tp / distance_to_sl
        
        return {
            'side': 'sell',
            'regime': 'Counter-Trend',
            'entry': entry,
            'sl': sl,
            'tp': tp,
            'rr': rr_raw,
            'ts': int(current.name.timestamp() * 1000) if hasattr(current.name, 'timestamp') else int(time.time() * 1000),
            'reason': 'CT Short validé (4 gates)'
        }
    
    # Aucun signal valide
    return None


def scan_symbol_for_signals(ex: ccxt.Exchange, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
    """
    Charge le DF préparé, appelle detect_signal(), enregistre le signal et notifie.
    Différencie désormais clairement :
      - PENDING : dès qu’un signal est détecté
      - VALID / SKIPPED : mis à jour plus tard par execute_trade() selon issue d’exécution
    Retourne le signal si valide, sinon None.
    """
    try:
        df = utils.fetch_and_prepare_df(ex, symbol, timeframe)
        if df is None or len(df) < 81:
            return None

        sig = detect_signal(symbol, df)
        ts = int(time.time() * 1000)

        if not sig:
            # Traçage léger optionnel (SKIPPED) — tu peux commenter si tu ne veux pas de logs
            try:
                record_signal_from_trader(
                    symbol=symbol, side="-", timeframe=timeframe, ts=ts,
                    price=float(df.iloc[-1]["close"]), rr=0.0, regime="-",
                    pattern="NONE", status="SKIPPED", meta={"reason": "no_pattern"}
                )
            except Exception:
                pass
            return None

        # ✅ marquer immédiatement le signal comme PENDING (en attente d’exécution)
        sig["ts"] = ts
        try:
            record_signal_from_trader(
                symbol=symbol,
                side=sig.get("side", "-"),
                timeframe=timeframe,
                ts=ts,
                price=float(sig.get("entry") or df.iloc[-1]["close"]),
                rr=float(sig.get("rr", 0.0)),
                regime=str(sig.get("regime", "-")),
                pattern="AUTO",
                status="PENDING",
                meta={"entry": sig.get("entry"), "tp": sig.get("tp"), "sl": sig.get("sl")}
            )
        except Exception:
            pass

        try:
            notifier.send_signal_notification(symbol, timeframe, sig)
        except Exception:
            pass

        return sig
    except Exception:
        return None


# ==============================================================================
# LOGIQUE D'EXÉCUTION (Améliorée)
# ==============================================================================

def get_account_balance_usdt(ex=None) -> Optional[float]:
    """
    Retourne le solde total en USDT (et le met en cache dans settings.CURRENT_BALANCE_USDT).
    Supporte Bybit/Bitget via ccxt.fetchBalance().
    
    ✅ CORRECTION : Erreurs silencieuses (logs console uniquement)
    """
    try:
        if ex is None and hasattr(globals(), "create_exchange"):
            ex = create_exchange()
        if ex is None:
            return None

        bal = ex.fetch_balance()
        if bal is None:
            return None

        total = None

        # Méthode 1 : Essais robustes sur clés standard
        for key in ("USDT", "usdT", "usdt"):
            try:
                wallet = bal.get(key) or {}
                total = wallet.get("total") or wallet.get("free") or wallet.get("used")
                if total is not None:
                    total = float(total)
                    break
            except Exception:
                continue

        # Méthode 2 : Bitget/Bybit dérivés via 'info'
        if total is None:
            info = bal.get("info") or {}
            
            # Bybit v5: result.list
            try:
                if isinstance(info, dict) and "result" in info:
                    result = info["result"]
                    if isinstance(result, dict) and "list" in result:
                        for acc in result["list"]:
                            if str(acc.get("coin", "")).upper() == "USDT":
                                total = float(acc.get("walletBalance", 0))
                                break
            except Exception:
                pass
            
            # Bitget: data list
            if total is None:
                try:
                    data = info.get("data") if isinstance(info, dict) else None
                    if isinstance(data, list):
                        for acc in data:
                            if str(acc.get("marginCoin", "")).upper() == "USDT":
                                available = float(acc.get("available", 0))
                                frozen = float(acc.get("frozen", 0))
                                total = available + frozen
                                break
                except Exception:
                    pass
            
            # Bitget: info direct (alternative)
            if total is None and isinstance(info, dict):
                try:
                    # Certaines versions Bitget retournent directement
                    if "availableBalance" in info:
                        total = float(info["availableBalance"])
                    elif "equity" in info:
                        total = float(info["equity"])
                except Exception:
                    pass

        if total is None:
            return None

        total = float(total)
        
        try:
            database.set_setting('CURRENT_BALANCE_USDT', f"{total:.6f}")
        except Exception:
            pass

        return total

    except Exception as e:
        # ✅ CORRECTION : Log console uniquement (pas de notification Telegram)
        print(f"⚠️ [get_account_balance_usdt] Erreur : {e}")
        return None

def clear_balance_cache():
    """
    Invalide le cache du solde USDT stocké en DB.
    
    Appelé après:
    - Ouverture position (capital utilisé)
    - Fermeture position (capital libéré)
    - Pyramiding (ajout capital)
    - Partial exit (récupération partielle capital)
    
    Force un recalcul frais lors du prochain appel à get_account_balance_usdt().
    """
    try:
        database.set_setting('CURRENT_BALANCE_USDT', '0.0')
    except Exception:
        pass        

def _import_exchange_position_to_db(ex: ccxt.Exchange, symbol: str, side: str, quantity: float, entry_px: float) -> None:
    """
    Crée/insère en DB une position détectée sur l'exchange mais absente de la DB.
    Regime = 'Importé', TP/SL init = entry_px (seront gérés ensuite par manage_open_positions).
    """
    try:
        management_strategy = str(database.get_setting('STRATEGY_MODE', 'NORMAL')).upper()
        entry_atr = 0.0
        try:
            df_tmp = utils.fetch_and_prepare_df(ex, symbol, TIMEFRAME)
            if df_tmp is not None and len(df_tmp) > 0:
                entry_atr = float(df_tmp.iloc[-1].get('atr', 0.0))
        except Exception:
            pass

        database.create_trade(
            symbol=symbol,
            side=side,
            regime="Importé",
            entry_price=float(entry_px),
            sl_price=float(entry_px),
            tp_price=float(entry_px),
            quantity=float(quantity),
            risk_percent=RISK_PER_TRADE_PERCENT,
            management_strategy=("SPLIT" if management_strategy == "SPLIT" else "NORMAL"),
            entry_atr=entry_atr,
            entry_rsi=0.0,
        )
        try:
            notifier.tg_send(f"♻️ Import DB: {symbol} {side} qty≈{quantity}, entry≈{entry_px}")
        except Exception:
            pass
    except Exception as e:
        notifier.tg_send_error(f"Import position {symbol} -> DB", e)
        

def _estimate_pnl_for_closed_trade(ex, row: Dict[str, Any]) -> float:
    """
    Estime le PnL d'un trade fermé côté exchange alors que la DB pense encore qu'il est ouvert.
    Utilisé uniquement dans sync_positions_with_exchange lorsque l'exchange est FLAT pour un symbole.

    Approche:
      - side: 'buy'/'sell'
      - entry: row['entry_price'] (ou 'entry')
      - qty:   row['quantity'] (ou 'qty')
      - exit:  row['exit_price']/'close_price' si présent, sinon dernier prix du ticker.

    Si une info clé manque → retourne 0.0 (comportement précédent).
    """
    try:
        symbol = str(row.get("symbol") or "")
        if not symbol:
            return 0.0

        side_raw = str(row.get("side") or "").lower()
        side = "buy" if side_raw in ("buy", "long") else "sell" if side_raw in ("sell", "short") else ""
        if not side:
            return 0.0

        entry = row.get("entry_price", row.get("entry"))
        qty = row.get("quantity", row.get("qty"))

        entry = float(entry or 0.0)
        qty = float(qty or 0.0)
        if entry <= 0.0 or qty <= 0.0:
            return 0.0

        # Prix de sortie: on privilégie ce qui est déjà stocké en DB
        exit_price = row.get("exit_price") or row.get("close_price") or row.get("avg_exit_price")
        if exit_price is None and ex is not None:
            try:
                t = ex.fetch_ticker(symbol) or {}
                exit_price = t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0
            except Exception:
                exit_price = 0.0
        exit_price = float(exit_price or 0.0)
        if exit_price <= 0.0:
            return 0.0

        if side == "buy":
            pnl = (exit_price - entry) * qty
        else:
            pnl = (entry - exit_price) * qty
        return float(pnl)
    except Exception:
        return 0.0

def sync_positions_with_exchange(ex) -> None:
    """
    Synchronise la table trades avec L’EXCHANGE COMME SOURCE DE VÉRITÉ (agrégation par symbole).
    - 1 seul trade OPEN par symbole côté DB (on agrège et on ferme les doublons).
    - Si exchange est flat pour un symbole ⇒ on ferme en DB (CLOSED_BY_EXCHANGE) en estimant le PnL.
    - Si exchange a une position et DB n’en a pas ⇒ on crée (regime='Importé').
    - On met à jour side/quantity/entry_price pour refléter l’exchange.
    - On recopie TP/SL depuis les ordres ouverts exchange si disponibles (sans créer/modifier les ordres ici).
    """
    try:
        if ex is None and hasattr(globals(), "create_exchange"):
            ex = create_exchange()
        if ex is None:
            return

        # --- Positions réelles exchange (nettes) ---
        ex_positions = _fetch_positions_safe(ex, None) or []
        # Normalise par symbole → une seule entrée par symbole avec side/qty/entry
        ex_map: Dict[str, Dict[str, Any]] = {}
        for p in ex_positions:
            sym = p.get("symbol")
            if not sym:
                continue
            raw_size = float(p.get("size") or p.get("contracts") or p.get("positionAmt") or 0.0)
            if raw_size == 0:
                continue
            side = p.get("side") or ("long" if raw_size > 0 else "short")
            qty = abs(raw_size)
            entry = float(p.get("entryPrice") or 0.0)
            # Canonise side pour notre DB: 'buy'/'sell'
            side_db = "buy" if str(side).lower() in ("long", "buy") else "sell"
            ex_map[sym] = {"symbol": sym, "side": side_db, "qty": qty, "entry": entry}

        # --- DB: liste des OPEN ---
        db_open = database.get_open_positions() or []
        # Grouper par symbole
        db_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
        for r in db_open:
            db_by_symbol.setdefault(r.get("symbol", ""), []).append(r)

        # Ensemble des symboles impliqués
        symbols_all = set(db_by_symbol.keys()) | set(ex_map.keys())

        for sym in symbols_all:
            ex_info = ex_map.get(sym)            # None si flat côté exchange
            db_list = db_by_symbol.get(sym, [])  # [] si pas de trade DB

            # --- Cas A: exchange FLAT, DB a des OPEN → fermer tous en DB (avec PnL estimé)
            if ex_info is None and db_list:
                for row in db_list:
                    try:
                        estimated_pnl = _estimate_pnl_for_closed_trade(ex, row)
                    except Exception:
                        estimated_pnl = 0.0
                    try:
                        database.close_trade(
                            int(row["id"]),
                            status="CLOSED_BY_EXCHANGE",
                            pnl=float(estimated_pnl),
                        )
                    except Exception:
                        # fallback: ancien comportement (pnl=0)
                        try:
                            database.close_trade(
                                int(row["id"]),
                                status="CLOSED_BY_EXCHANGE",
                                pnl=0.0,
                            )
                        except Exception:
                            pass
                continue

            # --- Cas B: exchange a une position, DB n’a rien → créer + recopie TP/SL si trouvés
            if ex_info is not None and not db_list:
                try:
                    # Crée un trade importé
                    database.create_trade(
                        symbol=sym,
                        side=ex_info["side"],
                        regime="Importé",
                        entry_price=float(ex_info["entry"] or 0.0),
                        sl_price=float(ex_info["entry"] or 0.0),
                        tp_price=float(ex_info["entry"] or 0.0),
                        quantity=float(ex_info["qty"] or 0.0),
                        risk_percent=RISK_PER_TRADE_PERCENT,
                        management_strategy=str(database.get_setting('STRATEGY_MODE', 'NORMAL') or 'NORMAL'),
                        entry_atr=0.0,
                        entry_rsi=0.0,
                    )
                except Exception:
                    pass
                # Recopie TP/SL éventuels depuis les ordres
                try:
                    tp_ex, sl_ex = _fetch_existing_tp_sl(ex, sym)
                    if tp_ex or sl_ex:
                        # retrouver le trade nouvellement créé (le plus récent pour ce symbole)
                        fresh = [t for t in database.get_open_positions() if t.get("symbol") == sym]
                        if fresh:
                            keep = max(fresh, key=lambda x: int(x.get("open_timestamp") or 0))
                            if tp_ex:
                                database.update_trade_tp(int(keep["id"]), float(tp_ex))
                            if sl_ex:
                                try:
                                    database.update_trade_sl(int(keep["id"]), float(sl_ex))
                                except AttributeError:
                                    database.update_trade_to_breakeven(
                                        int(keep["id"]),
                                        float(keep.get("quantity") or 0.0),
                                        float(sl_ex),
                                    )
                except Exception:
                    pass
                continue

            # --- Cas C: exchange a une position, DB a ≥1 OPEN → agrège: on garde 1, on ferme les autres
            if ex_info is not None and db_list:
                # Sélectionne le "keeper": le plus récent (open_timestamp) puis id
                try:
                    keeper = max(db_list, key=lambda x: (int(x.get("open_timestamp") or 0), int(x.get("id") or 0)))
                except Exception:
                    keeper = db_list[0]
                keep_id = int(keeper["id"])

                # Ferme les doublons
                for row in db_list:
                    rid = int(row["id"])
                    if rid == keep_id:
                        continue
                    try:
                        database.close_trade(rid, status='MERGED_BY_SYNC', pnl=0.0)
                    except Exception:
                        pass

                # Met à jour le trade conservé pour refléter l’exchange (side/qty/entry)
                try:
                    database.update_trade_core(
                        trade_id=keep_id,
                        side=str(ex_info["side"]),
                        entry_price=float(ex_info["entry"] or 0.0),
                        quantity=float(ex_info["qty"] or 0.0),
                        regime=keeper.get("regime") or "Importé"
                    )
                except Exception:
                    pass

                # Recopie TP/SL si présents sur l’exchange
                try:
                    tp_ex, sl_ex = _fetch_existing_tp_sl(ex, sym)
                    if tp_ex:
                        database.update_trade_tp(keep_id, float(tp_ex))
                    if sl_ex:
                        try:
                            database.update_trade_sl(keep_id, float(sl_ex))
                        except AttributeError:
                            database.update_trade_to_breakeven(
                                keep_id,
                                float(ex_info["qty"] or 0.0),
                                float(sl_ex),
                            )
                except Exception:
                    pass

        # Optionnel: cas exotiques déjà couverts par la clé exacte 'symbol'

    except Exception as e:
        print(f"[sync_positions_with_exchange] error: {e}")
            

def _validate_tp_for_side(side: str, tp_price: float, current_price: float, tick_size: float) -> float:
    """
    Garantit la règle Bitget :
      - short  (sell) : TP < current_price
      - long   (buy)  : TP > current_price
    Corrige automatiquement de 1 tick si la condition est violée.
    """
    if tick_size <= 0:
        return tp_price

    def _round_to_tick(px: float) -> float:
        # Arrondi au tick vers la grille la plus proche
        ticks = round(px / tick_size)
        return float(ticks) * float(tick_size)

    if str(side).lower() in ("sell", "short"):
        # si TP >= prix courant, pousse-le sous le marché d'un tick
        if tp_price >= current_price:
            tp_price = _round_to_tick(current_price - tick_size)
    else:
        # buy/long : si TP <= prix courant, pousse-le au-dessus d'un tick
        if tp_price <= current_price:
            tp_price = _round_to_tick(current_price + tick_size)

    return tp_price

def _bitget_tick_size(market: dict) -> float:
    """
    Retourne le tick_size Bitget à partir du market ccxt.
    Essaie d'abord market['limits']['price']['min'] si présent, sinon precision->price.
    """
    try:
        lim = market.get("limits", {}).get("price", {})
        if isinstance(lim.get("min"), (int, float)) and lim["min"] > 0:
            return float(lim["min"])
    except Exception:
        pass
    # fallback via precision décimale
    prec = None
    try:
        prec = market.get("precision", {}).get("price", None)
    except Exception:
        prec = None
    if isinstance(prec, int) and prec >= 0:
        return 10 ** (-prec) if prec > 0 else 1.0
    # dernier recours : 1e-4
    return 0.0001

def _prepare_validated_tp(exchange, symbol: str, side: str, raw_tp: float) -> float:
    ticker = exchange.fetch_ticker(symbol) or {}
    current_price = float(
        ticker.get("last") or
        ticker.get("close") or
        (ticker.get("info") or {}).get("last", 0) or
        0
    )
    market = exchange.market(symbol) or {}
    tick_size = _bitget_tick_size(market)
    return _validate_tp_for_side(side, float(raw_tp), current_price, tick_size)
    
# --- à placer près de _prepare_validated_tp / _bitget_tick_size ---

def _current_mark_price(exchange, symbol: str) -> float:
    """Renvoie un proxy du 'current price' pertinent pour les triggers mark."""
    t = exchange.fetch_ticker(symbol) or {}
    info = t.get("info") or {}
    # Plusieurs clés possibles selon ccxt/route
    for k in ("markPrice", "mark", "indexPrice", "last", "close", "bid", "ask"):
        v = info.get(k) if k in info else t.get(k)
        if v:
            try: return float(v)
            except Exception: pass
    return float(t.get("last") or t.get("close") or 0.0)

def _validate_sl_for_side(side: str, sl_price: float, current_mark: float, tick_size: float) -> float:
    """
    Bitget STRICT : 
      - SHORT (sell) : SL > current_mark (protection au-dessus)
      - LONG (buy)   : SL < current_mark (protection en-dessous)
    
    Ajoute 5 ticks de marge de sécurité (au lieu de 2) pour éviter rejets.
    """
    if tick_size <= 0: 
        tick_size = 0.0001
    
    # Marge de sécurité augmentée : 5 ticks au lieu de 2
    safety_margin = 5.0 * tick_size
    
    side_clean = str(side).lower().strip()
    
    if side_clean in ("sell", "short"):
        # SHORT : SL doit être > current_mark
        if sl_price <= current_mark:
            sl_price = current_mark + safety_margin
        # Double vérification après ajustement
        elif sl_price <= current_mark + tick_size:
            sl_price = current_mark + safety_margin
    else:
        # LONG : SL doit être < current_mark
        if sl_price >= current_mark:
            sl_price = current_mark - safety_margin
        # Double vérification après ajustement
        elif sl_price >= current_mark - tick_size:
            sl_price = current_mark - safety_margin
    
    return sl_price

def _place_sl_tp_safe(ex, symbol: str, side: str, qty: float, sl: Optional[float], tp: Optional[float], 
                      params: dict, is_long: bool, tick_size: float) -> tuple:
    """
    Place SL et TP de manière robuste avec détection des erreurs Bitget.
    
    ✅ CORRECTION : Continue à placer le TP même si le SL échoue
    ✅ Notifie si TP non placé (changement d'avis utilisateur)
    
    Returns:
        (sl_success: bool, tp_success: bool)
    """
    sl_ok = False
    tp_ok = False
    
    # Récupérer mark price pour validation
    try:
        mark = _current_mark_price(ex, symbol)
    except Exception:
        mark = 0.0
    
    # ========================================================================
    # ========== PLACEMENT SL ==========
    # ========================================================================
    
    if sl and qty > 0:
        try:
            # Validation STRICTE avant envoi
            if mark > 0:
                sl_validated = _validate_sl_for_side(
                    ('buy' if is_long else 'sell'),
                    float(sl),
                    mark,
                    tick_size
                )
            else:
                sl_validated = float(sl)
            
            # Vérification finale des règles Bitget
            sl_invalid = False
            
            if is_long:
                # LONG : SL < mark
                if sl_validated >= mark:
                    print(f"⚠️ {symbol} LONG : SL {sl_validated:.6f} >= mark {mark:.6f} → skip SL")
                    sl_invalid = True
            else:
                # SHORT : SL > mark
                if sl_validated <= mark:
                    print(f"⚠️ {symbol} SHORT : SL {sl_validated:.6f} <= mark {mark:.6f} → skip SL")
                    sl_invalid = True
            
            # ✅ CORRECTION : Ne pas placer le SL si invalide, mais CONTINUER vers le TP
            if not sl_invalid:
                # Placement SL
                sl_side = 'sell' if is_long else 'buy'
                
                try:
                    ex.create_order(
                        symbol, 'market', sl_side, qty, price=None,
                        params={**params, 'stopLossPrice': float(sl_validated), 'triggerType': 'mark'}
                    )
                    sl_ok = True
                    print(f"✅ {symbol} : SL placé à {sl_validated:.6f}")
                
                except Exception as e_sl:
                    err_msg = str(e_sl)
                    # Détection erreur 40836 (SL invalide)
                    if '40836' in err_msg or 'stop loss price' in err_msg.lower():
                        print(f"⚠️ {symbol} : SL invalide (40836) → SL skippé, mais TP va être tenté")
                    else:
                        print(f"❌ {symbol} : Erreur SL → {e_sl}")
            
            else:
                print(f"⚠️ {symbol} : SL invalide (règles Bitget) → SL skippé, mais TP va être tenté")
        
        except Exception as e:
            print(f"❌ {symbol} : Erreur validation SL → {e}")
            # ✅ IMPORTANT : Ne pas return ici, continuer vers le TP
    
    # ========================================================================
    # ========== PLACEMENT TP ==========
    # ========================================================================
    
    if tp and qty > 0:
        try:
            tp_side = 'sell' if is_long else 'buy'
            
            # Validation TP
            try:
                tp_validated = _prepare_validated_tp(ex, symbol, tp_side, float(tp))
            except Exception as e_val:
                print(f"⚠️ {symbol} : Erreur validation TP → {e_val}")
                tp_validated = float(tp)
            
            # Placement TP
            try:
                ex.create_order(
                    symbol, 'market', tp_side, qty, price=None,
                    params={**params, 'takeProfitPrice': float(tp_validated), 'triggerType': 'mark'}
                )
                tp_ok = True
                print(f"✅ {symbol} : TP placé à {tp_validated:.6f}")
            
            except Exception as e_tp:
                err_msg = str(e_tp)
                
                # Log détaillé pour debug
                print(f"❌ {symbol} : Erreur placement TP")
                print(f"   Prix TP : {tp_validated:.6f}")
                print(f"   Prix mark : {mark:.6f}")
                print(f"   Quantité : {qty:.6f}")
                print(f"   Erreur : {err_msg}")
                
                # ✅ Notifier si erreur critique (remis suite changement d'avis)
                if '40836' in err_msg or 'take profit price' in err_msg.lower():
                    try:
                        notifier.tg_send(
                            f"⚠️ **TP NON PLACÉ**\n\n"
                            f"{symbol} {side.upper()}\n"
                            f"Prix TP : {tp_validated:.6f}\n"
                            f"Prix mark : {mark:.6f}\n"
                            f"Erreur : TP invalide (40836)\n\n"
                            f"❗ Placer le TP manuellement"
                        )
                    except Exception:
                        pass
        
        except Exception as e:
            print(f"❌ {symbol} : Erreur TP → {e}")
    
    # ========================================================================
    # ========== RÉSUMÉ ==========
    # ========================================================================
    
    if sl and qty > 0 and not sl_ok:
        print(f"⚠️ {symbol} : SL NON placé")
    
    if tp and qty > 0 and not tp_ok:
        print(f"⚠️ {symbol} : TP NON placé")
        
        # ✅ NOTIFICATION TELEGRAM si TP échoue (remis suite changement d'avis)
        try:
            notifier.tg_send(
                f"⚠️ **ALERTE TP**\n\n"
                f"🎯 {symbol}\n"
                f"TP non placé sur l'exchange\n\n"
                f"{'✅' if sl_ok else '❌'} SL : {sl:.6f if sl else 'N/A'}\n"
                f"❌ TP : {tp:.6f if tp else 'N/A'}\n\n"
                f"❗ Vérifier et placer manuellement"
            )
        except Exception:
            pass
    
    return sl_ok, tp_ok

def _extract_tp_sl_from_orders(orders: list) -> Tuple[Optional[float], Optional[float]]:
    """Retourne (tp_price, sl_price) détectés dans les ordres ouverts."""
    tp, sl = None, None
    for o in (orders or []):
        info = o.get('info', {}) or {}
        # Essais multi-champs (Bitget/ccxt)
        for k in ('takeProfitPrice', 'tpTriggerPx', 'tpTriggerPrice', 'tpPrice', 'takeProfit'):
            v = info.get(k) or o.get(k)
            if v and tp is None:
                try: tp = float(v)
                except Exception: pass
        for k in ('stopLossPrice', 'slTriggerPx', 'slTriggerPrice', 'slPrice', 'stopLoss', 'stopPrice'):
            v = info.get(k) or o.get(k)
            if v and sl is None:
                try: sl = float(v)
                except Exception: pass
    return tp, sl


def _fetch_existing_tp_sl(exchange, symbol: str) -> Tuple[Optional[float], Optional[float]]:
    """Lit les ordres ouverts et tente d’en extraire TP/SL courants."""
    try:
        orders = exchange.fetch_open_orders(symbol)
    except Exception:
        orders = []
    return _extract_tp_sl_from_orders(orders)


def _apply_manual_override_if_needed(ex, pos: Dict[str, Any], tick_size: float) -> Dict[str, bool]:
    """
    Si l’utilisateur a déplacé TP/SL à la main, aligne la DB sur l’exchange
    et renvoie des flags pour la gestion.
    """
    symbol = pos['symbol']
    is_long = (pos['side'] == 'buy')
    tp_ex, sl_ex = _fetch_existing_tp_sl(ex, symbol)

    changed_tp = False
    changed_sl = False
    eps = max(float(tick_size), 1e-12)

    # TP
    if tp_ex:
        try:
            tp_db = float(pos.get('tp_price') or 0.0)
            if abs(float(tp_ex) - tp_db) > eps / 2:
                database.update_trade_tp(pos['id'], float(tp_ex))
                pos['tp_price'] = float(tp_ex)
                changed_tp = True
        except Exception:
            pass

    # SL
    if sl_ex:
        try:
            sl_db = float(pos.get('sl_price') or pos['entry_price'])
            if abs(float(sl_ex) - sl_db) > eps / 2:
                try:
                    database.update_trade_sl(pos['id'], float(sl_ex))
                except AttributeError:
                    database.update_trade_to_breakeven(pos['id'], float(pos['quantity']), float(sl_ex))
                pos['sl_price'] = float(sl_ex)
                changed_sl = True
        except Exception:
            pass

    # Si le SL manuel est déjà >= BE (long) ou <= BE (short), marque BE actif
    try:
        be_side = 'long' if is_long else 'short'
        be_price = compute_fee_safe_be_price(
            entry=float(pos['entry_price']),
            side=be_side,
            qty=float(pos['quantity']),
            fee_in_pct=FEE_ENTRY_PCT,
            fee_out_pct=FEE_EXIT_PCT,
            buffer_pct=BE_BUFFER_PCT,
            buffer_usdt=BE_BUFFER_USDT
        )
        sl_cur = float(pos.get('sl_price') or pos['entry_price'])
        if (is_long and sl_cur >= be_price - eps) or ((not is_long) and sl_cur <= be_price + eps):
            # Active le statut BE en DB pour éviter que le bloc BE “re-calcule”
            try:
                database.update_trade_to_breakeven(pos['id'], float(pos['quantity']), float(sl_cur))
                pos['breakeven_status'] = 'ACTIVE'
            except Exception:
                pass
    except Exception:
        pass

    return {"tp_changed": changed_tp, "sl_changed": changed_sl}

def _bitget_positions_params() -> Dict[str, str]:
    return {"productType": "USDT-FUTURES", "marginCoin": "USDT"}


def _ensure_bitget_mix_options(ex: ccxt.Exchange) -> None:
    """
    Idempotent: prépare l'instance ccxt pour Bitget “mix” USDT linéaire.
    - defaultType='swap' (perp)
    - defaultSubType='linear' (USDT)
    - initialise ex.params avec des valeurs sûres (subType/productType)
    - désactive l'obligation 'price' pour les BUY market (amount ≡ cost)
    Ne lève jamais d’exception.
    """
    try:
        if not hasattr(ex, "options") or ex.options is None:
            ex.options = {}

        # Perp par défaut
        if ex.options.get("defaultType") != "swap":
            ex.options["defaultType"] = "swap"

        if getattr(ex, "id", "") == "bitget":
            # Linear (USDT) par défaut
            if ex.options.get("defaultSubType") not in ("linear", "inverse"):
                ex.options["defaultSubType"] = "linear"
            ex.options.setdefault("defaultSettle", "USDT")
            # ❗ clé pour éviter l'erreur ccxt sur les market BUY
            ex.options["createMarketBuyOrderRequiresPrice"] = False

        # Paramètres génériques pour les requêtes
        if not hasattr(ex, "params") or ex.params is None:
            ex.params = {}
        if getattr(ex, "id", "") == "bitget":
            ex.params.setdefault("subType", ex.options.get("defaultSubType", "linear"))
            ex.params.setdefault("productType", "USDT-UMCBL")
    except Exception:
        pass

def create_market_order_smart(ex: ccxt.Exchange, symbol: str, side: str, amount: float,
                              ref_price: Optional[float] = None,
                              params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Envoi MARKET robuste.
    - Bitget BUY: `amount` est interprété comme COÛT (USDT). On convertit qty→cost via ref_price|ticker.
    - Autres cas: inchangé (amount = quantité).
    """
    _ensure_bitget_mix_options(ex)
    params = params or {}

    s = (side or "").lower()
    exid = getattr(ex, "id", "")

    if exid == "bitget" and s == "buy":
        px = None
        if ref_price is not None:
            try:
                px = float(ref_price)
            except Exception:
                px = None
        if px is None or px <= 0:
            try:
                t = ex.fetch_ticker(symbol) or {}
                px = float(t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0)
            except Exception:
                px = 0.0
        cost = float(amount) * float(px) if px and px > 0 else float(amount)
        return ex.create_order(symbol, "market", "buy", float(cost), None, params)

    return ex.create_order(symbol, "market", side, float(amount), None, params)


def _resolve_bitget_route(exchange):
    try:
        if not hasattr(exchange, "options") or not isinstance(exchange.options, dict):
            exchange.options = {}
        exchange.options["defaultType"] = "swap"
        exchange.options["defaultSettle"] = "USDT"
        exchange.options["productType"] = "USDT-FUTURES"
    except Exception:
        pass

    return {
        "marginCoin": "USDT",
        "primary": "USDT-FUTURES",
    }

def _fetch_positions_safe(ex: ccxt.Exchange, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Récupère les positions ouvertes de façon robuste.
    - Supporte Bitget (swap USDT) et fallback silencieux si non supporté.
    - Ne lève pas d’exception : retourne [] en cas d’échec.
    """
    try:
        # Certaines implémentations exigent les marchés chargés
        try:
            if not getattr(ex, "markets", None):
                ex.load_markets()
        except Exception:
            pass

        # Si la bourse ne supporte pas fetchPositions, on sort proprement
        if not getattr(ex, "has", {}).get("fetchPositions", False):
            return []

        # Appel principal (symbols peut être None)
        positions = ex.fetch_positions(symbols=symbols) if symbols is not None else ex.fetch_positions()

        # Normalisation légère / garde-fous
        out: List[Dict[str, Any]] = []
        for p in positions or []:
            try:
                sym   = p.get("symbol") or p.get("info", {}).get("symbol")
                size  = float(p.get("contracts") or p.get("contractsSize") or p.get("positionAmt") or 0.0)
                side  = p.get("side") or ("long" if size > 0 else "short" if size < 0 else None)
                entry = float(p.get("entryPrice") or p.get("averagePrice") or 0.0)
                lev   = float(p.get("leverage") or 0.0)
                upnl  = float(p.get("unrealizedPnl") or p.get("unrealizedProfit") or 0.0)
                if sym:
                    out.append({
                        "symbol": sym,
                        "side": side,
                        "size": size,
                        "entryPrice": entry,
                        "leverage": lev,
                        "unrealizedPnl": upnl,
                        "raw": p
                    })
            except Exception:
                # On skippe les lignes corrompues sans casser le flux
                continue
        return out
    except Exception:
        return []

def _cancel_all_orders_safe(ex: ccxt.Exchange, symbol: str) -> None:
    """
    Annule TOUS les ordres ouverts sur un symbole de manière robuste.
    
    Utilisé pour nettoyer les ordres restants (TP/SL/BE) après:
    - Fermeture manuelle position
    - Détection position fermée par exchange
    - Sync positions
    
    Ne lève JAMAIS d'exception (fail-safe).
    
    Args:
        ex: Exchange
        symbol: Symbole à nettoyer
    """
    try:
        # Récupérer tous les ordres ouverts
        orders = ex.fetch_open_orders(symbol)
        
        if not orders:
            return
        
        # Annuler chaque ordre individuellement
        for order in orders:
            try:
                order_id = order.get('id')
                if order_id:
                    ex.cancel_order(order_id, symbol)
            except Exception:
                # Skip silencieusement si ordre déjà annulé/exécuté
                continue
    
    except Exception:
        # Fail-safe : ne jamais casser l'exécution
        pass

def _fetch_balance_safe(exchange):
    """
    Récupère le solde de manière robuste.
    
    ✅ CORRECTION : Erreurs silencieuses (pas de spam Telegram)
    """
    try:
        exchange.load_markets()
    except Exception:
        pass

    try:
        if getattr(exchange, "id", "") == "bitget":
            from os import getenv
            if not hasattr(exchange, "options") or not isinstance(exchange.options, dict):
                exchange.options = {}
            exchange.options["defaultType"] = "swap"

            margin_coin = (getenv("MARGIN_COIN", "USDT") or "USDT").strip().upper()
            try:
                db_margin = str(database.get_setting("MARGIN_COIN", "")).strip().upper()
                if db_margin:
                    margin_coin = db_margin
            except Exception:
                pass
            exchange.options["defaultSettle"] = margin_coin
            exchange.options["productType"] = "USDT-FUTURES" if margin_coin in ("USDT", "USDC") else "COIN-FUTURES"

            pt_candidates = ["USDT-FUTURES"] if margin_coin in ("USDT", "USDC") else ["COIN-FUTURES"]

            last_err = None
            for pt in pt_candidates:
                try:
                    bal = exchange.fetch_balance({"type": "swap", "productType": pt, "marginCoin": margin_coin})
                    if bal:
                        return bal
                except Exception as e:
                    last_err = e
                    continue

            # ✅ CORRECTION : Erreur SILENCIEUSE (pas de notification Telegram)
            # Ancienne ligne commentée :
            # try:
            #     notifier.tg_send(f"❌ Erreur: Récupération du solde\nbitget {str(last_err)}")
            # except Exception:
            #     pass
            
            # Log console uniquement
            print(f"⚠️ [_fetch_balance_safe] Erreur Bitget : {last_err}")
            return {}

        bal = exchange.fetch_balance()
        return bal if bal else {}
    
    except Exception as e:
        # ✅ CORRECTION : Erreur SILENCIEUSE (pas de notification Telegram)
        # Ancienne ligne commentée :
        # try:
        #     notifier.tg_send(f"❌ Erreur: Récupération du solde\n{getattr(exchange,'id','')} {str(e)}")
        # except Exception:
        #     pass
        
        # Log console uniquement
        print(f"⚠️ [_fetch_balance_safe] Erreur {getattr(exchange,'id','')} : {e}")
        return {}

def get_portfolio_equity_usdt(exchange) -> float:
    """
    Renvoie l'équity totale convertie en USDT pour l'affichage/statistiques.
    - Bitget: privilégie les champs usdtEquity / totalEquity / equity dans info.data.
    - Fallback: utilise les champs normalisés CCXT (USDT total/free, etc.).
    Met systématiquement à jour settings.CURRENT_BALANCE_USDT si une valeur cohérente est trouvée.
    """
    try:
        bal = _fetch_balance_safe(exchange)
    except Exception:
        return 0.0

    if not bal:
        return 0.0

    from typing import List
    candidates: List[float] = []

    # --- Cas spécifique Bitget : on privilégie l'équity portefeuille ---
    try:
        if getattr(exchange, "id", "") == "bitget":
            info = bal.get("info") or {}
            data = info.get("data") or info.get("result") or {}

            # data dict: certains endpoints renvoient un seul bloc
            if isinstance(data, dict):
                for key in ("usdtEquity", "totalEquity", "equity"):
                    v = data.get(key)
                    if v is not None:
                        try:
                            candidates.append(float(v))
                        except Exception:
                            pass

            # data list: plusieurs comptes / coins
            elif isinstance(data, list):
                total_usdt_equity = 0.0
                for acc in data:
                    try:
                        mc = str(acc.get("marginCoin") or acc.get("marginCoinName") or "").upper()
                    except Exception:
                        mc = ""
                    # si marginCoin est renseigné, on garde uniquement les comptes USDT
                    if mc and "USDT" not in mc:
                        continue
                    v = acc.get("usdtEquity") or acc.get("totalEquity") or acc.get("equity")
                    if v is None:
                        continue
                    try:
                        total_usdt_equity += float(v)
                    except Exception:
                        continue
                if total_usdt_equity > 0:
                    candidates.append(total_usdt_equity)
    except Exception:
        # on ne casse jamais la fonction sur une bizarrerie de payload
        pass

    # --- Fallback générique CCXT (USDT total/free/used) ---
    try:
        usdt = bal.get("USDT") or bal.get("USDT:USDT")
        if isinstance(usdt, dict):
            for key in ("total", "free", "used", "availableBalance", "available"):
                v = usdt.get(key)
                if v is not None:
                    try:
                        candidates.append(float(v))
                    except Exception:
                        pass
    except Exception:
        pass

    # total['USDT'] / total['USDT:USDT']
    try:
        total = bal.get("total") or {}
        if isinstance(total, dict):
            for k in ("USDT", "USDT:USDT"):
                v = total.get(k)
                if v is not None:
                    try:
                        candidates.append(float(v))
                    except Exception:
                        pass
    except Exception:
        pass

    if not candidates:
        return 0.0

    equity = float(max(candidates))

    # Mémorisation dans settings pour réutilisation (reporting, dashboard…)
    try:
        database.set_setting("CURRENT_BALANCE_USDT", f"{equity:.6f}")
    except Exception:
        pass

    return equity

def _cap_qty_for_margin_and_filters(exchange, symbol: str, side: str, qty: float, price: float) -> Tuple[float, Dict[str, Any]]:
    """
    (MIS À JOUR) Borne la quantité par la marge disponible et respecte les filtres du marché.
    - Utilise _fetch_balance_safe() (évite l’erreur Bitget 'productType cannot be empty').
    - Retourne (qty_cappée, meta).
    """
    meta = {
        "reason": None, "available_margin": None, "leverage": None,
        "max_notional": None, "max_qty_by_margin": None,
        "min_qty": None, "qty_step": None, "min_notional": None
    }
    try:
        # 1) Marché & limites
        exchange.load_markets()
        m = exchange.market(symbol)
        limits = m.get("limits", {}) if m else {}
        amt_limits = limits.get("amount", {}) if limits else {}
        not_limits = limits.get("cost", {}) if limits else {}

        min_qty      = float(amt_limits.get("min") or 0.0)
        qty_precision = float((m.get("precision") or {}).get("amount") or 0.0)  # décimales, pas toujours un step réel
        qty_step     = 0.0  # inconnu dans ccxt pour certains marchés → on ne force pas
        min_notional = float(not_limits.get("min") or 0.0)

        meta.update({"min_qty": min_qty, "qty_step": qty_step, "min_notional": min_notional})

        # helper: floor to step (tolère step==0)
        def _floor_to_step(v: float, step: float) -> float:
            if step and step > 0:
                return (int(v / step)) * step
            return v

        # 2) Marge disponible (USDT futures) — version robuste
        bal = _fetch_balance_safe(exchange) or {}
        available = 0.0
        try:
            u = bal.get("USDT") or bal.get("USDC") or {}
            # préférer 'free' si présent, sinon 'availableBalance' / 'available'
            available = float(u.get("free") or u.get("availableBalance") or u.get("available") or 0.0)
        except Exception:
            available = 0.0
        meta["available_margin"] = available

        # 3) Levier (issu de la config globale existante)
        try:
            lev = int(LEVERAGE)
        except Exception:
            lev = 1
        meta["leverage"] = lev

        # 4) Cap par la marge (2% buffer)
        max_notional = available * lev * 0.98
        meta["max_notional"] = max_notional
        max_qty_by_margin = max_notional / float(price) if price else 0.0
        max_qty_by_margin = _floor_to_step(max_qty_by_margin, qty_step)
        meta["max_qty_by_margin"] = max_qty_by_margin

        capped_qty = min(float(qty), max_qty_by_margin) if max_qty_by_margin > 0 else 0.0

        # 5) Respect min_qty / min_notional (on ne force pas vers le haut)
        if capped_qty < min_qty or (price and capped_qty * float(price) < min_notional):
            meta["reason"] = "INSUFFICIENT_AFTER_CAP"
            return 0.0, meta

        # 6) Si on a réduit la taille
        if capped_qty < float(qty):
            meta["reason"] = "CAPPED_BY_MARGIN"

        # 7) Arrondi doux via amount_to_precision (si dispo)
        try:
            capped_qty = float(exchange.amount_to_precision(symbol, capped_qty))
        except Exception:
            pass

        return capped_qty, meta

    except Exception as e:
        # En cas d'imprévu, on ne casse jamais l'exécution: on renvoie la qty initiale telle quelle.
        meta["reason"] = f"GUARD_ERROR:{e}"
        return float(qty), meta


def place_order(exchange, symbol: str, side: str, order_type: str, qty: float,
                price: Optional[float] = None, params: Optional[Dict[str, Any]] = None):
    """
    (MODIFIÉ) Envoi d'ordre avec garde-fous:
      - Cap par marge + respect min_qty/step/min_notional
      - Annulation propre + notif TG si solde insuffisant
      - Bitget BUY market: conversion qty→cost via create_market_order_smart()
    """
    try:
        q = abs(float(qty))
        p = float(price) if price is not None else None

        ref_price = p
        if ref_price is None:
            try:
                t = exchange.fetch_ticker(symbol)
                ref_price = float(t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0)
            except Exception:
                ref_price = 0.0

        capped_qty, meta = _cap_qty_for_margin_and_filters(exchange, symbol, side, q, ref_price or 0.0)

        if capped_qty <= 0.0 and meta.get("reason") == "INSUFFICIENT_AFTER_CAP":
            try:
                need_notional = (q * (ref_price or 0.0))
                max_notional = meta.get("max_notional")
                txt = (
                    f"❌ <b>Ordre annulé</b> (solde insuffisant)\n"
                    f"• {symbol} {side.upper()} {order_type.upper()}\n"
                    f"• Notional requise: <code>{need_notional:.2f} USDT</code>\n"
                    f"• Max possible (marge): <code>{(max_notional or 0.0):.2f} USDT</code>\n"
                    f"• Levier: <code>{meta.get('leverage')}</code>\n"
                    f"• Marge dispo: <code>{(meta.get('available_margin') or 0.0):.2f} USDT</code>\n"
                    f"• Filtres marché: min_qty=<code>{meta.get('min_qty')}</code>, "
                    f"min_notional=<code>{meta.get('min_notional')}</code>\n"
                )
                notifier.tg_send(txt)
            except Exception:
                pass
            return None

        if meta.get("reason") == "CAPPED_BY_MARGIN":
            try:
                reduced_pct = (1.0 - (capped_qty / q)) * 100.0 if q > 0 else 0.0
                notifier.tg_send(
                    f"⚠️ Taille réduite par marge\n"
                    f"• {symbol} {side.upper()} {order_type.upper()}\n"
                    f"• Demandée: <code>{q}</code> → Envoyée: <code>{capped_qty}</code> "
                    f"(-{reduced_pct:.2f}%)\n"
                    f"• Levier: <code>{meta.get('leverage')}</code> | Marge dispo: <code>{(meta.get('available_margin') or 0.0):.2f} USDT</code>\n"
                )
            except Exception:
                pass

        params = params or {}
        if order_type.lower() == "market":
            # Bitget BUY ⇒ amount=cost
            return create_market_order_smart(exchange, symbol, side, capped_qty, ref_price=ref_price, params=params)
        else:
            return exchange.create_order(symbol, order_type, side, capped_qty, p, params)

    except Exception as e:
        try:
            notifier.tg_send(f"❌ Erreur: Envoi d'ordre {symbol} {side.upper()} {order_type.upper()} — {e}")
        except Exception:
            pass
        raise


def adjust_tp_for_bb_offset(raw_tp: float, side: str, atr: float = 0.0, ref_price: Optional[float] = None) -> float:
    """
    (MISE À JOUR) Offset hybride pour le TP : max(pourcentage, ATR*k).
    - ref_price: la borne visée (BB80_up/lo ou BB20_mid) pour convertir ATR en %.
    - Sans ref_price, on retombe sur l'ancien comportement (pourcentage seul).
    """
    try:
        pct = float(database.get_setting('TP_BB_OFFSET_PCT', '0.003'))  # 0.30%
    except Exception:
        pct = 0.003
    try:
        atr_k = float(database.get_setting('TP_ATR_K', '0.50'))
    except Exception:
        atr_k = 0.50

    eff_pct = pct
    if ref_price and ref_price > 0 and atr > 0:
        eff_pct = max(pct, (atr_k * float(atr)) / float(ref_price))

    s = (side or "").lower()
    if s in ("buy", "long"):
        return float(raw_tp) * (1.0 - eff_pct)
    if s in ("sell", "short"):
        return float(raw_tp) * (1.0 + eff_pct)
    return float(raw_tp)


def adjust_sl_for_offset(raw_sl: float, side: str, atr: float = 0.0, ref_price: Optional[float] = None) -> float:
    """
    (MISE À JOUR) Offset hybride pour le SL : min(pourcentage, ATR*k) pour éviter un SL trop éloigné.
    - ref_price: l’ancre (high/low de la bougie d’ancrage) pour convertir ATR en %.
    - Sans ref_price, on retombe sur l'ancien comportement (pourcentage seul).
    """
    try:
        pct = float(database.get_setting('SL_OFFSET_PCT', '0.003'))  # 0.30%
    except Exception:
        pct = 0.003
    try:
        atr_k = float(database.get_setting('SL_ATR_K', '0.50'))
    except Exception:
        atr_k = 0.50

    eff_pct = pct
    if ref_price and ref_price > 0 and atr > 0:
        # Avant : eff_pct = max(pct, (atr_k * atr) / ref_price) -> pouvait envoyer le SL très loin.
        # Maintenant : on CAPE l'effet ATR pour garder un SL plus proche (≤ pct).
        atr_pct = (atr_k * float(atr)) / float(ref_price)
        eff_pct = min(pct, atr_pct)

    s = (side or "").lower()
    if s in ("buy", "long"):
        return float(raw_sl) * (1.0 - eff_pct)
    if s in ("sell", "short"):
        return float(raw_sl) * (1.0 + eff_pct)
    return float(raw_sl)

def _update_signal_state(
    symbol: str,
    timeframe: str,
    signal: Dict[str, Any],
    entry_price: float,
    state: str,
    reason: Optional[str] = None,
    tp: Optional[float] = None,
    sl: Optional[float] = None,
) -> None:
    """Met à jour l'état d'un signal déjà persisté."""
    try:
        ts_sig = int(signal.get("ts", 0) or 0)
        if ts_sig <= 0:
            return
        
        # ✅ CORRECTION: Utiliser database.insert_signal() au lieu de record_signal_from_trader()
        try:
            database.insert_signal(
                symbol=symbol,
                side=signal.get("side", "-"),
                timeframe=timeframe,
                ts=ts_sig,
                regime=str(signal.get("regime", "-")),
                entry=float(entry_price),
                sl=float(sl or signal.get("sl", 0.0)),
                tp=float(tp or signal.get("tp", 0.0)),
                rr=float(signal.get("rr", 0.0)),
                state=state
            )
        except Exception:
            pass
        
    except Exception:
        pass

        
def _is_reaction_candle(row: pd.Series, is_long: bool) -> bool:
    """
    Détection d'une bougie de réaction (Tendance & Contre-Tendance).

    Utilise les ratios déjà définis dans la stratégie :
    - Pinbar (grosse mèche opposée, petit corps)
    - Wick simple (mèche significative côté réaction)
    - Impulsion directionnelle (marubozu / gros corps dans le sens du trade)

    Si des tags de pattern existent déjà dans la ligne (ex: 'pattern'),
    ils sont utilisés en priorité, sinon on retombe sur l'analyse OHLC.
    """
    # 1) Si la ligne porte déjà un tag de pattern exploitable
    try:
        pattern = str(row.get("pattern", "")).lower()
        if is_long and pattern in ("pinbar_long", "wick_long", "impulse_long", "reaction_long"):
            return True
        if (not is_long) and pattern in ("pinbar_short", "wick_short", "impulse_short", "reaction_short"):
            return True
    except Exception:
        pass

    # 2) Analyse directe OHLC
    try:
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
    except Exception:
        return False

    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l

    body_pct = body / rng
    upper_pct = upper / rng
    lower_pct = lower / rng

    # Seuils proches de ceux de la strat (pinbar / wick / marubozu)
    # PINBAR_MAX_BODY ≈ 0.35
    # SIMPLE_WICK_MIN ≈ 0.27
    # PINBAR_OPP_WICK_MAX ≈ 0.24
    # MARUBOZU_MIN_BODY ≈ 0.28

    if is_long:
        bullish = c >= o
        pinbar = (body_pct <= 0.35 and lower_pct >= 0.45 and upper_pct <= 0.24)
        simple_wick = (lower_pct >= 0.27 and body_pct <= 0.60)
        impulsion = bullish and body_pct >= 0.28 and upper_pct <= 0.25
        return bullish and (pinbar or simple_wick or impulsion)
    else:
        bearish = c <= o
        pinbar = (body_pct <= 0.35 and upper_pct >= 0.45 and lower_pct <= 0.24)
        simple_wick = (upper_pct >= 0.27 and body_pct <= 0.60)
        impulsion = bearish and body_pct >= 0.28 and lower_pct <= 0.25
        return bearish and (pinbar or simple_wick or impulsion)


def _check_reaction_before_entry(df: pd.DataFrame, signal: Dict[str, Any], is_long: bool) -> bool:
    """
    Vérifie la présence d'une bougie de réaction OBLIGATOIRE avant l'entrée.
    S'applique en Tendance ET en Contre-Tendance.

    Logique :
      1) Si le signal indique déjà explicitement une réaction (has_reaction / idx_reaction) → OK.
      2) Sinon, on cherche une bougie de réaction dans les 1 à 3 bougies AVANT la bougie d'entrée.
         - entry_index dans le signal si présent
         - sinon, dernière bougie clôturée (len(df) - 2)

    En cas de doute ou d'erreur → on considère qu'il n'y a PAS de réaction (fail-safe).
    """
    # 1) Signal déjà taggé comme "avec réaction"
    try:
        if bool(signal.get("has_reaction")):
            return True
    except Exception:
        pass

    # 2) Index de bougie de réaction explicite
    try:
        idx_react = signal.get("idx_reaction")
        if idx_react is not None:
            idx_react = int(idx_react)
            if 0 <= idx_react < len(df):
                return True
    except Exception:
        pass

    # 3) Recherche locale autour de l'entrée
    if df is None or len(df) < 3:
        return False

    try:
        entry_idx = signal.get("entry_index")
        if entry_idx is None:
            # on prend la dernière bougie clôturée comme référence
            entry_idx = len(df) - 2
        entry_idx = int(entry_idx)
    except Exception:
        entry_idx = len(df) - 2

    # fenêtre: 1 à 3 bougies avant l'entrée
    start = max(0, entry_idx - 3)
    end = max(0, entry_idx - 1)

    if end < start:
        return False

    window = df.iloc[start : end + 1]

    for _, row in window.iterrows():
        if _is_reaction_candle(row, is_long):
            return True

    return False

def _recalc_sl_tp_live(
    df: pd.DataFrame,
    side: str,
    regime: str,
    entry_price: float,
    symbol: str,
    timeframe: str,
    signal: Optional[Dict[str, Any]] = None
) -> Tuple[float, float, Optional[str]]:
    """
    Recalcule SL/TP live au moment de l'exécution selon la stratégie Darwin.
    
    CORRECTION CRITIQUE : SL basé sur HIGH/LOW de CONTACT + RÉACTION (pas BB20/BB80 dernière bougie)
    
    TENDANCE :
    - SL = MAX/MIN(contact, reaction) + offset ajustable
    - TP = BB80 opposée + offset
    
    CONTRE-TENDANCE :
    - SL = MAX/MIN(contact, reaction) + offset ajustable
    - TP = BB20_mid + offset
    
    Args:
        df: DataFrame marché
        side: 'buy' ou 'sell'
        regime: 'Tendance' ou 'Contre-tendance'
        entry_price: Prix d'entrée
        symbol: Symbole
        timeframe: Timeframe
        signal: Signal dict contenant contact_high/low, reaction_high/low
    
    Returns:
        (sl_price, tp_price, error_message)
    """
    try:
        if df is None or len(df) < 3:
            return 0.0, 0.0, "df_insufficient"
        
        last = df.iloc[-1]
        is_long = (str(side).lower() == 'buy')
        
        # Récupérer les BB
        try:
            bb20_up = float(last['bb20_up'])
            bb20_lo = float(last['bb20_lo'])
            bb20_mid = float(last['bb20_mid'])
            bb80_up = float(last['bb80_up'])
            bb80_lo = float(last['bb80_lo'])
            atr = float(last.get('atr', 0.0))
        except Exception as e:
            return 0.0, 0.0, f"bb_missing:{e}"
        
        # ========================================================================
        # ✅ CORRECTION : Utiliser les niveaux de CONTACT et RÉACTION du signal
        # ========================================================================
        
        sl_anchor = None
        
        if signal:
            contact_high = signal.get('contact_high')
            contact_low = signal.get('contact_low')
            reaction_high = signal.get('reaction_high')
            reaction_low = signal.get('reaction_low')
            
            if is_long:
                # LONG : SL = MIN(contact_low, reaction_low)
                if contact_low is not None and reaction_low is not None:
                    sl_anchor = min(float(contact_low), float(reaction_low))
                elif contact_low is not None:
                    sl_anchor = float(contact_low)
                elif reaction_low is not None:
                    sl_anchor = float(reaction_low)
            else:
                # SHORT : SL = MAX(contact_high, reaction_high)
                if contact_high is not None and reaction_high is not None:
                    sl_anchor = max(float(contact_high), float(reaction_high))
                elif contact_high is not None:
                    sl_anchor = float(contact_high)
                elif reaction_high is not None:
                    sl_anchor = float(reaction_high)
        
        # ========================================================================
        # FALLBACK : Si pas de signal ou niveaux manquants → BB20/BB80
        # ========================================================================
        
        if sl_anchor is None:
            # Fallback sur BB (comportement ancien pour trades manuels)
            if regime == 'Tendance':
                sl_anchor = bb20_lo if is_long else bb20_up
            else:  # Contre-tendance
                sl_anchor = bb80_lo if is_long else bb80_up
        
        # ========================================================================
        # CALCUL SL AVEC OFFSET AJUSTABLE (depuis Telegram)
        # ========================================================================
        
        sl = adjust_sl_for_offset(
            raw_sl=float(sl_anchor),
            side=('buy' if is_long else 'sell'),
            atr=float(atr),
            ref_price=float(sl_anchor)
        )
        
        # ========================================================================
        # CALCUL TP (DYNAMIQUE - Dernière bougie)
        # ========================================================================
        
        if regime == 'Tendance':
            if is_long:
                tp_raw = bb80_up
            else:
                tp_raw = bb80_lo
        else:  # Contre-tendance
            tp_raw = bb20_mid
        
        tp = adjust_tp_for_bb_offset(
            raw_tp=float(tp_raw),
            side=('buy' if is_long else 'sell'),
            atr=float(atr),
            ref_price=float(tp_raw)
        )
        
        # ========================================================================
        # VALIDATIONS FINALES
        # ========================================================================
        
        # 1. SL ne doit pas être du mauvais côté de l'entry
        if is_long and sl >= entry_price:
            return 0.0, 0.0, f"sl_above_entry_long:sl={sl:.4f},entry={entry_price:.4f}"
        
        if not is_long and sl <= entry_price:
            return 0.0, 0.0, f"sl_below_entry_short:sl={sl:.4f},entry={entry_price:.4f}"
        
        # 2. TP ne doit pas être du mauvais côté de l'entry
        if is_long and tp <= entry_price:
            return 0.0, 0.0, f"tp_below_entry_long:tp={tp:.4f},entry={entry_price:.4f}"
        
        if not is_long and tp >= entry_price:
            return 0.0, 0.0, f"tp_above_entry_short:tp={tp:.4f},entry={entry_price:.4f}"
        
        # 3. SL ne doit pas être au-delà du TP
        if is_long and sl >= tp:
            return 0.0, 0.0, f"sl_beyond_tp_long:sl={sl:.4f},tp={tp:.4f}"
        
        if not is_long and sl <= tp:
            return 0.0, 0.0, f"sl_beyond_tp_short:sl={sl:.4f},tp={tp:.4f}"
        
        return float(sl), float(tp), None
    
    except Exception as e:
        return 0.0, 0.0, f"recalc_error:{e}"

def execute_signal_with_gates(
    ex: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    signal: Dict[str, Any],
    entry_price: float,
) -> Tuple[bool, str]:
    """Encapsule le recalcul live SL/TP + gate RR + validations Bitget + exécution robuste."""
    side = (signal.get('side') or '').lower()
    regime = str(signal.get('regime', 'Tendance'))
    is_long = (side == 'buy')
    entry_px = float(entry_price)

    if df is None or len(df) < 3:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="df_short_for_entry_gate")
        return False, "Rejeté: données insuffisantes pour valider l'entrée."

    # --- ✅ GATE CORRELATION/SECTEUR ---
    if not check_correlation_risk(ex, symbol, side):
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="correlation_risk")
        return False, "Rejeté: risque correlation/secteur trop élevé."

    # --- GATE RÉACTION OBLIGATOIRE (TENDANCE + CT) ---
    passed_reaction = _check_reaction_before_entry(df, signal, is_long)
    msg_react = "no_reaction_pattern" if not passed_reaction else "reaction_found"
    if not passed_reaction:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason=msg_react)
        return False, msg_react

    # --- RECALCUL SL/TP LIVE ---
    sl, tp, err = _recalc_sl_tp_live(
        df=df,
        side=side,
        regime=regime,
        entry_price=entry_px,
        symbol=symbol,
        timeframe=timeframe,
        signal=signal
    )
    
    if err:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason=err)
        return False, err

    # --- GATE RR MINIMUM ---
    rr_calc = abs((tp - entry_px) / (entry_px - sl + 1e-8))
    if rr_calc < MIN_RR:
        _update_signal_state(
            symbol, timeframe, signal, entry_px, "SKIPPED",
            reason=f"RR={rr_calc:.2f} < {MIN_RR}"
        )
        return False, f"Rejeté: RR={rr_calc:.2f} < {MIN_RR}."

    # --- CALCUL QTÉ ---
    balance_usdt = get_account_balance_usdt(ex)
    if balance_usdt is None or balance_usdt <= 0:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="balance_unavailable")
        return False, "Rejeté: solde USDT indisponible."
    
    raw_qty = calculate_position_size(
        balance=balance_usdt,
        risk_percent=RISK_PER_TRADE_PERCENT,
        entry_price=entry_px,
        sl_price=sl
    )
    
    capped_qty, meta = _cap_qty_for_margin_and_filters(ex, symbol, side, raw_qty, entry_px)
    
    if meta.get('reason') == 'INSUFFICIENT_AFTER_CAP':
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="insufficient_margin")
        return False, "Rejeté: marge insuffisante après application des filtres."
    
    if capped_qty is None or capped_qty <= 0:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="qty_zero")
        return False, "Rejeté: taille position = 0 ou invalide."

    # ========================================================================
    # ✅ VALIDATION 1/3 : QUANTITY MAX (Erreur 45133)
    # ========================================================================
    
    try:
        market = ex.market(symbol)
        max_qty = market.get('limits', {}).get('amount', {}).get('max')
        
        if max_qty and capped_qty > float(max_qty):
            # Cap à 95% du max pour sécurité
            original_qty = capped_qty
            capped_qty = float(max_qty) * 0.95
            
            print(f"⚠️ {symbol} : Quantité réduite {original_qty:.6f} → {capped_qty:.6f} (max: {max_qty})")
            
            try:
                notifier.tg_send(
                    f"⚠️ **Quantité ajustée**\n\n"
                    f"{symbol} {side.upper()}\n"
                    f"Demandée : {original_qty:.6f}\n"
                    f"Max exchange : {max_qty}\n"
                    f"Ajustée : {capped_qty:.6f} (95% du max)\n\n"
                    f"Trade va être exécuté avec quantité réduite"
                )
            except Exception:
                pass
    except Exception as e:
        print(f"⚠️ {symbol} : Erreur validation quantity max → {e}")

    # ========================================================================
    # ✅ VALIDATION 2/3 : TP DISTANCE MINIMUM (Erreur 40836)
    # ========================================================================
    
    try:
        # Récupérer mark price
        try:
            mark_price = _current_mark_price(ex, symbol)
        except Exception:
            ticker = ex.fetch_ticker(symbol)
            mark_price = float(ticker.get('last') or ticker.get('close') or entry_px)
        
        # Distance minimale configurable (défaut 0.5%)
        try:
            min_tp_distance_pct = float(database.get_setting('MIN_TP_DISTANCE_PCT', '0.5'))
        except:
            min_tp_distance_pct = 0.5
        
        min_distance = mark_price * (min_tp_distance_pct / 100.0)
        
        # Vérifier distance TP vs mark
        tp_distance = abs(tp - mark_price)
        
        if tp_distance < min_distance:
            # Ajuster TP pour respecter distance minimale
            if is_long:
                # LONG : TP au-dessus de mark
                tp_adjusted = mark_price * (1.0 + min_tp_distance_pct / 100.0)
            else:
                # SHORT : TP en-dessous de mark
                tp_adjusted = mark_price * (1.0 - min_tp_distance_pct / 100.0)
            
            print(f"⚠️ {symbol} : TP ajusté {tp:.6f} → {tp_adjusted:.6f} (trop proche mark: {mark_price:.6f})")
            
            # Vérifier que RR reste acceptable
            rr_adjusted = abs((tp_adjusted - entry_px) / (entry_px - sl + 1e-8))
            
            if rr_adjusted >= MIN_RR:
                tp = tp_adjusted
                
                try:
                    notifier.tg_send(
                        f"⚠️ **TP ajusté**\n\n"
                        f"{symbol} {side.upper()}\n"
                        f"TP initial trop proche mark\n"
                        f"Mark : {mark_price:.6f}\n"
                        f"TP ajusté : {tp:.6f}\n"
                        f"RR ajusté : {rr_adjusted:.2f}\n\n"
                        f"Trade va être exécuté"
                    )
                except Exception:
                    pass
            else:
                # RR insuffisant après ajustement → rejeter
                _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="tp_distance_insufficient")
                return False, f"Rejeté: TP trop proche mark ({tp_distance:.6f} < {min_distance:.6f}) et ajustement dégrade RR ({rr_adjusted:.2f} < {MIN_RR})"
    
    except Exception as e:
        print(f"⚠️ {symbol} : Erreur validation TP distance → {e}")

    # ========================================================================
    # ✅ VALIDATION 3/3 : NOTIONAL MINIMUM (Erreur 45110)
    # ========================================================================
    
    try:
        notional = entry_px * capped_qty
        
        # Minimum Bitget = 5 USDT
        try:
            min_notional = float(database.get_setting('MIN_NOTIONAL_USDT', '5.0'))
        except:
            min_notional = 5.0
        
        if notional < min_notional:
            _update_signal_state(
                symbol, timeframe, signal, entry_px, "SKIPPED",
                reason=f"notional_too_small:{notional:.2f}<{min_notional}"
            )
            
            try:
                notifier.tg_send(
                    f"❌ **Trade rejeté**\n\n"
                    f"{symbol} {side.upper()}\n"
                    f"Notional : {notional:.2f} USDT\n"
                    f"Minimum : {min_notional:.2f} USDT\n\n"
                    f"Position trop petite pour l'exchange"
                )
            except Exception:
                pass
            
            return False, f"Rejeté: Notional {notional:.2f} USDT < minimum {min_notional:.2f} USDT"
    
    except Exception as e:
        print(f"⚠️ {symbol} : Erreur validation notional → {e}")

    # ========================================================================
    # EXÉCUTION (si toutes validations passées)
    # ========================================================================

    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    price_ref = entry_px

    try:
        sl = float(ex.price_to_precision(symbol, sl))
        tp = float(ex.price_to_precision(symbol, tp))
        quantity = float(ex.amount_to_precision(symbol, capped_qty))
    except Exception:
        quantity = float(capped_qty)

    if quantity <= 0:
        _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason="qty_zero_after_precision")
        return False, "Rejeté: quantité finale arrondie à 0."

    final_entry_price = price_ref
    management_strategy = "NORMAL"
    if str(database.get_setting('STRATEGY_MODE', 'NORMAL')).upper() == 'SPLIT':
        management_strategy = "SPLIT"

    common_params = {'tdMode': 'cross', 'posMode': 'oneway'}

    if not is_paper_mode:
        try:
            try:
                ex.set_leverage(LEVERAGE, symbol)
                try:
                    ex.set_margin_mode('cross', symbol)
                except Exception:
                    pass
                try:
                    ex.set_position_mode(False, symbol)
                except Exception:
                    pass
            except Exception:
                pass

            # --- FERMETURE POSITION INVERSE SI EXISTANTE ---
            open_positions = database.get_open_positions()
            for pos_open in open_positions:
                if pos_open.get('symbol') == symbol and pos_open.get('side', '').lower() != side:
                    try:
                        close_position_manually(ex, int(pos_open['id']))
                        clear_balance_cache()
                    except Exception as e_close:
                        notifier.tg_send(f"⚠️ Fermeture position inverse échouée pour {symbol}: {e_close}")
                        continue

            # --- Ordre marché d'entrée ---
            order = create_market_order_smart(
                ex, symbol, side, quantity, ref_price=final_entry_price, params=common_params
            )
            if order and order.get('price'):
                final_entry_price = float(order['price'])
            
            clear_balance_cache()

            # --- RÉCUPÉRATION TAILLE RÉELLE ---
            try:
                market_real = None
                try:
                    market_real = ex.market(symbol)
                except Exception:
                    pass

                real_qty = None
                positions = _fetch_positions_safe(ex, [symbol])
                for p in positions:
                    same = (p.get('symbol') == symbol)
                    if (not same) and market_real:
                        same = (p.get('raw', {}).get('symbol') == market_real.get('id'))
                    if same:
                        contracts = float(p.get('size') or p.get('contracts') or p.get('positionAmt') or 0.0)
                        contracts = abs(contracts)
                        if contracts and contracts > 0:
                            real_qty = contracts
                            break
                
                if real_qty is not None and real_qty > 0:
                    quantity = real_qty
                    quantity = float(ex.amount_to_precision(symbol, quantity))
            except Exception:
                pass

            # --- Ordres SL/TP ---
            try:
                market = ex.market(symbol) or {}
                tick_size = _bitget_tick_size(market)
            except Exception:
                tick_size = 0.0001

            sl_ok, tp_ok = _place_sl_tp_safe(
                ex, symbol, side, quantity,
                sl=float(sl),
                tp=float(tp),
                params=common_params,
                is_long=is_long,
                tick_size=tick_size
            )

        except Exception as e:
            try:
                close_side = 'sell' if is_long else 'buy'
                create_market_order_smart(
                    ex, symbol, close_side, quantity, ref_price=final_entry_price, params=common_params
                )
                clear_balance_cache()
            except Exception:
                pass
            notifier.tg_send_error(f"Exécution d'ordre sur {symbol}", e)
            _update_signal_state(symbol, timeframe, signal, entry_px, "SKIPPED", reason=f"execution_error:{e}")
            return False, f"Erreur d'exécution: {e}"

    # --- Persistance & notification ---
    signal['entry'] = final_entry_price
    signal['sl'] = float(sl)
    signal['tp'] = float(tp)

    database.create_trade(
        symbol=symbol,
        side=side,
        regime=regime,
        entry_price=final_entry_price,
        sl_price=float(sl),
        tp_price=float(tp),
        quantity=float(quantity),
        risk_percent=RISK_PER_TRADE_PERCENT,
        management_strategy=management_strategy,
        entry_atr=float(signal.get('entry_atr', 0.0) or 0.0),
        entry_rsi=float(signal.get('entry_rsi', 0.0) or 0.0),
    )

    _update_signal_state(symbol, timeframe, signal, final_entry_price, "VALID", tp=float(tp), sl=float(sl))

    # ========================================================================
    # GÉNÉRATION GRAPHIQUE
    # ========================================================================
    
    chart_image = None
    
    try:
        required_keys = ['contact_index', 'reaction_index', 'entry_index']
        missing_keys = [k for k in required_keys if k not in signal]
        
        if missing_keys:
            print(f"⚠️ Signal {symbol} incomplet pour graphique. Manquant: {missing_keys}")
            
            try:
                notifier.tg_send(
                    f"⚠️ Graphique {symbol} non généré\n"
                    f"Index manquants: {', '.join(missing_keys)}\n"
                    f"Signal: {signal.get('regime', 'N/A')} {signal.get('pattern', 'N/A')}"
                )
            except Exception:
                pass
        
        else:
            contact_idx = signal.get('contact_index')
            reaction_idx = signal.get('reaction_index')
            entry_idx = signal.get('entry_index')
            
            max_idx = max(contact_idx, reaction_idx, entry_idx)
            
            if df is None or len(df) <= max_idx:
                print(f"⚠️ DF {symbol} trop court pour graphique")
                
                try:
                    notifier.tg_send(
                        f"⚠️ Graphique {symbol} non généré\n"
                        f"DF trop court: {len(df) if df is not None else 0} bougies\n"
                        f"Requis: {max_idx + 1}"
                    )
                except Exception:
                    pass
            
            else:
                print(f"📊 Génération graphique {symbol}...")
                
                chart_image = charting.generate_trade_chart(symbol, df, signal)
                
                if chart_image:
                    print(f"✅ Graphique {symbol} généré avec succès")
                else:
                    print(f"⚠️ Graphique {symbol} retourné None")
    
    except Exception as e:
        print(f"❌ ERREUR génération graphique {symbol}: {e}")
        
        import traceback
        traceback.print_exc()
        
        try:
            error_msg = str(e)[:200]
            notifier.tg_send(
                f"❌ **Erreur Graphique**\n\n"
                f"🎯 {symbol}\n"
                f"⚠️ {error_msg}\n\n"
                f"Le trade a été ouvert mais sans graphique."
            )
        except Exception:
            pass
        
        chart_image = None

    mode_text = "PAPIER" if is_paper_mode else "RÉEL"
    trade_message = notifier.format_trade_message(symbol, signal, quantity, mode_text, RISK_PER_TRADE_PERCENT)

    try:
        if chart_image is not None:
            notifier.tg_send_with_photo(photo_buffer=chart_image, caption=trade_message)
        else:
            notifier.tg_send(trade_message)
    except Exception as e:
        print(f"❌ Erreur envoi notification {symbol}: {e}")
        try:
            notifier.tg_send(trade_message)
        except Exception:
            pass

    return True, "Position ouverte avec succès."


def get_tp_offset_pct() -> float:
    """Retourne le pourcentage d'offset (ex: 0.003 = 0.3%) pour TP/SL depuis la DB,
    clampé pour garantir que le TP se place AVANT la borne (jamais 0)."""
    try:
        v = float(database.get_setting('TP_BB_OFFSET_PCT', 0.003))
    except Exception:
        v = 0.003
    # Clamp : min 0.05% ; max 10%
    if v < 0.0005: v = 0.0005
    if v > 0.1:     v = 0.1
    return v

def compute_fee_safe_be_price(
    entry: float,
    side: str,                 # 'long' | 'short'
    qty: float,                # taille position (en "coin" pour linéaires USDT)
    fee_in_pct: float,
    fee_out_pct: float,
    buffer_pct: float = 0.0,   # petit surplus pour finir > 0
    buffer_usdt: float = 0.0   # OU buffer absolu sur la position
) -> float:
    """
    Retourne le prix de stop 'break-even' qui couvre:
      - PnL +/- (exit - entry) * qty
      - frais d'entrée: fee_in_pct * entry * qty
      - frais de sortie: fee_out_pct * exit  * qty
      - buffer: soit % de notional d'entrée (buffer_pct * entry * qty), soit absolu USDT (buffer_usdt)
    Formules (linéaire USDT):
      Long  : exit >= (E*(1+fin) + b_per_qty)/(1 - fout)
      Short : exit <= (E*(1-fin) - b_per_qty)/(1 + fout)
    """
    side = (side or "").lower()
    E = float(entry)
    Q = max(0.0, float(qty))
    fin = max(0.0, float(fee_in_pct))
    fout = max(0.0, float(fee_out_pct))

    # buffer exprimé par "unité de qty"
    b_per_qty = float(buffer_pct) * E
    if buffer_usdt and Q > 0:
        b_per_qty += float(buffer_usdt) / Q

    if side == 'long':
        # exit >= (E*(1+fin) + b_per_qty) / (1 - fout)
        denom = (1.0 - fout)
        if denom <= 0:
            # sécurité extrême (frais erronés) : fallback sans fout
            return E * (1.0 + fin) + b_per_qty
        return (E * (1.0 + fin) + b_per_qty) / denom

    elif side == 'short':
        # exit <= (E*(1-fin) - b_per_qty) / (1 + fout)
        denom = (1.0 + fout)
        if denom <= 0:
            # sécurité extrême : fallback sans fout
            return E * (1.0 - fin) - b_per_qty
        return (E * (1.0 - fin) - b_per_qty) / denom

    else:
        return E  # si side inconnu, ne bouge pas

def _compute_trailing_sl(mark_price: float, side: str, atr: float) -> float:
    """
    Trailing pro (mix) : distance = max(d%, k*ATR).
      - d%   = TRAIL_PCT (def 0.0035 = 0.35%)
      - kATR = TRAIL_ATR_K (def 1.0)
    Retourne le prix de SL cible par rapport au prix 'mark'.
    """
    try:
        d_pct = float(database.get_setting('TRAIL_PCT', 0.0035))     # 0.35%
    except Exception:
        d_pct = 0.0035
    try:
        k_atr = float(database.get_setting('TRAIL_ATR_K', 1.0))      # 1×ATR
    except Exception:
        k_atr = 1.0

    mark = float(mark_price)
    atr  = max(0.0, float(atr))

    # distance en prix
    dist_pct = abs(d_pct) * mark
    dist_atr = abs(k_atr) * atr
    dist = max(dist_pct, dist_atr)

    if (side or "").lower() in ("buy", "long"):
        return max(0.0, mark - dist)
    else:
        return max(0.0, mark + dist)

def execute_trade(ex: ccxt.Exchange, symbol: str, timeframe: str, signal: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Wrapper d’exécution attendu par le reste du projet.
    - (Option) Sync positions avant exécution si SYNC_BEFORE_EXECUTE = true
    - Charge le DF préparé
    - Détermine un prix d’entrée « live » robuste (respecte CT_ENTRY_ON_NEXT_BAR si activé)
    - Délègue à execute_signal_with_gates (recalc SL/TP/RR + envoi ordres + persistance)
    """
    try:
        # 1) Sync optionnelle avant exécution
        try:
            if str(database.get_setting('SYNC_BEFORE_EXECUTE', 'true')).lower() == 'true':
                sync_positions_with_exchange(ex)
        except Exception:
            pass

        # 2) Données marché
        df = utils.fetch_and_prepare_df(ex, symbol, timeframe)
        if df is None or len(df) < 3:
            return False, "DF indisponible ou trop court pour exécuter."

        # 3) Politique d’entrée (next bar vs live ticker) + indices pour le chart
        try:
            enforce_next_bar = str(database.get_setting('CT_ENTRY_ON_NEXT_BAR', 'true')).lower() == 'true'
        except Exception:
            enforce_next_bar = True

        n = len(df)
        entry_index: Optional[int] = None
        reaction_index: Optional[int] = None

        if n >= 1:
            if enforce_next_bar and n >= 2:
                # Bougie d’entrée = dernière, bougie de réaction = précédante
                entry_index = n - 1
                reaction_index = n - 2
            else:
                # Entrée sur la bougie courante (pas de next-bar strict)
                entry_index = n - 1
                reaction_index = n - 1

        entry_price: float = 0.0
        if enforce_next_bar and entry_index is not None:
            # Entrée stricte: OPEN de la bougie d’entrée (proxy du “next bar open” en live)
            try:
                entry_price = float(df.iloc[entry_index].get('open', 0.0))
            except Exception:
                entry_price = 0.0
        else:
            # Entrée sur la clôture de la bougie d’entrée
            if entry_index is not None:
                try:
                    entry_price = float(df.iloc[entry_index].get('close', 0.0))
                except Exception:
                    entry_price = 0.0

        # Fallback sur ticker si nécessaire
        if not entry_price or entry_price <= 0.0:
            try:
                t = ex.fetch_ticker(symbol) or {}
                entry_price = float(t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0)
            except Exception:
                entry_price = 0.0

        # Dernier fallback: close/open du DF
        if not entry_price or entry_price <= 0.0:
            try:
                last_row = df.iloc[-1]
                entry_price = float(last_row.get('close') or last_row.get('open') or 0.0)
            except Exception:
                entry_price = 0.0

        if entry_price <= 0.0:
            return False, "Impossible d’estimer un prix d’entrée."

        # 3.bis) Indices pour le graphique : contact / réaction / entrée
        try:
            contact_idx = _find_contact_index(df, base_exclude_last=True, max_lookback=5)
        except Exception:
            contact_idx = None

        try:
            if contact_idx is not None:
                signal["contact_index"] = int(contact_idx)
        except Exception:
            pass

        try:
            if reaction_index is not None:
                signal["reaction_index"] = int(reaction_index)
        except Exception:
            pass

        try:
            if entry_index is not None:
                signal["entry_index"] = int(entry_index)
        except Exception:
            pass

        # 4) Normalisations minimales du signal
        if not signal.get("ts"):
            signal["ts"] = int(time.time() * 1000)
        if not signal.get("side"):
            last = df.iloc[-1]
            signal["side"] = "buy" if float(last.get("close", 0.0)) >= float(last.get("open", 0.0)) else "sell"
        if not signal.get("regime"):
            signal["regime"] = "Tendance"

        # 5) Délégation à l’exécuteur avec garde-fous RR/SL/TP
        return execute_signal_with_gates(
            ex=ex,
            symbol=symbol,
            timeframe=timeframe,
            df=df,
            signal=signal,
            entry_price=float(entry_price),
        )

    except Exception as e:
        try:
            notifier.tg_send(f"❌ execute_trade({symbol}) a échoué: {e}")
        except Exception:
            pass
        return False, f"Erreur interne execute_trade: {e}"


def _progress_to_tp(entry: float, tp: float, mark: float, is_long: bool) -> float:
    """
    Retourne la progression normalisée du prix entre l'entrée et le TP:
      - 0.0  : encore loin de l'objectif
      - 1.0+ : TP atteint ou dépassé
    Utilisé pour activer / resserrer le trailing uniquement quand on 'accroche' le TP.
    """
    try:
        entry = float(entry)
        tp = float(tp)
        mark = float(mark)
    except Exception:
        return 0.0

    if entry <= 0 or tp <= 0:
        return 0.0

    if is_long:
        # Long : progression (entry → tp)
        if tp <= entry or mark <= entry:
            return 0.0
        if mark >= tp:
            return 1.0
        return max(0.0, min(1.0, (mark - entry) / (tp - entry)))
    else:
        # Short : progression (entry → tp) mais dans l'autre sens
        if tp >= entry or mark >= entry:
            return 0.0
        if mark <= tp:
            return 1.0
        return max(0.0, min(1.0, (entry - mark) / (entry - tp)))


def _find_last_swing_anchor(df: pd.DataFrame, is_long: bool, max_lookback: int = 15) -> Optional[float]:
    """
    Cherche le DERNIER swing confirmé dans les dernières bougies :
      - SHORT : swing high (top local)
      - LONG  : swing low  (bottom local)
    Utilisé comme ancre de SL pour le passage à BE (avec offset hybride).
    """
    try:
        if df is None:
            return None
        n = len(df)
        if n < 3:
            return None

        lb = max(3, int(max_lookback))
        start = max(1, n - lb - 1)
        end = n - 2  # on évite la dernière bougie (encore en formation)

        if end <= 0 or start > end:
            return None

        if is_long:
            # Swing LOW (bottom) pour les longs
            for i in range(end, start - 1, -1):
                cur = df.iloc[i]
                prev = df.iloc[i - 1]
                nxt = df.iloc[i + 1]
                l = float(cur["low"])
                l_prev = float(prev["low"])
                l_next = float(nxt["low"])
                if l < l_prev and l < l_next:
                    return l
        else:
            # Swing HIGH (top) pour les shorts
            for i in range(end, start - 1, -1):
                cur = df.iloc[i]
                prev = df.iloc[i - 1]
                nxt = df.iloc[i + 1]
                h = float(cur["high"])
                h_prev = float(prev["high"])
                h_next = float(nxt["high"])
                if h > h_prev and h > h_next:
                    return h
    except Exception:
        return None

    return None

def should_pyramid_position(ex, pos: dict, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Détermine si on peut ajouter à une position gagnante (pyramiding).
    
    RÈGLES DARWIN PYRAMIDING :
    1. Position en profit > seuil minimum (défaut 2%)
    2. Breakout confirmé d'un niveau clé (BB80 ou swing high/low)
    3. Maximum 2 ajouts par position (total 3 entrées)
    4. Volume en augmentation (confirmation tendance)
    5. Pas de pyramiding si déjà en BE ou trailing actif
    
    Args:
        ex: Exchange
        pos: Position DB
        df: DataFrame du symbole
    
    Returns:
        Dict avec infos pyramiding si conditions OK, sinon None
    """
    try:
        # ====== VÉRIFICATIONS PRÉLIMINAIRES ======
        
        # 1. Feature activée ?
        try:
            enable_pyramiding = str(database.get_setting('ENABLE_PYRAMIDING', 'false')).lower() == 'true'
        except Exception:
            enable_pyramiding = False
        
        if not enable_pyramiding:
            return None
        
        # 2. Nombre d'ajouts déjà effectués
        pyramid_count = int(pos.get('pyramid_count', 0))
        try:
            max_pyramids = int(database.get_setting('MAX_PYRAMIDS', '2'))
        except Exception:
            max_pyramids = 2
        
        if pyramid_count >= max_pyramids:
            return None
        
        # 3. Pas de pyramiding si BE/trailing actif (trop tard)
        be_status = str(pos.get('breakeven_status', '')).upper()
        if be_status == 'ACTIVE':
            return None
        
        # ====== CALCUL PROFIT ACTUEL ======
        
        symbol = pos['symbol']
        entry_price = float(pos['entry_price'])
        is_long = (pos['side'] == 'buy')
        
        if df is None or len(df) < 2:
            return None
        
        last = df.iloc[-1]
        current_price = float(last['close'])
        
        # Profit en %
        if is_long:
            profit_pct = ((current_price - entry_price) / entry_price) * 100
        else:
            profit_pct = ((entry_price - current_price) / entry_price) * 100
        
        # 4. Profit minimum requis
        try:
            min_profit_for_pyramid = float(database.get_setting('PYRAMID_MIN_PROFIT_PCT', '2.0'))
        except Exception:
            min_profit_for_pyramid = 2.0
        
        if profit_pct < min_profit_for_pyramid:
            return None
        
        # ====== DÉTECTION BREAKOUT NIVEAU CLÉ ======
        
        bb80_up = float(last['bb80_up'])
        bb80_lo = float(last['bb80_lo'])
        bb20_up = float(last['bb20_up'])
        bb20_lo = float(last['bb20_lo'])
        
        breakout_detected = False
        breakout_level = None
        
        if is_long:
            # LONG : Chercher breakout au-dessus BB80 ou swing high
            prev = df.iloc[-2]
            prev_high = float(prev['high'])
            curr_high = float(last['high'])
            
            # Breakout BB80
            if prev_high <= bb80_up and curr_high > bb80_up:
                breakout_detected = True
                breakout_level = 'BB80_UP'
            
            # OU breakout swing high récent
            else:
                try:
                    lookback = min(10, len(df) - 1)
                    window = df.iloc[-lookback:-1]
                    swing_high = window['high'].max()
                    
                    if prev_high <= swing_high and curr_high > swing_high:
                        breakout_detected = True
                        breakout_level = f'SWING_HIGH_{swing_high:.4f}'
                except Exception:
                    pass
        
        else:  # SHORT
            # SHORT : Chercher breakout en-dessous BB80 ou swing low
            prev = df.iloc[-2]
            prev_low = float(prev['low'])
            curr_low = float(last['low'])
            
            # Breakout BB80
            if prev_low >= bb80_lo and curr_low < bb80_lo:
                breakout_detected = True
                breakout_level = 'BB80_LO'
            
            # OU breakout swing low récent
            else:
                try:
                    lookback = min(10, len(df) - 1)
                    window = df.iloc[-lookback:-1]
                    swing_low = window['low'].min()
                    
                    if prev_low >= swing_low and curr_low < swing_low:
                        breakout_detected = True
                        breakout_level = f'SWING_LOW_{swing_low:.4f}'
                except Exception:
                    pass
        
        if not breakout_detected:
            return None
        
        # ====== CONFIRMATION VOLUME (optionnel) ======
        
        try:
            volume_confirm = str(database.get_setting('PYRAMID_VOLUME_CONFIRM', 'true')).lower() == 'true'
            
            if volume_confirm:
                curr_vol = float(last.get('volume', 0))
                avg_vol = df['volume'].tail(20).mean()
                
                if curr_vol < avg_vol * 0.8:
                    return None
        except Exception:
            pass
        
        # ====== CALCUL TAILLE AJOUT ======
        
        try:
            pyramid_size_pct = float(database.get_setting('PYRAMID_SIZE_PCT', '50'))
        except Exception:
            pyramid_size_pct = 50.0
        
        initial_qty = float(pos['quantity'])
        add_qty = initial_qty * (pyramid_size_pct / 100.0)
        
        try:
            add_qty = float(ex.amount_to_precision(symbol, add_qty))
        except Exception:
            pass
        
        if add_qty <= 0:
            return None
        
        # ====== RETOUR INFOS PYRAMIDING ======
        
        return {
            'symbol': symbol,
            'side': pos['side'],
            'add_qty': add_qty,
            'current_price': current_price,
            'profit_pct': profit_pct,
            'breakout_level': breakout_level,
            'pyramid_count': pyramid_count,
            'position_id': pos['id']
        }
    
    except Exception as e:
        print(f"Erreur should_pyramid_position: {e}")
        return None


def execute_pyramid_add(ex, pyramid_info: Dict[str, Any]) -> bool:
    """
    Exécute l'ajout pyramiding sur une position gagnante.
    
    ACTIONS :
    1. Ouvrir position additionnelle
    2. Recalculer prix d'entrée moyen
    3. Ajuster SL (ne jamais reculer)
    4. Mettre à jour DB
    5. Notifier (SANS donner l'impression d'un nouveau trade)
    
    Args:
        ex: Exchange
        pyramid_info: Dict retourné par should_pyramid_position()
    
    Returns:
        True si succès
    """
    _ensure_bitget_mix_options(ex)
    
    try:
        symbol = pyramid_info['symbol']
        side = pyramid_info['side']
        add_qty = pyramid_info['add_qty']
        current_price = pyramid_info['current_price']
        position_id = pyramid_info['position_id']
        
        is_long = (side == 'buy')
        
        # ====== 1. OUVRIR POSITION ADDITIONNELLE ======
        
        common_params = {'tdMode': 'cross', 'posMode': 'oneway'}
        
        try:
            ex.set_leverage(LEVERAGE, symbol)
            try:
                ex.set_margin_mode('cross', symbol)
            except Exception:
                pass
            try:
                ex.set_position_mode(False, symbol)
            except Exception:
                pass
        except Exception:
            pass
        
        order = create_market_order_smart(
            ex, symbol, side, add_qty, 
            ref_price=current_price, 
            params=common_params
        )
        
        if not order:
            raise Exception("Ordre pyramiding échoué")
        
        filled_price = float(order.get('price', current_price))
        
        clear_balance_cache()
        
        # ====== 2. RÉCUPÉRER POSITION DB ======
        
        pos = database.get_trade_by_id(position_id)
        if not pos:
            raise Exception("Position introuvable en DB")
        
        old_qty = float(pos['quantity'])
        old_entry = float(pos['entry_price'])
        old_sl = float(pos.get('sl_price', old_entry))
        old_tp = float(pos.get('tp_price', old_entry))
        
        # ====== 3. CALCULER NOUVEAU PRIX MOYEN ======
        
        total_qty = old_qty + add_qty
        new_avg_entry = ((old_qty * old_entry) + (add_qty * filled_price)) / total_qty
        
        # ====== 4. AJUSTER SL (ne jamais reculer) ======
        
        new_sl = old_entry
        
        try:
            pyramid_sl_offset = float(database.get_setting('PYRAMID_SL_OFFSET_PCT', '1.0'))
        except Exception:
            pyramid_sl_offset = 1.0
        
        if is_long:
            proposed_sl = new_avg_entry * (1 - pyramid_sl_offset / 100)
            new_sl = max(old_sl, proposed_sl)
        else:
            proposed_sl = new_avg_entry * (1 + pyramid_sl_offset / 100)
            new_sl = min(old_sl, proposed_sl)
        
        # ====== 5. GARDER TP INITIAL (ou ajuster) ======
        
        new_tp = old_tp
        
        try:
            extend_tp = str(database.get_setting('PYRAMID_EXTEND_TP', 'false')).lower() == 'true'
            
            if extend_tp:
                tp_extension = float(database.get_setting('PYRAMID_TP_EXTENSION_PCT', '5.0'))
                
                if is_long:
                    new_tp = old_tp * (1 + tp_extension / 100)
                else:
                    new_tp = old_tp * (1 - tp_extension / 100)
        except Exception:
            pass
        
        # ====== 6. METTRE À JOUR EXCHANGE (SL/TP) ======
        
        close_side = 'sell' if is_long else 'buy'
        
        try:
            market = ex.market(symbol) or {}
            tick_size = _bitget_tick_size(market)
            
            mark_now = _current_mark_price(ex, symbol)
            new_sl = _validate_sl_for_side(side, float(new_sl), mark_now, tick_size)
            
            try:
                new_tp = _prepare_validated_tp(ex, symbol, close_side, float(new_tp))
            except Exception:
                pass
            
            try:
                new_sl = float(ex.price_to_precision(symbol, new_sl))
                new_tp = float(ex.price_to_precision(symbol, new_tp))
                total_qty_prec = float(ex.amount_to_precision(symbol, total_qty))
            except Exception:
                total_qty_prec = total_qty
            
            # ✅ ANNULER ANCIENS ORDRES TP/SL AVANT D'EN PLACER DE NOUVEAUX
            try:
                _cancel_all_orders_safe(ex, symbol)
                print(f"[pyramiding] Anciens ordres annulés pour {symbol}")
            except Exception as e:
                print(f"[pyramiding] Erreur annulation ordres: {e}")
            
            # ✅ PLACER NOUVEAUX TP/SL AVEC LOGS
            sl_ok, tp_ok = _place_sl_tp_safe(
                ex, symbol, close_side, total_qty_prec,
                sl=float(new_sl),
                tp=float(new_tp),
                params={**common_params, 'reduceOnly': True},
                is_long=is_long,
                tick_size=tick_size
            )
            
            if not sl_ok:
                print(f"[pyramiding] ⚠️ SL non placé sur {symbol}")
            else:
                print(f"[pyramiding] ✅ SL placé sur {symbol}: {new_sl}")
            
            if not tp_ok:
                print(f"[pyramiding] ⚠️ TP non placé sur {symbol}")
            else:
                print(f"[pyramiding] ✅ TP placé sur {symbol}: {new_tp}")
        
        except Exception as e:
            print(f"[pyramiding] Erreur placement SL/TP: {e}")
            import traceback
            traceback.print_exc()
        
        # ====== 7. METTRE À JOUR DB ======
        
        pyramid_count = int(pos.get('pyramid_count', 0)) + 1
        
        try:
            database.update_trade_pyramid(
                trade_id=position_id,
                new_quantity=float(total_qty),
                new_avg_entry=float(new_avg_entry),
                new_sl=float(new_sl),
                new_tp=float(new_tp),
                pyramid_count=pyramid_count
            )
        except AttributeError:
            try:
                database.update_trade_core(
                    trade_id=position_id,
                    side=side,
                    entry_price=float(new_avg_entry),
                    quantity=float(total_qty),
                    regime=pos.get('regime', 'Tendance')
                )
                database.update_trade_sl(position_id, float(new_sl))
                database.update_trade_tp(position_id, float(new_tp))
            except Exception:
                pass
        
        # ====== 8. ✅ NOTIFICATION AMÉLIORÉE (Pas comme un nouveau trade) ======
        
        try:
            # Calculer distances TP/BE
            if is_long:
                distance_to_tp = ((new_tp - current_price) / current_price) * 100
                distance_to_be = ((new_sl - current_price) / current_price) * 100
            else:
                distance_to_tp = ((current_price - new_tp) / current_price) * 100
                distance_to_be = ((current_price - new_sl) / current_price) * 100
            
            notifier.tg_send(
                f"📈 **PYRAMIDING #{pyramid_count}**\n\n"
                f"{'🟢' if is_long else '🔴'} {symbol} {side.upper()}\n"
                f"💚 Profit actuel : +{pyramid_info['profit_pct']:.2f}%\n"
                f"📊 Breakout : {pyramid_info['breakout_level']}\n\n"
                f"➕ **Ajout position**\n"
                f"  • Quantité ajoutée : {add_qty:.6f}\n"
                f"  • Prix d'ajout : {filled_price:.4f}\n"
                f"  • Prix moyen : {old_entry:.4f} → {new_avg_entry:.4f}\n\n"
                f"📦 **Position totale**\n"
                f"  • Quantité : {old_qty:.6f} → {total_qty:.6f}\n\n"
                f"🎯 **Nouveaux objectifs**\n"
                f"  • TP : {new_tp:.4f} ({distance_to_tp:+.2f}%)\n"
                f"  • BE : {new_sl:.4f} ({distance_to_be:+.2f}%)\n\n"
                f"🔢 Trade #{position_id}"
            )
        except Exception as e:
            # Fallback notification simple
            try:
                notifier.tg_send(
                    f"📈 PYRAMIDING #{pyramid_count}\n"
                    f"{symbol} : +{add_qty:.6f} @ {filled_price:.4f}\n"
                    f"Total : {total_qty:.6f}\n"
                    f"TP : {new_tp:.4f} | BE : {new_sl:.4f}"
                )
            except Exception:
                pass
            print(f"Erreur notification pyramiding: {e}")
        
        return True
    
    except Exception as e:
        try:
            notifier.tg_send(f"❌ Erreur pyramiding {pyramid_info['symbol']}: {e}")
        except Exception:
            pass
        print(f"Erreur execute_pyramid_add: {e}")
        import traceback
        traceback.print_exc()
        return False

def should_take_partial_profit(pos: dict, current_price: float) -> Optional[Dict[str, Any]]:
    """
    Détermine si on doit prendre un profit partiel.
    
    PALIERS DARWIN :
    - 50% du chemin vers TP → Close 40% position
    - 75% du chemin vers TP → Close 30% additionnel (70% total)
    - 100% TP → Close le reste (100%)
    
    Args:
        pos: Position DB
        current_price: Prix actuel
    
    Returns:
        Dict avec infos partial exit si conditions OK, sinon None
    """
    try:
        # ====== FEATURE ACTIVÉE ? ======
        
        try:
            enable_partial = str(database.get_setting('ENABLE_PARTIAL_EXITS', 'false')).lower() == 'true'
        except Exception:
            enable_partial = False
        
        if not enable_partial:
            return None
        
        # ====== RÉCUPÉRER INFOS POSITION ======
        
        entry_price = float(pos['entry_price'])
        tp_price = float(pos.get('tp_price', 0))
        qty_remaining = float(pos.get('quantity', 0))
        is_long = (pos['side'] == 'buy')
        
        if tp_price <= 0 or qty_remaining <= 0:
            return None
        
        # ====== CALCULER PROGRESSION VERS TP ======
        
        if is_long:
            if tp_price <= entry_price or current_price <= entry_price:
                return None
            if current_price >= tp_price:
                progress = 1.0
            else:
                progress = (current_price - entry_price) / (tp_price - entry_price)
        else:
            if tp_price >= entry_price or current_price >= entry_price:
                return None
            if current_price <= tp_price:
                progress = 1.0
            else:
                progress = (entry_price - current_price) / (entry_price - tp_price)
        
        progress = max(0.0, min(1.0, progress))
        
        # ====== VÉRIFIER PALIERS ======
        
        try:
            partial_exits = pos.get('partial_exits', {})
            if isinstance(partial_exits, str):
                import json
                partial_exits = json.loads(partial_exits)
        except Exception:
            partial_exits = {}
        
        try:
            palier_50_pct = float(database.get_setting('PARTIAL_EXIT_50_PCT', '40'))
            palier_75_pct = float(database.get_setting('PARTIAL_EXIT_75_PCT', '30'))
        except Exception:
            palier_50_pct = 40.0
            palier_75_pct = 30.0
        
        exit_info = None
        
        if progress >= 0.75 and not partial_exits.get('75'):
            exit_info = {
                'palier': '75',
                'progress': progress,
                'close_pct': palier_75_pct,
                'close_qty': qty_remaining * (palier_75_pct / 100.0),
                'reason': '75% du TP atteint'
            }
        
        elif progress >= 0.50 and not partial_exits.get('50'):
            exit_info = {
                'palier': '50',
                'progress': progress,
                'close_pct': palier_50_pct,
                'close_qty': qty_remaining * (palier_50_pct / 100.0),
                'reason': '50% du TP atteint'
            }
        
        if not exit_info:
            return None
        
        # ====== CALCULER PROFIT RÉALISÉ ======
        
        if is_long:
            profit_per_unit = current_price - entry_price
        else:
            profit_per_unit = entry_price - current_price
        
        profit_usdt = exit_info['close_qty'] * profit_per_unit
        
        exit_info.update({
            'symbol': pos['symbol'],
            'side': pos['side'],
            'position_id': pos['id'],
            'current_price': current_price,
            'entry_price': entry_price,
            'profit_usdt': profit_usdt,
            'profit_pct': (profit_per_unit / entry_price) * 100,
            'qty_remaining_after': qty_remaining - exit_info['close_qty']
        })
        
        return exit_info
    
    except Exception as e:
        print(f"Erreur should_take_partial_profit: {e}")
        return None


def execute_partial_exit(ex, exit_info: Dict[str, Any]) -> bool:
    """
    Exécute une sortie partielle.
    
    ACTIONS :
    1. Close X% de la position
    2. Mettre à jour quantité DB
    3. Ajuster SL (plus serré sur reste)
    4. Enregistrer le palier atteint
    5. Notifier
    
    Args:
        ex: Exchange
        exit_info: Dict retourné par should_take_partial_profit()
    
    Returns:
        True si succès
    """
    _ensure_bitget_mix_options(ex)
    
    try:
        symbol = exit_info['symbol']
        side = exit_info['side']
        close_qty = exit_info['close_qty']
        position_id = exit_info['position_id']
        palier = exit_info['palier']
        
        is_long = (side == 'buy')
        close_side = 'sell' if is_long else 'buy'
        
        # ====== 1. ARRONDIR QUANTITÉ ======
        
        try:
            close_qty = float(ex.amount_to_precision(symbol, close_qty))
        except Exception:
            pass
        
        if close_qty <= 0:
            return False
        
        # ====== 2. FERMER PARTIELLEMENT ======
        
        common_params = {'reduceOnly': True, 'tdMode': 'cross', 'posMode': 'oneway'}
        
        try:
            ex.set_leverage(LEVERAGE, symbol)
            try:
                ex.set_margin_mode('cross', symbol)
            except Exception:
                pass
            try:
                ex.set_position_mode(False, symbol)
            except Exception:
                pass
        except Exception:
            pass
        
        try:
            t = ex.fetch_ticker(symbol) or {}
            ref_px = float(t.get('last') or t.get('close') or exit_info['current_price'])
        except Exception:
            ref_px = exit_info['current_price']
        
        order = create_market_order_smart(
            ex, symbol, close_side, close_qty,
            ref_price=ref_px,
            params=common_params
        )
        
        if not order:
            raise Exception("Ordre partial exit échoué")
        
        exit_price = float(order.get('price', ref_px))
        
        clear_balance_cache()
        
        # ====== 3. METTRE À JOUR DB ======
        
        pos = database.get_trade_by_id(position_id)
        if not pos:
            raise Exception("Position introuvable")
        
        old_qty = float(pos['quantity'])
        new_qty = old_qty - close_qty
        
        if new_qty < 0:
            new_qty = 0
        
        try:
            database.update_trade_quantity(position_id, float(new_qty))
        except AttributeError:
            try:
                database.update_trade_core(
                    trade_id=position_id,
                    side=side,
                    entry_price=float(pos['entry_price']),
                    quantity=float(new_qty),
                    regime=pos.get('regime', 'Tendance')
                )
            except Exception:
                pass
        
        try:
            partial_exits = pos.get('partial_exits', {})
            if isinstance(partial_exits, str):
                import json
                partial_exits = json.loads(partial_exits)
            
            partial_exits[palier] = {
                'qty_closed': close_qty,
                'exit_price': exit_price,
                'profit_usdt': exit_info['profit_usdt'],
                'timestamp': int(time.time())
            }
            
            database.update_trade_meta(position_id, {'partial_exits': partial_exits})
        except Exception as e:
            print(f"Erreur enregistrement partial_exits: {e}")
        
        # ====== 4. AJUSTER SL (plus serré sur reste) ======
        
        if new_qty > 0:
            try:
                entry = float(pos['entry_price'])
                old_sl = float(pos.get('sl_price', entry))
                tp = float(pos.get('tp_price', entry))
                
                try:
                    sl_tighten_pct = float(database.get_setting('PARTIAL_EXIT_SL_TIGHTEN_PCT', '50'))
                except Exception:
                    sl_tighten_pct = 50.0
                
                profit_range = exit_info['current_price'] - entry if is_long else entry - exit_info['current_price']
                
                if is_long:
                    new_sl = entry + (profit_range * sl_tighten_pct / 100)
                    new_sl = max(old_sl, new_sl)
                else:
                    new_sl = entry - (profit_range * sl_tighten_pct / 100)
                    new_sl = min(old_sl, new_sl)
                
                market = ex.market(symbol) or {}
                tick_size = _bitget_tick_size(market)
                mark_now = _current_mark_price(ex, symbol)
                
                new_sl = _validate_sl_for_side(side, float(new_sl), mark_now, tick_size)
                
                try:
                    new_sl = float(ex.price_to_precision(symbol, new_sl))
                    new_qty_prec = float(ex.amount_to_precision(symbol, new_qty))
                except Exception:
                    new_qty_prec = new_qty
                
                ex.create_order(
                    symbol, 'market', close_side, new_qty_prec, price=None,
                    params={**common_params, 'stopLossPrice': float(new_sl), 'triggerType': 'mark'}
                )
                ex.create_order(
                    symbol, 'market', close_side, new_qty_prec, price=None,
                    params={**common_params, 'takeProfitPrice': float(tp), 'triggerType': 'mark'}
                )
                
                try:
                    database.update_trade_sl(position_id, float(new_sl))
                except Exception:
                    pass
            
            except Exception as e:
                print(f"Erreur ajustement SL après partial exit: {e}")
        
        # ====== 5. SI POSITION ENTIÈREMENT FERMÉE ======
        
        if new_qty <= 0:
            try:
                database.close_trade(position_id, status='CLOSED_PARTIAL_COMPLETE', pnl=0.0)
            except Exception:
                pass
        
        # ====== 6. NOTIFICATION ======
        
        try:
            remaining_pct = (new_qty / old_qty) * 100 if old_qty > 0 else 0
            
            notifier.tg_send(
                f"💰 **PROFIT PARTIEL #{palier}%**\n\n"
                f"🎯 {symbol} {side.upper()}\n"
                f"📊 Progression : {exit_info['progress']*100:.1f}%\n\n"
                f"✂️ Fermeture partielle :\n"
                f"  • Fermé : {close_qty:.6f} ({exit_info['close_pct']:.0f}%)\n"
                f"  • Reste : {new_qty:.6f} ({remaining_pct:.0f}%)\n\n"
                f"💵 Prix de sortie : {exit_price:.4f}\n"
                f"💰 Profit réalisé : +{exit_info['profit_usdt']:.2f} USDT\n"
                f"📈 Profit % : +{exit_info['profit_pct']:.2f}%\n\n"
                f"🛡️ SL resserré sur reste"
            )
        except Exception:
            pass
        
        return True
    
    except Exception as e:
        try:
            notifier.tg_send(f"❌ Erreur partial exit {exit_info['symbol']}: {e}")
        except Exception:
            pass
        print(f"Erreur execute_partial_exit: {e}")
        return False



def manage_open_positions(ex):
    """
    Gestion positions ouvertes : sync, fermeture, TP dynamique, BE, trailing, pyramiding, partial exits.
    Inclut protection anti-recul BE + notification PnL sécurisé RÉEL + ANTI-SPAM BE.
    
    ✅ CORRECTIONS FINALES :
    - Variable pnl_secured définie avant utilisation
    - Migration TA-Lib → Colonnes DataFrame (utils.py)
    - Pyramiding : create_market_order_smart
    - Partial exits : create_market_order_smart
    - TP DYNAMIQUE : Utilise adjust_tp_for_bb_offset
    - BE ANTI-SPAM : Mise à jour meta['be_notified'] = True
    """
    
    def _sl_improves_or_equal(new_sl: float, old_sl: float, is_long: bool) -> bool:
        """
        Vérifie que nouveau SL est amélioration (ou égal).
        
        RÈGLE ABSOLUE : BE ne recule JAMAIS
        - LONG : new_sl >= old_sl (monte ou stable)
        - SHORT : new_sl <= old_sl (descend ou stable)
        """
        try:
            new_sl = float(new_sl)
            old_sl = float(old_sl)
            
            if is_long:
                return new_sl >= old_sl
            else:
                return new_sl <= old_sl
        except Exception:
            return False
    
    # ========== SYNC POSITIONS EXCHANGE ==========
    try:
        exch_pos = ex.fetch_positions()
    except Exception as e:
        print(f"❌ Erreur fetch_positions: {e}")
        exch_pos = []
    
    pos_map = {}
    for p in exch_pos:
        try:
            sym = str(p.get('symbol', ''))
            contracts = float(p.get('contracts') or 0.0)
            if contracts != 0.0:
                pos_map[sym] = p
        except Exception:
            continue
    
    # ========== FILTRAGE DOUBLONS (1 TRADE ACTIF PAR SYMBOLE) ==========
    db_trades = database.get_open_positions()
    active_per_symbol = {}
    for pos in db_trades:
        sym = pos.get('symbol', '')
        if sym not in active_per_symbol:
            active_per_symbol[sym] = []
        active_per_symbol[sym].append(pos)
    
    unique_trades = []
    for sym, trades_list in active_per_symbol.items():
        if len(trades_list) == 1:
            unique_trades.append(trades_list[0])
        else:
            trades_sorted = sorted(trades_list, key=lambda t: t.get('id', 0), reverse=True)
            unique_trades.append(trades_sorted[0])
            for dup in trades_sorted[1:]:
                try:
                    print(f"⚠️ Doublon détecté {sym} trade #{dup['id']} → Fermé")
                    database.close_trade(dup['id'], 'CLOSED_BY_EXCHANGE')
                except Exception:
                    pass
    
    for pos in unique_trades:
        try:
            symbol = pos.get('symbol', '')
            side = pos.get('side', 'BUY').upper()
            is_long = (side == 'BUY')
            qty = float(pos.get('quantity') or 0.0)
            
            if qty <= 0:
                continue
            
            # ========== DÉTECTION FERMETURE + NOTIFICATION PNL ==========
            if symbol not in pos_map:
                try:
                    entry = float(pos.get('entry_price') or 0.0)
                    sl_price = float(pos.get('sl_price') or 0.0)
                    tp_price = float(pos.get('tp_price') or 0.0)
                    
                    recent_trades = ex.fetch_my_trades(symbol, limit=50)
                    close_price = None
                    for t in reversed(recent_trades):
                        t_side = str(t.get('side', '')).upper()
                        close_side = 'SELL' if is_long else 'BUY'
                        if t_side == close_side:
                            close_price = float(t.get('price') or 0.0)
                            break
                    
                    if close_price is None:
                        try:
                            ticker = ex.fetch_ticker(symbol)
                            close_price = float(ticker.get('last') or 0.0)
                        except Exception:
                            close_price = entry
                    
                    # Calcul PnL
                    if is_long:
                        pnl = (close_price - entry) * qty
                    else:
                        pnl = (entry - close_price) * qty
                    
                    # Déterminer TP ou SL
                    hit_tp = False
                    hit_sl = False
                    
                    if tp_price > 0:
                        if is_long and close_price >= tp_price * 0.999:
                            hit_tp = True
                        elif not is_long and close_price <= tp_price * 1.001:
                            hit_tp = True
                    
                    if sl_price > 0 and not hit_tp:
                        if is_long and close_price <= sl_price * 1.001:
                            hit_sl = True
                        elif not is_long and close_price >= sl_price * 0.999:
                            hit_sl = True
                    
                    if hit_tp:
                        status = 'CLOSED_TP'
                        emoji = '🎯'
                        result = 'Take Profit atteint'
                        color = '$'
                    elif hit_sl:
                        status = 'CLOSED_SL'
                        emoji = '🛡️'
                        result = 'Stop Loss touché'
                        color = '🔴' if pnl < 0 else '🟡'
                    else:
                        status = 'CLOSED_BY_EXCHANGE'
                        emoji = '✅'
                        result = 'Fermé par exchange'
                        color = '🔵'
                    
                    database.close_trade(pos['id'], status)
                    
                    # Notification riche avec PnL
                    try:
                        pnl_pct = (pnl / (entry * qty)) * 100 if (entry * qty) > 0 else 0
                        
                        msg = (
                            f"{emoji} **{result}**\n\n"
                            f"📊 {symbol} {side}\n"
                            f"{'🟢' if is_long else '🔴'} {'LONG' if is_long else 'SHORT'}\n\n"
                            f"💵 Prix d'entrée : {entry:.4f}\n"
                            f"💵 Prix de sortie : {close_price:.4f}\n"
                            f"📦 Quantité : {qty:.6f}\n\n"
                            f"{color} **PnL : {pnl:+.2f} USDT ({pnl_pct:+.2f}%)**\n\n"
                            f"🔢 Trade #{pos['id']}"
                        )
                        
                        notifier.tg_send(msg)
                    except Exception as e:
                        print(f"❌ Erreur notification fermeture: {e}")
                    
                    print(f"✅ {symbol} fermé → {status} (PnL: {pnl:+.2f} USDT)")
                except Exception as e:
                    print(f"❌ Erreur détection fermeture {symbol}: {e}")
                continue
            
            # ========== GESTION TRADES MANUELS (IMPORTÉS) ==========
            try:
                meta = pos.get('meta', {})
                if isinstance(meta, str):
                    import json
                    meta = json.loads(meta)
                is_manual_trade = meta.get('manual_import', False)
            except Exception:
                is_manual_trade = False
            
            if is_manual_trade:
                try:
                    exch_pos_data = pos_map.get(symbol)
                    if not exch_pos_data:
                        continue
                    
                    exch_qty = abs(float(exch_pos_data.get('contracts') or 0.0))
                    db_qty = float(pos.get('quantity') or 0.0)
                    
                    if abs(exch_qty - db_qty) > 0.001:
                        try:
                            database.update_trade_core(
                                trade_id=pos['id'],
                                side=side.lower(),
                                entry_price=float(pos.get('entry_price') or 0.0),
                                quantity=float(exch_qty),
                                regime=pos.get('regime', 'Importé')
                            )
                            print(f"🔄 {symbol} Manuel : Qty sync {db_qty:.6f} → {exch_qty:.6f}")
                        except Exception as e:
                            print(f"❌ Erreur update qty manuel: {e}")
                except Exception as e:
                    print(f"❌ Erreur sync qty manuel {symbol}: {e}")
                continue
            
            # ========== TP DYNAMIQUE (SUIT BB80/BB20) ==========
            try:
                regime = pos.get('regime', 'Tendance')
                tp_price = float(pos.get('tp_price') or 0.0)
                sl_price = float(pos.get('sl_price') or 0.0)
                
                if tp_price > 0:
                    try:
                        # ✅ MIGRATION : Les colonnes BB/ATR sont déjà calculées par utils.fetch_and_prepare_df()
                        df = utils.fetch_and_prepare_df(ex, symbol, timeframe='1h')
                        
                        if df is None or len(df) < 80:
                            raise Exception("DataFrame insuffisant pour TP dynamique")
                        
                        # Accès direct aux colonnes (déjà calculées par utils.py)
                        last_row = df.iloc[-1]
                        
                        bb80_up_val = float(last_row.get('bb80_up', 0.0))
                        bb80_lo_val = float(last_row.get('bb80_lo', 0.0))
                        bb20_up_val = float(last_row.get('bb20_up', 0.0))
                        bb20_lo_val = float(last_row.get('bb20_lo', 0.0))
                        last_atr = float(last_row.get('atr', 0.0))
                        
                        if bb80_up_val <= 0 or bb80_lo_val <= 0 or bb20_up_val <= 0 or bb20_lo_val <= 0:
                            raise Exception("Colonnes BB manquantes ou invalides")
                        
                        # ✅ CORRECTION : Déterminer bande selon régime
                        if regime == 'Tendance':
                            tp_raw = bb80_up_val if is_long else bb80_lo_val
                        else:
                            tp_raw = bb20_up_val if is_long else bb20_lo_val
                        
                        if tp_raw > 0:
                            # ✅ CORRECTION : Utiliser fonction offset existante
                            target_tp = adjust_tp_for_bb_offset(
                                raw_tp=float(tp_raw),
                                side=('buy' if is_long else 'sell'),
                                atr=float(last_atr),
                                ref_price=float(tp_raw)
                            )
                            
                            current_tp = tp_price
                            
                            # Vérifier amélioration significative
                            try:
                                tp_update_eps = float(database.get_setting('TP_UPDATE_EPS', '0.0005'))
                            except Exception:
                                tp_update_eps = 0.0005
                            
                            if is_long:
                                improve = (target_tp < current_tp * (1.0 - tp_update_eps))
                            else:
                                improve = (target_tp > current_tp * (1.0 + tp_update_eps))
                            
                            if improve:
                                close_side = 'sell' if is_long else 'buy'
                                
                                try:
                                    market = ex.market(symbol) or {}
                                    tick_size = _bitget_tick_size(market)
                                except Exception:
                                    tick_size = 0.0001
                                
                                common_params = {'tdMode': 'cross', 'posMode': 'oneway'}
                                
                                sl_ok, tp_ok = _place_sl_tp_safe(
                                    ex, symbol, close_side, qty,
                                    sl=sl_price if sl_price > 0 else None,
                                    tp=target_tp,
                                    params=common_params,
                                    is_long=is_long,
                                    tick_size=tick_size
                                )
                                
                                if tp_ok:
                                    database.update_trade_tp(pos['id'], float(target_tp))
                                    print(f"✅ {symbol} TP dynamique : {current_tp:.4f} → {target_tp:.4f}")
                    
                    except Exception as e:
                        print(f"❌ Erreur TP dynamique {symbol}: {e}")
            
            except Exception as e:
                print(f"❌ Erreur TP dynamique {symbol}: {e}")
            
            # ========== PYRAMIDING (AJOUT POSITION GAGNANTE) ==========
            try:
                pyramid_info = should_pyramid_position(ex, pos, utils.fetch_and_prepare_df(ex, symbol, '1h'))
                
                if pyramid_info:
                    execute_pyramid_add(ex, pyramid_info)
            
            except Exception as e:
                print(f"❌ Erreur pyramiding {symbol}: {e}")
            
            # ========== PARTIAL EXITS (SORTIES PARTIELLES) ==========
            try:
                try:
                    ticker = ex.fetch_ticker(symbol)
                    current_price = float(ticker.get('last') or ticker.get('close') or 0.0)
                except Exception:
                    current_price = 0.0
                
                if current_price > 0:
                    exit_info = should_take_partial_profit(pos, current_price)
                    
                    if exit_info:
                        execute_partial_exit(ex, exit_info)
            
            except Exception as e:
                print(f"❌ Erreur partial exit {symbol}: {e}")
            
            # ========== BE DYNAMIQUE (CONTACT BB20_MID) + PNL SÉCURISÉ RÉEL + ANTI-SPAM ==========
            try:
                try:
                    be_enabled = str(database.get_setting('DYNAMIC_BE_ENABLED', 'true')).lower() == 'true'
                except Exception:
                    be_enabled = True
                
                if be_enabled:
                    entry = float(pos.get('entry_price') or 0.0)
                    sl_current = float(pos.get('sl_price') or 0.0)
                    
                    if entry > 0 and sl_current > 0:
                        be_status = pos.get('be_status', 'INACTIVE')
                        prev_be_status = be_status
                        
                        try:
                            # ✅ MIGRATION : Les colonnes BB sont déjà calculées par utils.fetch_and_prepare_df()
                            df = utils.fetch_and_prepare_df(ex, symbol, timeframe='1h')
                            
                            if df is None or len(df) < 25:
                                raise Exception("DataFrame insuffisant pour BE dynamique")
                            
                            # Accès direct à la colonne bb20_mid
                            bb20_mid_val = float(df.iloc[-1].get('bb20_mid', 0.0))
                            
                            if bb20_mid_val <= 0:
                                raise Exception("Colonne bb20_mid manquante ou invalide")
                            
                            try:
                                be_lookback = int(database.get_setting('BE_TOUCH_LOOKBACK', '2'))
                            except Exception:
                                be_lookback = 2
                            
                            last_n_close = df['close'].iloc[-be_lookback:].values
                            last_n_high = df['high'].iloc[-be_lookback:].values
                            last_n_low = df['low'].iloc[-be_lookback:].values
                            
                            touched_bb20_mid = False
                            for i in range(len(last_n_close)):
                                c_val = float(last_n_close[i])
                                h_val = float(last_n_high[i])
                                l_val = float(last_n_low[i])
                                
                                if is_long:
                                    if c_val >= bb20_mid_val * 0.998 or h_val >= bb20_mid_val * 0.998:
                                        touched_bb20_mid = True
                                        break
                                else:
                                    if c_val <= bb20_mid_val * 1.002 or l_val <= bb20_mid_val * 1.002:
                                        touched_bb20_mid = True
                                        break
                            
                            improve_sl = False
                            want_sl = sl_current
                            
                            # ✅ CORRECTION : Initialiser pnl_secured AVANT le bloc
                            pnl_secured = 0.0
                            
                            if touched_bb20_mid and be_status != 'ACTIVE':
                                try:
                                    be_offset_pct = float(database.get_setting('BE_OFFSET_PCT', '0.001'))
                                except Exception:
                                    be_offset_pct = 0.001
                                
                                if is_long:
                                    want_sl = entry * (1.0 + be_offset_pct)
                                else:
                                    want_sl = entry * (1.0 - be_offset_pct)
                                
                                improve_sl = True
                            
                            # ✅ PROTECTION ANTI-RECUL
                            if improve_sl and _sl_improves_or_equal(want_sl, sl_current, is_long):
                                close_side = 'sell' if is_long else 'buy'
                                
                                try:
                                    market = ex.market(symbol) or {}
                                    tick_size = _bitget_tick_size(market)
                                except Exception:
                                    tick_size = 0.0001
                                
                                common_params = {'tdMode': 'cross', 'posMode': 'oneway'}
                                
                                sl_ok, tp_ok = _place_sl_tp_safe(
                                    ex, symbol, close_side, qty,
                                    sl=want_sl,
                                    tp=float(pos.get('tp_price') or 0.0) if pos.get('tp_price') else None,
                                    params=common_params,
                                    is_long=is_long,
                                    tick_size=tick_size
                                )
                                
                                if sl_ok:
                                    database.update_trade_sl(pos['id'], float(want_sl))
                                    
                                    try:
                                        database.update_trade_to_breakeven(
                                            pos['id'],
                                            float(qty),
                                            float(want_sl)
                                        )
                                    except Exception:
                                        pass
                                    
                                    # ✅ ANTI-SPAM : Vérifier si déjà notifié
                                    try:
                                        meta = pos.get('meta', {})
                                        if isinstance(meta, str):
                                            import json
                                            meta = json.loads(meta)
                                        be_already_notified = meta.get('be_notified', False)
                                    except Exception:
                                        be_already_notified = False
                                    
                                    # ✅ NOTIFIER SEULEMENT SI PAS DÉJÀ NOTIFIÉ
                                    if prev_be_status != 'ACTIVE' and not be_already_notified:
                                        try:
                                            remaining_qty = float(qty)
                                            entry_price_be = float(pos.get('entry_price') or 0.0)
                                            be_price_placed = float(want_sl)
                                            
                                            # ✅ CALCUL PNL SÉCURISÉ RÉEL (BE - Entry)
                                            if entry_price_be <= 0 or remaining_qty <= 0:
                                                pnl_secured = 0.0
                                            else:
                                                if is_long:
                                                    pnl_secured = max(0.0, (be_price_placed - entry_price_be) * remaining_qty)
                                                else:
                                                    pnl_secured = max(0.0, (entry_price_be - be_price_placed) * remaining_qty)
                                            
                                            if is_manual_trade:
                                                notifier.tg_send(
                                                    f"🔵 **Trade Manuel → BE Activé**\n\n"
                                                    f"📊 {symbol} {side}\n"
                                                    f"$ PnL sécurisé : {pnl_secured:.2f} USDT\n"
                                                    f"🛡️ SL déplacé au Break-Even"
                                                )
                                            else:
                                                notifier.send_breakeven_notification(
                                                    symbol=symbol,
                                                    pnl_realised=float(pnl_secured),
                                                    remaining_qty=float(remaining_qty)
                                                )
                                            
                                            # ✅ MARQUER COMME NOTIFIÉ (CORRECTION CRITIQUE ANTI-SPAM)
                                            try:
                                                if not isinstance(meta, dict):
                                                    meta = {}
                                                meta['be_notified'] = True
                                                database.update_trade_meta(pos['id'], meta)
                                            except Exception as e:
                                                print(f"⚠️ Erreur update meta be_notified: {e}")
                                        
                                        except Exception as e:
                                            print(f"❌ Erreur notification BE {symbol}: {e}")
                                    
                                    print(f"✅ {symbol} BE activé : SL {sl_current:.4f} → {want_sl:.4f} (PnL sécurisé: {pnl_secured:.2f} USDT)")
                            elif improve_sl:
                                print(f"⚠️ {symbol} BE rejeté (recul interdit : {want_sl:.4f} vs {sl_current:.4f})")
                        
                        except Exception as e:
                            print(f"❌ Erreur BE dynamique {symbol}: {e}")
            
            except Exception as e:
                print(f"❌ Erreur BE dynamique {symbol}: {e}")
            
            # ========== TRAILING PAR PALIERS (25%, 50%, 75%) ==========
            try:
                try:
                    trail_stepped = str(database.get_setting('TRAIL_USE_STEPPED', 'true')).lower() == 'true'
                except Exception:
                    trail_stepped = True
                
                if trail_stepped:
                    entry = float(pos.get('entry_price') or 0.0)
                    tp_price = float(pos.get('tp_price') or 0.0)
                    sl_current = float(pos.get('sl_price') or 0.0)
                    
                    if entry > 0 and tp_price > 0 and sl_current > 0:
                        data_bars = ex.fetch_ohlcv(symbol, timeframe='1h', limit=50)
                        
                        if len(data_bars) >= 20:
                            import pandas as pd
                            df = pd.DataFrame(data_bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                            current_price = float(df['close'].iloc[-1])
                            
                            if is_long:
                                distance = tp_price - entry
                                progress = (current_price - entry) / distance if distance > 0 else 0
                            else:
                                distance = entry - tp_price
                                progress = (entry - current_price) / distance if distance > 0 else 0
                            
                            progress = max(0.0, min(1.0, progress))
                            
                            meta = pos.get('meta', {})
                            if isinstance(meta, str):
                                import json
                                meta = json.loads(meta)
                            
                            last_step = meta.get('trail_step', 0)
                            
                            try:
                                step_50_pct = float(database.get_setting('TRAIL_STEP_50_PCT', '0.01'))
                            except Exception:
                                step_50_pct = 0.01
                            
                            try:
                                step_75_pct = float(database.get_setting('TRAIL_STEP_75_PCT', '0.015'))
                            except Exception:
                                step_75_pct = 0.015
                            
                            new_step = 0
                            target_sl = None
                            
                            if progress >= 0.75 and last_step < 75:
                                if is_long:
                                    target_sl = entry + (distance * (0.5 + step_75_pct))
                                else:
                                    target_sl = entry - (distance * (0.5 + step_75_pct))
                                new_step = 75
                            
                            elif progress >= 0.50 and last_step < 50:
                                if is_long:
                                    target_sl = entry + (distance * (0.25 + step_50_pct))
                                else:
                                    target_sl = entry - (distance * (0.25 + step_50_pct))
                                new_step = 50
                            
                            elif progress >= 0.25 and last_step < 25:
                                if is_long:
                                    target_sl = entry + (distance * step_50_pct)
                                else:
                                    target_sl = entry - (distance * step_50_pct)
                                new_step = 25
                            
                            # ✅ PROTECTION ANTI-RECUL
                            if target_sl and _sl_improves_or_equal(target_sl, sl_current, is_long):
                                close_side = 'sell' if is_long else 'buy'
                                
                                try:
                                    market = ex.market(symbol) or {}
                                    tick_size = _bitget_tick_size(market)
                                except Exception:
                                    tick_size = 0.0001
                                
                                common_params = {'tdMode': 'cross', 'posMode': 'oneway'}
                                
                                sl_ok, tp_ok = _place_sl_tp_safe(
                                    ex, symbol, close_side, qty,
                                    sl=target_sl,
                                    tp=tp_price,
                                    params=common_params,
                                    is_long=is_long,
                                    tick_size=tick_size
                                )
                                
                                if sl_ok:
                                    database.update_trade_sl(pos['id'], float(target_sl))
                                    print(f"✅ {symbol} Trailing palier {new_step}% : SL {sl_current:.4f} → {target_sl:.4f}")
                            elif target_sl:
                                print(f"⚠️ {symbol} Trailing palier {new_step}% rejeté (recul interdit)")
            except Exception as e:
                print(f"❌ Erreur trailing paliers {symbol}: {e}")
            
            # ========== TRAILING FINAL SERRÉ (>90% TP) ==========
            try:
                entry = float(pos.get('entry_price') or 0.0)
                tp_price = float(pos.get('tp_price') or 0.0)
                sl_current = float(pos.get('sl_price') or 0.0)
                
                if entry > 0 and tp_price > 0 and sl_current > 0:
                    data_bars = ex.fetch_ohlcv(symbol, timeframe='1h', limit=50)
                    
                    if len(data_bars) >= 20:
                        import pandas as pd
                        df = pd.DataFrame(data_bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                        current_price = float(df['close'].iloc[-1])
                        
                        if is_long:
                            distance = tp_price - entry
                            progress = (current_price - entry) / distance if distance > 0 else 0
                        else:
                            distance = entry - tp_price
                            progress = (entry - current_price) / distance if distance > 0 else 0
                        
                        progress = max(0.0, min(1.0, progress))
                        
                        if progress >= 0.90:
                            try:
                                trail_min_move = float(database.get_setting('TRAIL_MIN_MOVE_PCT', '0.001'))
                            except Exception:
                                trail_min_move = 0.001
                            
                            try:
                                trail_offset = float(database.get_setting('TRAIL_FINAL_OFFSET_PCT', '0.002'))
                            except Exception:
                                trail_offset = 0.002
                            
                            if is_long:
                                want_sl = current_price * (1.0 - trail_offset)
                                move = (want_sl - sl_current) / sl_current if sl_current > 0 else 0
                            else:
                                want_sl = current_price * (1.0 + trail_offset)
                                move = (sl_current - want_sl) / sl_current if sl_current > 0 else 0
                            
                            # ✅ PROTECTION ANTI-RECUL
                            if move >= trail_min_move and _sl_improves_or_equal(want_sl, sl_current, is_long):
                                close_side = 'sell' if is_long else 'buy'
                                
                                try:
                                    market = ex.market(symbol) or {}
                                    tick_size = _bitget_tick_size(market)
                                except Exception:
                                    tick_size = 0.0001
                                
                                common_params = {'tdMode': 'cross', 'posMode': 'oneway'}
                                
                                sl_ok, tp_ok = _place_sl_tp_safe(
                                    ex, symbol, close_side, qty,
                                    sl=want_sl,
                                    tp=tp_price,
                                    params=common_params,
                                    is_long=is_long,
                                    tick_size=tick_size
                                )
                                
                                if sl_ok:
                                    database.update_trade_sl(pos['id'], float(want_sl))
                                    print(f"✅ {symbol} Trailing final : SL {sl_current:.4f} → {want_sl:.4f}")
                            elif move >= trail_min_move:
                                print(f"⚠️ {symbol} Trailing final rejeté (recul interdit)")
            except Exception as e:
                print(f"❌ Erreur trailing final {symbol}: {e}")
        
        except Exception as e:
            print(f"❌ Erreur manage_open_positions trade {pos.get('id')}: {e}")
            continue







def get_usdt_balance(ex: ccxt.Exchange) -> Optional[float]:
    """
    Retourne le solde USDT (équity portefeuille) en float.
    - Préfère get_portfolio_equity_usdt() pour Bitget/Bybit (équity globale).
    - Fallback sur diverses clés de fetch_balance() si nécessaire.
    - Met à jour settings.CURRENT_BALANCE_USDT si une valeur est trouvée.
    """
    # 1) Source principale : équity globale (Bitget/Bybit, etc.)
    equity = 0.0
    try:
        equity = float(get_portfolio_equity_usdt(ex))
    except Exception:
        equity = 0.0

    if equity > 0.0:
        try:
            database.set_setting("CURRENT_BALANCE_USDT", f"{equity:.6f}")
        except Exception:
            pass
        return equity

    # 2) Fallback direct sur la structure de balance CCXT
    try:
        bal = _fetch_balance_safe(ex)
    except Exception:
        bal = None

    if not bal:
        return None

    candidates: List[float] = []

    # Sections normalisées: total / free / used
    for section in ("total", "free", "used"):
        try:
            sec = bal.get(section) or {}
            if isinstance(sec, dict):
                for k in ("USDT", "USDT:USDT"):
                    v = sec.get(k)
                    if v is not None:
                        try:
                            candidates.append(float(v))
                        except Exception:
                            pass
        except Exception:
            pass

    # Entrées directes par devise (bal['USDT'], bal['USDT:USDT'])
    for k in ("USDT", "USDT:USDT"):
        try:
            coin = bal.get(k)
            if isinstance(coin, dict):
                for sub in ("total", "free", "availableBalance", "available"):
                    v = coin.get(sub)
                    if v is not None:
                        try:
                            candidates.append(float(v))
                        except Exception:
                            pass
        except Exception:
            pass

    if not candidates:
        return None

    balance_usdt = float(max(candidates))

    try:
        database.set_setting("CURRENT_BALANCE_USDT", f"{balance_usdt:.6f}")
    except Exception:
        pass

    return balance_usdt

def calculate_position_size(balance: float, risk_percent: float, entry_price: float, sl_price: float) -> float:
    """Calcule la quantité d'actifs à trader."""
    if balance <= 0 or entry_price == sl_price: return 0.0
    risk_amount_usdt = balance * (risk_percent / 100.0)
    price_diff_per_unit = abs(entry_price - sl_price)
    return risk_amount_usdt / price_diff_per_unit if price_diff_per_unit > 0 else 0.0


def close_position_manually(ex: ccxt.Exchange, trade_id: int):
    """(MODIFIÉ) Clôture manuelle robuste :
    - utilise create_market_order_smart() pour BUY et SELL
    - annule tous les ordres restants (TP/SL/BE) sur le symbole après fermeture.
    - invalide le cache solde après fermeture
    """
    _ensure_bitget_mix_options(ex)
    is_paper_mode = database.get_setting('PAPER_TRADING_MODE', 'true') == 'true'
    trade = database.get_trade_by_id(trade_id)
    if not trade or trade.get('status') != 'OPEN':
        return notifier.tg_send(f"Trade #{trade_id} déjà fermé ou invalide.")
    
    symbol = trade['symbol']
    side = trade['side']
    qty_db = float(trade['quantity'])

    try:
        # Contexte marge/levier/position
        try:
            ex.set_leverage(LEVERAGE, symbol)
            try:
                ex.set_margin_mode('cross', symbol)
            except Exception:
                pass
            try:
                ex.set_position_mode(False, symbol)
            except Exception:
                pass
        except Exception:
            pass

        # Quantité réelle côté exchange (sécurise si DB désync)
        real_qty = 0.0
        market = None
        try:
            market = ex.market(symbol)
        except Exception:
            pass

        try:
            positions = _fetch_positions_safe(ex, [symbol])
            for p in positions:
                same = (p.get('symbol') == symbol) or (market and p.get('raw', {}).get('symbol') == market.get('id'))
                if same:
                    # ⚠️ On lit d'abord 'size' (rempli par _fetch_positions_safe), puis fallback
                    contracts = float(p.get('size') or p.get('contracts') or p.get('positionAmt') or 0.0)
                    contracts = abs(contracts)
                    if contracts and contracts > 0:
                        real_qty = contracts
                        break
        except Exception:
            pass

        if real_qty <= 0:
            # Pas de position réelle → on ferme en DB et on nettoie les ordres éventuels par sécurité
            try:
                _cancel_all_orders_safe(ex, symbol)
            except Exception:
                pass
            database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
            
            # ✅ MODIFICATION 1 : Invalider cache (position déjà fermée côté exchange)
            clear_balance_cache()
            
            return notifier.tg_send(
                f"ℹ️ Aucune position ouverte détectée pour {symbol}. "
                f"Trade #{trade_id} marqué fermé et ordres annulés."
            )

        qty_to_close = min(qty_db, real_qty)
        try:
            qty_to_close = float(ex.amount_to_precision(symbol, qty_to_close))
        except Exception:
            pass
        if qty_to_close <= 0:
            try:
                _cancel_all_orders_safe(ex, symbol)
            except Exception:
                pass
            database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
            
            # ✅ MODIFICATION 2 : Invalider cache (quantité nulle = position fermée)
            clear_balance_cache()
            
            return notifier.tg_send(
                f"ℹ️ Quantité nulle à clôturer sur {symbol}. "
                f"Trade #{trade_id} marqué fermé et ordres annulés."
            )

        if not is_paper_mode:
            close_side = 'sell' if side == 'buy' else 'buy'
            params = {'reduceOnly': True, 'tdMode': 'cross', 'posMode': 'oneway'}

            # Prix de référence pour conversion qty→cost si nécessaire (Bitget BUY)
            try:
                t = ex.fetch_ticker(symbol) or {}
                ref_px = float(t.get('last') or t.get('close') or t.get('bid') or t.get('ask') or 0.0)
            except Exception:
                ref_px = 0.0

            # ✅ Unifie le chemin: toujours via create_market_order_smart (BUY & SELL)
            create_market_order_smart(
                ex, symbol, close_side, qty_to_close, ref_price=ref_px, params=params
            )
            
            # ✅ MODIFICATION 3 : Invalider cache APRÈS fermeture position (CRITIQUE)
            clear_balance_cache()

            # 🧹 Après fermeture de la position, on enlève tous les ordres restants (TP/SL/BE)
            try:
                _cancel_all_orders_safe(ex, symbol)
            except Exception:
                pass

        else:
            # En mode papier, on ne touche pas l'exchange, mais on peut tout de même nettoyer les ordres
            try:
                _cancel_all_orders_safe(ex, symbol)
            except Exception:
                pass

        database.close_trade(trade_id, status='CLOSED_MANUAL', pnl=0.0)
        
        # ✅ MODIFICATION 4 : Invalider cache après close en DB (sécurité finale)
        clear_balance_cache()
        
        notifier.tg_send(f"✅ Position sur {symbol} (Trade #{trade_id}) fermée manuellement (qty={qty_to_close}).")

    except Exception as e:
        notifier.tg_send_error(f"Fermeture manuelle de {symbol}", e)
