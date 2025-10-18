import os
import time
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands
from dotenv import load_dotenv
from notifier import tg_send

load_dotenv()

# --- Config depuis les variables d'environnement (BITGET) ---
BITGET_TESTNET = os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY = os.getenv("BITGET_API_KEY")
API_SECRET = os.getenv("BITGET_API_SECRET")
PASSPHRASE = os.getenv("BITGET_PASSPHRASE")  # Bitget a une passphrase
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTC/USDT:USDT").split(",") if s.strip()]
TF = os.getenv("TIMEFRAME", "1m")
HIGHER_TF = os.getenv("HIGHER_TF", "5m")
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.002"))
MIN_RR = float(os.getenv("MIN_RR", "3"))
MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "10"))
LOOP_DELAY = int(os.getenv("LOOP_DELAY", "5"))
POSITION_MODE = os.getenv("POSITION_MODE", "cross")  # cross par défaut sur Bitget

# --- Initialisation de l’exchange Bitget ---
def create_exchange():
    exchange = ccxt.bitget({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "password": PASSPHRASE,           # requis par Bitget
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap"         # Perp/Futures USDT linéaires
        }
    })
    if BITGET_TESTNET:
        try:
            exchange.set_sandbox_mode(True)   # testnet si disponible via CCXT
            print("[INFO] Bitget sandbox mode activé (testnet)")
        except Exception:
            print("[WARN] Sandbox/testnet non supporté par cette version, reste en prod (attention).")
    return exchange

# --- Téléchargement des données OHLCV ---
def fetch_ohlcv_df(exchange, symbol, timeframe, limit=200):
    raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts", "open", "high]()
