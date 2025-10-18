import os
import time
import ccxt
import pandas as pd
import numpy as np
from ta.volatility import BollingerBands
from dotenv import load_dotenv
from notifier import tg_send

load_dotenv()

# --- Config depuis les variables d'environnement ---
BYBIT_TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() in ("1", "true", "yes")
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT").split(",") if s.strip()]
TF = os.getenv("TIMEFRAME", "1m")
HIGHER_TF = os.getenv("HIGHER_TF", "5m")
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.002"))
MIN_RR = float(os.getenv("MIN_RR", "3"))
MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "10"))
LOOP_DELAY = int(os.getenv("LOOP_DELAY", "5"))
POSITION_MODE = os.getenv("POSITION_MODE", "isolated")


# --- Initialisation de l’exchange Bybit ---
def create_exchange():
    exchange = ccxt.bybit({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "enableRateLimit": True,
        "options": {"defaultType": "future"}  # important : futures USDT
    })
    if BYBIT_TESTNET:
        try:
            exchange.set_sandbox_mode(True)
            print("[INFO] Bybit sandbox mode activé (testnet)")
        except Exception:
            print("[WARN] sandbox mode non supporté (ccxt)")
    return exchange


# --- Téléchargement des données OHLCV ---
def fetch_ohlcv_df(exchange, symbol, timeframe, limit=200):
    raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "vol"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    df.set_index("ts", inplace=True)
    return df


# --- Calcul des bandes de Bollinger ---
def add_bbands(df, period=20, std=2, prefix='bb'):
    bb = BollingerBands(close=df['close'], window=period, window_dev=std)
    df[f'{prefix}_mid'] = bb.bollinger_mavg()
    df[f'{prefix}_upper'] = bb.bollinger_hban]()
