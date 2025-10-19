# Fichier: charting.py
import io
import pandas as pd
import mplfinance as mpf
from typing import Dict, Any, Optional

def generate_trade_chart(symbol: str, df: pd.DataFrame, signal: Dict[str, Any]) -> Optional[io.BytesIO]:
    """Génère une image du graphique pour un trade, avec les deux Bandes de Bollinger."""
    try:
        # Assurer que les colonnes nécessaires existent (calculées dans main.py)
        required_cols = ['bb20_up', 'bb20_lo', 'bb80_mid', 'bb80_up', 'bb80_lo']
        if not all(col in df.columns for col in required_cols):
            print("Erreur graphique: le DataFrame ne contient pas toutes les colonnes de BB nécessaires.")
            return None

        # Lignes pour l'entrée, SL et TP
        lines = [
            (signal['entry'], '#007FFF', 'Entrée'),
            (signal['sl'], '#FF0000', 'SL'),
            (signal['tp'], '#00FF00', 'TP')
        ]
        
        # Style du graphique
        style = mpf.make_mpf_style(base_mpf_style='charles', gridstyle=':')
        
        # Définition des indicateurs à tracer
        plots = [
            # BB(20,2) - "BB Blanche"
            mpf.make_addplot(df['bb20_up'].tail(100), color='lightblue', width=0.8),
            mpf.make_addplot(df['bb20_lo'].tail(100), color='lightblue', width=0.8),
            
            # BB(80,2) - "BB Jaune"
            mpf.make_addplot(df['bb80_up'].tail(100), color='orange', linestyle='dashdot', width=0.9),
            mpf.make_addplot(df['bb80_lo'].tail(100), color='orange', linestyle='dashdot', width=0.9),
            mpf.make_addplot(df['bb80_mid'].tail(100), color='orange', width=1.2)
        ]

        # Génération de la figure
        fig, axes = mpf.plot(
            df.tail(100), type='candle', style=style,
            title=f"Setup de Trade: {symbol} ({signal['regime']})",
            ylabel='Prix (USDT)',
            addplot=plots,
            alines=dict(alines=lines, colors=['#007FFF', '#FF0000', '#00FF00'], linewidths=1.2),
            returnfig=True,
            figsize=(12, 7)
        )
        
        # Sauvegarde de l'image en mémoire
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
        buf.seek(0)
        return buf
        
    except Exception as e:
        print(f"Erreur de génération de graphique: {e}")
        return None
