# Fichier: charting.py
import io
import pandas as pd
import mplfinance as mpf
from typing import Dict, Any, Optional

def generate_trade_chart(symbol: str, df: pd.DataFrame, signal: Dict[str, Any]) -> Optional[io.BytesIO]:
    """Génère une image du graphique pour un trade."""
    try:
        lines = [
            (signal['entry'], '#007FFF', 'Entrée'),
            (signal['sl'], '#FF0000', 'SL'),
            (signal['tp'], '#00FF00', 'TP')
        ]
        style = mpf.make_mpf_style(base_mpf_style='charles', gridstyle=':')
        fig, axes = mpf.plot(
            df.tail(70), type='candle', style=style,
            title=f"Setup de Trade: {symbol}", ylabel='Prix (USDT)',
            addplot=[
                mpf.make_addplot(df['bb20_up'].tail(70), color='lightblue', width=0.8),
                mpf.make_addplot(df['bb20_lo'].tail(70), color='lightblue', width=0.8),
                mpf.make_addplot(df['bb80_mid'].tail(70), color='orange', width=1.2),
            ],
            alines=dict(alines=lines, colors=['#007FFF', '#FF0000', '#00FF00'], linewidths=1.2),
            returnfig=True,
            figsize=(11, 6)
        )
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
        buf.seek(0)
        return buf
    except Exception as e:
        print(f"Erreur de génération de graphique: {e}")
        return None
