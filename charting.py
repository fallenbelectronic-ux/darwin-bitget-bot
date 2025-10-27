# Fichier: charting.py
import io
import pandas as pd
import mplfinance as mpf
from typing import Dict, Any, Optional

def generate_trade_chart(symbol: str, df: pd.DataFrame, signal: Dict[str, Any]) -> Optional[io.BytesIO]:
    """Génère une image du graphique pour un trade, avec les deux Bandes de Bollinger."""
    try:
        # Colonnes nécessaires pour ce graphique
        required_cols = ['bb20_up', 'bb20_lo', 'bb80_mid', 'bb80_up', 'bb80_lo']
        if not all(col in df.columns for col in required_cols):
            print("Erreur graphique: le DataFrame ne contient pas toutes les colonnes de BB nécessaires.")
            return None

        # Style
        style = mpf.make_mpf_style(base_mpf_style='charles', gridstyle=':')

        # Indicateurs (on limite à 100 bougies pour garder un rendu lisible)
        plots = [
            mpf.make_addplot(df['bb20_up'].tail(100), color='lightblue', width=0.8),
            mpf.make_addplot(df['bb20_lo'].tail(100), color='lightblue', width=0.8),
            mpf.make_addplot(df['bb80_up'].tail(100), color='orange', linestyle='dashdot', width=0.9),
            mpf.make_addplot(df['bb80_lo'].tail(100), color='orange', linestyle='dashdot', width=0.9),
            mpf.make_addplot(df['bb80_mid'].tail(100), color='orange', width=1.2),
        ]

        # Lignes horizontales pour Entrée / SL / TP (cast en float pour robustesse)
        h_prices  = [float(signal['entry']), float(signal['sl']), float(signal['tp'])]
        h_colors  = ['#007FFF', '#FF0000', '#00FF00']
        h_widths  = [1.2, 1.2, 1.2]

        fig, axes = mpf.plot(
            df.tail(100),
            type='candle',
            style=style,
            title=f"Setup de Trade: {symbol} ({signal['regime']})",
            ylabel='Prix (USDT)',
            addplot=plots,
            hlines=dict(hlines=h_prices, colors=h_colors, linewidths=h_widths, alpha=0.9),
            returnfig=True,
            figsize=(12, 7)
        )

        # Annotations texte à droite, avec fond blanc semi-transparent pour la lisibilité
        ax = axes[0]
        last_x = ax.get_xlim()[1]
        labels = ['Entrée', 'SL', 'TP']
        for y, label, color in zip(h_prices, labels, h_colors):
            ax.text(
                last_x, y, f"  {label}",
                va='center', ha='left', color=color, fontsize=9,
                bbox=dict(facecolor='white', alpha=0.6, edgecolor='none', pad=1.5)
            )

        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
        buf.seek(0)

        # Fermer la figure pour éviter les fuites de mémoire
        try:
            import matplotlib.pyplot as plt
            plt.close(fig)
        except Exception:
            fig.clf()

        return buf

    except Exception as e:
        print(f"Erreur de génération de graphique: {e}")
        return None
