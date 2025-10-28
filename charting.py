import io
import pandas as pd
import mplfinance as mpf
from typing import Dict, Any, Optional

def generate_trade_chart(symbol: str, df: pd.DataFrame, signal: Dict[str, Any]) -> Optional[io.BytesIO]:
    """Génère une image du graphique pour un trade, fond sombre + BB20/BB80 stylisées.
       - Fond sombre/gris
       - BB20 turquoise (lignes pleines), MM20 turquoise pointillé
       - BB80 bleu (plus épais), MM80 bleu pointillé
    """
    try:
        # Colonnes nécessaires (ajout de bb20_mid pour tracer la MM20)
        required_cols = ['bb20_up', 'bb20_mid', 'bb20_lo', 'bb80_mid', 'bb80_up', 'bb80_lo']
        if not all(col in df.columns for col in required_cols):
            print("Erreur graphique: le DataFrame ne contient pas toutes les colonnes de BB nécessaires.")
            return None

        # Zoom: fenêtre compacte pour plus de lisibilité
        BARS = 20

        # ---- Thème sombre (mplfinance) ----
        # Couleurs
        bg_fig   = '#121417'  # fond global
        bg_ax    = '#0f1215'  # fond axe
        grid_col = '#1f2937'

        # Marché (couleurs bougies)
        mc = mpf.make_marketcolors(
            up='#22c55e',    # vert doux
            down='#ef4444',  # rouge doux
            edge='inherit',
            wick='inherit',
            volume='in'
        )
        style = mpf.make_mpf_style(
            base_mpf_style='nightclouds',
            marketcolors=mc,
            gridstyle='-',
            gridcolor=grid_col,
            facecolor=bg_ax,
            figcolor=bg_fig,
            edgecolor=bg_ax
        )

        # ---- Indicateurs (sur BARS bougies) ----
        col_turq = '#2dd4bf'  # turquoise
        col_blue = '#60a5fa'  # bleu

        plots = [
            # BB20 (turquoise, lignes pleines) + MM20 (pointillé)
            mpf.make_addplot(df['bb20_up'].tail(BARS),  color=col_turq, linestyle='-',  width=1.2),
            mpf.make_addplot(df['bb20_lo'].tail(BARS),  color=col_turq, linestyle='-',  width=1.2),
            mpf.make_addplot(df['bb20_mid'].tail(BARS), color=col_turq, linestyle='--', width=1.2),

            # BB80 (bleu, plus épais) + MM80 (pointillé)
            mpf.make_addplot(df['bb80_up'].tail(BARS),  color=col_blue, linestyle='-',  width=1.6),
            mpf.make_addplot(df['bb80_lo'].tail(BARS),  color=col_blue, linestyle='-',  width=1.6),
            mpf.make_addplot(df['bb80_mid'].tail(BARS), color=col_blue, linestyle='--', width=1.2),
        ]

        # Lignes horizontales Entrée / SL / TP
        h_prices = [float(signal['entry']), float(signal['sl']), float(signal['tp'])]
        h_colors = ['#60a5fa', '#ef4444', '#22c55e']  # bleu, rouge, vert
        h_widths = [1.2, 1.2, 1.2]

        fig, axes = mpf.plot(
            df.tail(BARS),
            type='candle',
            style=style,
            title=f"Setup de Trade: {symbol} ({signal['regime']})",
            ylabel='Prix (USDT)',
            addplot=plots,
            hlines=dict(hlines=h_prices, colors=h_colors, linewidths=h_widths, alpha=0.95),
            returnfig=True,
            figsize=(12, 7)
        )

        # Annotations texte à droite (fond sombre semi-transparent)
        ax = axes[0]
        last_x = ax.get_xlim()[1]
        labels = ['Entrée', 'SL', 'TP']
        for y, label, color in zip(h_prices, labels, h_colors):
            ax.text(
                last_x, y, f"  {label}",
                va='center', ha='left', color=color, fontsize=9,
                bbox=dict(facecolor='#0b0f14', alpha=0.7, edgecolor='none', pad=1.5)
            )

        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=110)
        buf.seek(0)

        # Nettoyage figure pour éviter les fuites mémoire
        try:
            import matplotlib.pyplot as plt
            plt.close(fig)
        except Exception:
            fig.clf()

        return buf

    except Exception as e:
        print(f"Erreur de génération de graphique: {e}")
        return None
