import io
import pandas as pd
import mplfinance as mpf
from typing import Dict, Any, Optional

def generate_trade_chart(symbol: str, df: pd.DataFrame, signal: Dict[str, Any]) -> Optional[io.BytesIO]:
    """Génère une image PNG (fond sombre) pour un trade, avec:
       - BB20 turquoise (lignes pleines) + MM20 turquoise pointillé
       - BB80 bleu (plus épais)      + MM80 bleu pointillé
       Retourne un io.BytesIO prêt pour Telegram."""
    try:
        # Colonnes nécessaires (inclut bb20_mid pour tracer la MM20)
        required_cols = ['bb20_up', 'bb20_mid', 'bb20_lo', 'bb80_up', 'bb80_mid', 'bb80_lo']
        if not all(col in df.columns for col in required_cols):
            print("Erreur graphique: le DataFrame ne contient pas toutes les colonnes de BB nécessaires.")
            return None

        # Fenêtre compacte
        BARS = 20

        # Assurer un DatetimeIndex pour mplfinance
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df = df.copy()
                df.index = pd.to_datetime(df.index, utc=True)
            except Exception:
                return None

        # Sous-DataFrame OHLCV avec noms attendus par mplfinance
        df_plot = df[['open', 'high', 'low', 'close', 'volume']].tail(BARS).copy()
        df_plot.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        # Thème sombre
        bg_fig   = '#121417'   # fond global
        bg_ax    = '#0f1215'   # fond axe
        grid_col = '#1f2937'

        mc = mpf.make_marketcolors(
            up='#22c55e',
            down='#ef4444',
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

        # Couleurs BB
        col_turq = '#2dd4bf'   # BB20
        col_blue = '#60a5fa'   # BB80

        # Séries addplot alignées sur la même fenêtre
        seg = slice(-BARS, None)
        plots = [
            # BB20 (pleines) + MM20 (pointillé)
            mpf.make_addplot(df['bb20_up'].iloc[seg],  color=col_turq, linestyle='-',  width=1.2),
            mpf.make_addplot(df['bb20_lo'].iloc[seg],  color=col_turq, linestyle='-',  width=1.2),
            mpf.make_addplot(df['bb20_mid'].iloc[seg], color=col_turq, linestyle='--', width=1.2),

            # BB80 (plus épaisses) + MM80 (pointillé)
            mpf.make_addplot(df['bb80_up'].iloc[seg],  color=col_blue, linestyle='-',  width=1.6),
            mpf.make_addplot(df['bb80_lo'].iloc[seg],  color=col_blue, linestyle='-',  width=1.6),
            mpf.make_addplot(df['bb80_mid'].iloc[seg], color=col_blue, linestyle='--', width=1.2),
        ]

        # Lignes horizontales Entrée / SL / TP
        h_prices = [float(signal['entry']), float(signal['sl']), float(signal['tp'])]
        h_colors = ['#60a5fa', '#ef4444', '#22c55e']  # bleu, rouge, vert
        h_widths = [1.2, 1.2, 1.2]

        fig, axes = mpf.plot(
            df_plot,
            type='candle',
            style=style,
            title=f"Setup de Trade: {symbol} ({signal['regime']})",
            ylabel='Prix (USDT)',
            addplot=plots,
            hlines=dict(hlines=h_prices, colors=h_colors, linewidths=h_widths, alpha=0.95),
            returnfig=True,
            figsize=(12, 7)
        )

        # Annotations à droite
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

        # Clean
        try:
            import matplotlib.pyplot as plt
            plt.close(fig)
        except Exception:
            fig.clf()

        return buf

    except Exception as e:
        print(f"Erreur de génération de graphique: {e}")
        return None
