import io
import pandas as pd
import mplfinance as mpf
from typing import Dict, Any, Optional

def generate_trade_chart(symbol: str, df: pd.DataFrame, signal: Dict[str, Any]) -> Optional[io.BytesIO]:
    """Génère une image PNG (fond sombre) pour un trade.
       Gère df=None (auto-fetch + préparation), colonnes manquantes (volume),
       et hlines optionnelles (entry/sl/tp présentes ou non)."""
    try:
        import os
        import ccxt
        import utils

        # 1) Si aucun DF fourni, on le prépare (OHLCV + BB requis par utils.fetch_and_prepare_df)
        if df is None:
            timeframe = os.getenv("TIMEFRAME", "1h")
            try:
                ex = ccxt.bitget({"enableRateLimit": True, "options": {"defaultType": "swap"}})
                if os.getenv("BITGET_TESTNET", "true").lower() in ("1", "true", "yes"):
                    try:
                        ex.set_sandbox_mode(True)
                    except Exception:
                        pass
                df = utils.fetch_and_prepare_df(ex, symbol, timeframe)
            except Exception:
                df = None
            if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                return None

        # 2) Vérifs colonnes nécessaires BB
        bb_cols = ['bb20_up', 'bb20_mid', 'bb20_lo', 'bb80_up', 'bb80_mid', 'bb80_lo']
        if not all(c in df.columns for c in bb_cols):
            # On ne tente pas de recalcul ici: on renvoie None proprement
            return None

        # 3) S'assurer d'un DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df = df.copy()
                df.index = pd.to_datetime(df.index, utc=True)
            except Exception:
                return None

        # 4) Sous-DF pour mplfinance (OHLCV)
        base_cols = ['open', 'high', 'low', 'close']
        if not all(c in df.columns for c in base_cols):
            return None
        df_plot = df[base_cols].copy()
        # Volume facultatif → colonne neutre si absente
        if 'volume' in df.columns:
            df_plot['volume'] = df['volume']
        else:
            df_plot['volume'] = 0.0
        df_plot = df_plot.tail(20)
        df_plot.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        # 5) Thème sombre & couleurs
        bg_fig, bg_ax, grid_col = '#121417', '#0f1215', '#1f2937'
        mc = mpf.make_marketcolors(up='#22c55e', down='#ef4444', edge='inherit', wick='inherit', volume='in')
        style = mpf.make_mpf_style(
            base_mpf_style='nightclouds',
            marketcolors=mc, gridstyle='-', gridcolor=grid_col,
            facecolor=bg_ax, figcolor=bg_fig, edgecolor=bg_ax
        )
        col_turq, col_blue = '#2dd4bf', '#60a5fa'

        # 6) Séries supplémentaires (BB20/BB80)
        seg = slice(-len(df_plot), None)
        plots = [
            mpf.make_addplot(df['bb20_up'].iloc[seg],  color=col_turq, linestyle='-',  width=1.2),
            mpf.make_addplot(df['bb20_lo'].iloc[seg],  color=col_turq, linestyle='-',  width=1.2),
            mpf.make_addplot(df['bb20_mid'].iloc[seg], color=col_turq, linestyle='--', width=1.2),
            mpf.make_addplot(df['bb80_up'].iloc[seg],  color=col_blue, linestyle='-',  width=1.6),
            mpf.make_addplot(df['bb80_lo'].iloc[seg],  color=col_blue, linestyle='-',  width=1.6),
            mpf.make_addplot(df['bb80_mid'].iloc[seg], color=col_blue, linestyle='--', width=1.2),
        ]

        # 7) Hlines optionnelles (entry/sl/tp si dispo)
        def _fget(k):
            try:
                v = signal.get(k, None)
                return float(v) if v is not None else None
            except Exception:
                return None

        h_vals = [('Entrée', _fget('entry'), '#60a5fa'),
                  ('SL',     _fget('sl'),    '#ef4444'),
                  ('TP',     _fget('tp'),    '#22c55e')]
        hlines_prices = [v for _, v, _ in h_vals if v is not None]
        hlines_colors = [c for _, v, c in h_vals if v is not None]
        hlines_widths = [1.2] * len(hlines_prices)
        hlines_cfg = dict(hlines=hlines_prices, colors=hlines_colors, linewidths=hlines_widths, alpha=0.95) if hlines_prices else None

        # 8) Plot
        regime = (signal.get('regime') or '').upper()
        fig, axes = mpf.plot(
            df_plot, type='candle', style=style,
            title=f"Setup de Trade: {symbol} ({regime})",
            ylabel='Prix (USDT)',
            addplot=plots,
            hlines=hlines_cfg,
            returnfig=True,
            figsize=(12, 7)
        )

        # 9) Labels des hlines (si présentes)
        if hlines_prices:
            ax = axes[0]
            last_x = ax.get_xlim()[1]
            for (label, val, color) in h_vals:
                if val is None:
                    continue
                ax.text(
                    last_x, val, f"  {label}",
                    va='center', ha='left', color=color, fontsize=9,
                    bbox=dict(facecolor='#0b0f14', alpha=0.7, edgecolor='none', pad=1.5)
                )

        # 10) Buffer PNG
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

