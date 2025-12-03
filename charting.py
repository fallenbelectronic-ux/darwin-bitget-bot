# Charting.py
import io
import pandas as pd
import mplfinance as mpf
from typing import Dict, Any, Optional

def generate_trade_chart(symbol: str, df: pd.DataFrame, signal: Dict[str, Any]) -> Optional[io.BytesIO]:
    """Génère une image PNG (fond sombre) pour un trade, avec:
       - BB20 turquoise (lignes pleines) + MM20 turquoise pointillé
       - BB80 bleu (plus épais)      + MM80 bleu pointillé
       - Lignes Entrée / SL / TP
       - (AJOUT) Bloc vert/rouge entre Entrée–TP / Entrée–SL
       - (AJOUT) Marqueurs verticaux + flèches : Contact / Réaction / Entrée
       Retourne un io.BytesIO prêt pour Telegram."""
    try:
        import matplotlib.pyplot as plt
        from matplotlib import dates as mdates
        from matplotlib.patches import Rectangle

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
        if df_plot.empty:
            return None
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

        # Lignes horizontales Entrée / SL / TP (base d'origine)
        try:
            entry_price = float(signal['entry'])
            sl_price    = float(signal['sl'])
            tp_price    = float(signal['tp'])
            h_prices = [entry_price, sl_price, tp_price]
        except Exception:
            # Si un des prix est manquant, on ne casse pas le graphe
            entry_price = sl_price = tp_price = None
            h_prices = []
        h_colors = ['#60a5fa', '#ef4444', '#22c55e']  # bleu, rouge, vert
        h_widths = [1.2, 1.2, 1.2]

        fig, axes = mpf.plot(
            df_plot,
            type='candle',
            style=style,
            title=f"Setup de Trade: {symbol} ({signal.get('regime', '')})",
            ylabel='Prix (USDT)',
            addplot=plots,
            hlines=dict(hlines=h_prices, colors=h_colors[:len(h_prices)], linewidths=h_widths[:len(h_prices)], alpha=0.95) if h_prices else None,
            returnfig=True,
            figsize=(12, 7)
        )

        # Récupération de l'axe principal
        try:
            ax = axes[0]
        except TypeError:
            ax = axes

        # -------------------------
        # Bloc position (profit / perte)
        # -------------------------
        if entry_price is not None and sl_price is not None and tp_price is not None:
            # X : par défaut toute la fenêtre
            x_left, x_right = ax.get_xlim()
            x0 = x_left
            x1 = x_right

            # Si on a un index de bougie d'entrée dans le signal ET qu'elle est dans les 20 dernières,
            # on fait commencer le bloc sur cette bougie.
            def _resolve_entry_pos() -> Optional[int]:
                keys = ['entry_index', 'entry_idx', 'entry_bar']
                for k in keys:
                    if k in signal and signal[k] is not None:
                        try:
                            idx_full = int(signal[k])
                        except Exception:
                            continue
                        if idx_full < 0 or idx_full >= len(df):
                            continue
                        ts_full = df.index[idx_full]
                        try:
                            loc = df_plot.index.get_loc(ts_full)
                            if isinstance(loc, slice):
                                loc = loc.start
                            if isinstance(loc, (list, tuple)):
                                loc = loc[0]
                            return int(loc)
                        except Exception:
                            continue
                # Sinon, on tente avec un timestamp
                ts_keys = ['entry_ts', 'entry_time', 'entry_timestamp']
                for k in ts_keys:
                    if k in signal and signal[k] is not None:
                        raw = signal[k]
                        try:
                            if isinstance(raw, (int, float)):
                                if raw > 1e12:
                                    ts = pd.to_datetime(int(raw), unit='ms', utc=True)
                                else:
                                    ts = pd.to_datetime(int(raw), unit='s', utc=True)
                            else:
                                ts = pd.to_datetime(raw, utc=True)
                        except Exception:
                            continue
                        try:
                            if ts < df_plot.index[0] or ts > df_plot.index[-1]:
                                continue
                            deltas = (df_plot.index - ts)
                            pos = int(deltas.abs().argmin())
                            return pos
                        except Exception:
                            continue
                return None

            try:
                entry_pos = _resolve_entry_pos()
            except Exception:
                entry_pos = None

            if entry_pos is not None and 0 <= entry_pos < len(df_plot):
                entry_dt = df_plot.index[entry_pos]
                x0 = mdates.date2num(entry_dt.to_pydatetime())

            width = max(x1 - x0, 0.0)

            def _add_box(y1: float, y2: float, color: str, alpha: float) -> None:
                if y1 is None or y2 is None or width <= 0:
                    return
                y_bottom = min(y1, y2)
                height = abs(y2 - y1)
                if height <= 0:
                    return
                rect = Rectangle(
                    (x0, y_bottom),
                    width,
                    height,
                    facecolor=color,
                    edgecolor=color,
                    alpha=alpha,
                    linewidth=1.0,
                    zorder=1
                )
                ax.add_patch(rect)

            # LONG ou SHORT
            if tp_price > entry_price:
                # LONG
                _add_box(entry_price, tp_price, '#22c55e', 0.16)   # zone profit
                _add_box(sl_price, entry_price, '#ef4444', 0.18)   # zone perte
            else:
                # SHORT
                _add_box(tp_price, entry_price, '#22c55e', 0.16)   # zone profit
                _add_box(entry_price, sl_price, '#ef4444', 0.18)   # zone perte

        # -------------------------
        # Marqueurs Contact / Réaction / Entrée
        # -------------------------
        y_min, y_max = ax.get_ylim()
        y_range = max(y_max - y_min, 1e-9)

        def _resolve_bar_position(base_key: str) -> Optional[int]:
            # Essai via index
            idx_keys = [f"{base_key}_index", f"{base_key}_idx", f"{base_key}_bar"]
            for k in idx_keys:
                if k in signal and signal[k] is not None:
                    try:
                        idx_full = int(signal[k])
                    except Exception:
                        continue
                    if idx_full < 0 or idx_full >= len(df):
                        continue
                    ts_full = df.index[idx_full]
                    try:
                        loc = df_plot.index.get_loc(ts_full)
                        if isinstance(loc, slice):
                            loc = loc.start
                        if isinstance(loc, (list, tuple)):
                            loc = loc[0]
                        return int(loc)
                    except Exception:
                        continue

            # Essai via timestamp
            ts_keys = [f"{base_key}_ts", f"{base_key}_time", f"{base_key}_timestamp"]
            for k in ts_keys:
                if k in signal and signal[k] is not None:
                    raw = signal[k]
                    try:
                        if isinstance(raw, (int, float)):
                            if raw > 1e12:
                                ts = pd.to_datetime(int(raw), unit='ms', utc=True)
                            else:
                                ts = pd.to_datetime(int(raw), unit='s', utc=True)
                        else:
                            ts = pd.to_datetime(raw, utc=True)
                    except Exception:
                        continue
                    try:
                        if ts < df_plot.index[0] or ts > df_plot.index[-1]:
                            continue
                        deltas = (df_plot.index - ts)
                        pos = int(deltas.abs().argmin())
                        return pos
                    except Exception:
                        continue

            return None

        contact_pos  = _resolve_bar_position('contact')
        reaction_pos = _resolve_bar_position('reaction')
        entry_pos_m  = _resolve_bar_position('entry')

        markers = [
            ("Contact",  contact_pos,  '#f97316'),
            ("Réaction", reaction_pos, '#eab308'),
            ("Entrée",   entry_pos_m,  '#3b82f6'),
        ]

        label_idx = 0
        for name, pos, color in markers:
            if pos is None or pos < 0 or pos >= len(df_plot):
                continue

            x_dt = df_plot.index[pos]

            ax.axvline(x_dt, color=color, linestyle='--', linewidth=1.1, alpha=0.95)

            try:
                row = df_plot.iloc[pos]
                y_candle = float(row['High'])
            except Exception:
                y_candle = y_max - 0.2 * y_range

            y_text = y_max - 0.04 * y_range * (label_idx + 1)
            ax.annotate(
                name,
                xy=(x_dt, y_candle),
                xytext=(x_dt, y_text),
                color=color,
                fontsize=9,
                ha='center',
                va='top',
                arrowprops=dict(
                    arrowstyle='->',
                    color=color,
                    linewidth=1.0,
                    shrinkA=0,
                    shrinkB=2
                ),
                bbox=dict(facecolor='#020617', edgecolor='none', alpha=0.7, pad=1.5)
            )
            label_idx += 1

        # Annotations texte à droite (version d’origine, on garde)
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

        try:
            plt.close(fig)
        except Exception:
            fig.clf()

        return buf

    except Exception as e:
        print(f"Erreur de génération de graphique: {e}")
        return None
