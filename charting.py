# Charting.py
import io
import pandas as pd
import mplfinance as mpf
from typing import Dict, Any, Optional

def generate_trade_chart(symbol: str, df: pd.DataFrame, signal: Dict[str, Any]) -> Optional[io.BytesIO]:
    """Génère une image PNG (fond sombre) pour un trade, avec:
       - BB20 turquoise (lignes pleines) + MM20 turquoise pointillé
       - BB80 bleu (plus épais)      + MM80 bleu pointillé
       - Lignes Entrée / SL / TP (si dispo)
       - Bloc vert/rouge entre Entrée–TP / Entrée–SL (si dispo)
       - Marqueurs verticaux + flèches : Contact / Réaction / Entrée (si dans la fenêtre)
       Retourne un io.BytesIO prêt pour Telegram.
    """
    try:
        import matplotlib.pyplot as plt
        from matplotlib import dates as mdates
        from matplotlib.patches import Rectangle

        # --- 1) DF minimum valide ---
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            print("Erreur graphique: DF vide ou invalide.")
            return None

        # On essaie d'avoir un DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df = df.copy()
                df.index = pd.to_datetime(df.index, utc=True)
            except Exception:
                print("Erreur graphique: impossible de convertir l'index en DatetimeIndex.")
                return None

        # --- 2) Fenêtre compacte ---
        BARS = 20

        # OHLC obligatoires
        for col in ['open', 'high', 'low', 'close']:
            if col not in df.columns:
                print(f"Erreur graphique: colonne manquante dans DF: {col}")
                return None

        # Volume optionnel
        has_volume = 'volume' in df.columns

        if has_volume:
            df_plot = df[['open', 'high', 'low', 'close', 'volume']].tail(BARS).copy()
        else:
            df_plot = df[['open', 'high', 'low', 'close']].tail(BARS).copy()
            df_plot['volume'] = 0.0

        if df_plot.empty:
            print("Erreur graphique: df_plot vide après tail().")
            return None

        df_plot.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

        # --- 3) Bollinger si présentes ---
        bb_cols = ['bb20_up', 'bb20_mid', 'bb20_lo', 'bb80_up', 'bb80_mid', 'bb80_lo']
        have_bb = all(col in df.columns for col in bb_cols)

        plots = []
        if have_bb:
            seg = slice(-len(df_plot), None)
            df_bb = df[bb_cols].iloc[seg].copy()
            # réalignement défensif
            df_bb = df_bb.reindex(df_plot.index).ffill().bfill()

            col_turq = '#2dd4bf'   # BB20
            col_blue = '#60a5fa'   # BB80

            plots = [
                mpf.make_addplot(df_bb['bb20_up'],  color=col_turq, linestyle='-',  width=1.2),
                mpf.make_addplot(df_bb['bb20_lo'],  color=col_turq, linestyle='-',  width=1.2),
                mpf.make_addplot(df_bb['bb20_mid'], color=col_turq, linestyle='--', width=1.2),
                mpf.make_addplot(df_bb['bb80_up'],  color=col_blue, linestyle='-',  width=1.6),
                mpf.make_addplot(df_bb['bb80_lo'],  color=col_blue, linestyle='-',  width=1.6),
                mpf.make_addplot(df_bb['bb80_mid'], color=col_blue, linestyle='--', width=1.2),
            ]
        else:
            print("Attention: BB manquantes, on trace seulement les bougies.")

        # --- 4) Thème sombre ---
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

        # --- 5) Récup prix Entrée/SL/TP de façon robuste ---
        def _get_price(keys) -> Optional[float]:
            for k in keys:
                if k in signal and signal[k] is not None:
                    try:
                        return float(signal[k])
                    except Exception:
                        continue
            return None

        entry_price = _get_price(['entry', 'entry_price'])
        sl_price    = _get_price(['sl', 'sl_price', 'stop_loss'])
        tp_price    = _get_price(['tp', 'tp_price', 'take_profit'])

        h_prices = []
        h_colors = []
        h_widths = []
        if entry_price is not None:
            h_prices.append(entry_price)
            h_colors.append('#60a5fa')
            h_widths.append(1.2)
        if sl_price is not None:
            h_prices.append(sl_price)
            h_colors.append('#ef4444')
            h_widths.append(1.2)
        if tp_price is not None:
            h_prices.append(tp_price)
            h_colors.append('#22c55e')
            h_widths.append(1.2)

        hlines_cfg = None
        if h_prices:
            hlines_cfg = dict(
                hlines=h_prices,
                colors=h_colors,
                linewidths=h_widths,
                alpha=0.95
            )

        regime = str(signal.get('regime', '')).upper()

        fig, axes = mpf.plot(
            df_plot,
            type='candle',
            style=style,
            title=f"Setup de Trade: {symbol} ({regime})",
            ylabel='Prix (USDT)',
            addplot=plots if plots else None,
            hlines=hlines_cfg,
            returnfig=True,
            figsize=(12, 7)
        )

        try:
            ax = axes[0]
        except TypeError:
            ax = axes

        x_left, x_right = ax.get_xlim()

        # -------------------------
        # Bloc position (profit / perte)
        # -------------------------
        if entry_price is not None and sl_price is not None and tp_price is not None:
            # par défaut toute la fenêtre
            x0 = x_left
            x1 = x_right

            # tentative de faire démarrer sur la bougie d'entrée si elle est dans la fenêtre
            def _resolve_entry_pos() -> Optional[int]:
                idx_keys = ['entry_index', 'entry_idx', 'entry_bar']
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

            entry_pos = _resolve_entry_pos()
            if entry_pos is not None and 0 <= entry_pos < len(df_plot):
                entry_dt = df_plot.index[entry_pos]
                x0 = mdates.date2num(entry_dt.to_pydatetime())

            width = max(x1 - x0, 0.0)

            def _add_box(y1: float, y2: float, color: str, alpha: float) -> None:
                if width <= 0 or y1 is None or y2 is None:
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

            if tp_price > entry_price:
                # LONG
                _add_box(entry_price, tp_price, '#22c55e', 0.16)
                _add_box(sl_price,    entry_price, '#ef4444', 0.18)
            else:
                # SHORT
                _add_box(tp_price,    entry_price, '#22c55e', 0.16)
                _add_box(entry_price, sl_price,    '#ef4444', 0.18)

        # -------------------------
        # Marqueurs Contact / Réaction / Entrée
        # -------------------------
        y_min, y_max = ax.get_ylim()
        y_range = max(y_max - y_min, 1e-9)

        def _resolve_bar_position(base_key: str) -> Optional[int]:
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

        # -------------------------
        # Labels texte à droite pour Entrée / SL / TP (si disponibles)
        # -------------------------
        last_x = ax.get_xlim()[1]
        label_map = [
            (entry_price, 'Entrée', '#60a5fa'),
            (sl_price,    'SL',     '#ef4444'),
            (tp_price,    'TP',     '#22c55e'),
        ]
        for y, label, color in label_map:
            if y is None:
                continue
            ax.text(
                last_x, y, f"  {label}",
                va='center', ha='left', color=color, fontsize=9,
                bbox=dict(facecolor='#0b0f14', alpha=0.7, edgecolor='none', pad=1.5)
            )

        # -------------------------
        # Export PNG
        # -------------------------
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=110)
        buf.seek(0)

        plt.close(fig)
        return buf

    except Exception as e:
        print(f"Erreur de génération de graphique: {e}")
        return None
