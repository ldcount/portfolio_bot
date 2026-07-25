"""
chart.py — in-memory portfolio charts using matplotlib.

Public API:
    build_portfolio_chart(entries)  -> io.BytesIO  (line chart, last 30 days)
    build_pie_chart(summary, grouping) -> io.BytesIO (donut allocation chart)
"""

import io
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def build_portfolio_chart(
    entries: list[dict], currency: str = "USD", line_color: str = "#4A90D9"
) -> io.BytesIO:
    """
    Build a portfolio line chart from history entries and return it
    as an in-memory PNG BytesIO buffer ready for Telegram send_photo().

    Parameters
    ----------
    entries : list of dicts with keys "date" (DD-MM-YYYY), "USD", "RUB"
              Expected newest-first (as returned by history_manager.get_history).
    currency : str
        Either "USD" or "RUB".
    line_color : str
        Hex color used for the chart line and marker edges.

    Returns
    -------
    io.BytesIO — PNG image buffer (position reset to 0).
    """
    # Lazy import so the rest of the bot still starts if matplotlib is unavailable
    try:
        import matplotlib

        matplotlib.use("Agg")  # non-interactive backend — no display needed
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError as exc:
        raise RuntimeError(
            "matplotlib is not installed. Run: pip install matplotlib"
        ) from exc

    if not entries:
        raise ValueError("No history entries to plot.")

    currency = currency.upper()
    if currency not in {"USD", "RUB"}:
        raise ValueError("Unsupported currency for chart. Use USD or RUB.")

    symbol = "$" if currency == "USD" else "₽"

    # Entries arrive newest-first — reverse for chronological order on the x-axis
    chronological = list(reversed(entries))

    # Parse dates and currency values
    dates = [datetime.strptime(e["date"], "%d-%m-%Y") for e in chronological]
    values = [e[currency] for e in chronological]

    # --- Build the figure ---
    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=150)

    ax.plot(
        dates,
        values,
        marker="o",
        markersize=5,
        linewidth=2,
        color=line_color,
        markerfacecolor="#FFFFFF",
        markeredgecolor=line_color,
        markeredgewidth=1.5,
    )
    ax.fill_between(dates, values, min(values), color=line_color, alpha=0.10)

    # Keep mobile labels readable: only latest, minimum, and maximum are annotated.
    important_indexes = {len(values) - 1, values.index(min(values)), values.index(max(values))}
    for index in sorted(important_indexes):
        d = dates[index]
        v = values[index]
        offset = -16 if v == max(values) and v != min(values) else 9
        ax.annotate(
            f"{symbol}{v:,.0f}".replace(",", " "),
            xy=(d, v),
            xytext=(0, offset),
            textcoords="offset points",
            ha="center",
            fontsize=8,
            fontweight="semibold",
            color="#333333",
        )

    # Limit date ticks so the exported image stays legible in Telegram.
    locator = mdates.AutoDateLocator(minticks=4, maxticks=7)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))

    # Y-axis: compact currency formatting (e.g. $42 000 / ₽42 000)
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda val, _: f"{symbol}{val:,.0f}".replace(",", " "))
    )

    period_change = values[-1] - values[0]
    period_pct = period_change / values[0] * 100 if values[0] else 0.0
    direction = "▲" if period_change > 0 else "▼" if period_change < 0 else "•"
    ax.set_title(
        f"Portfolio ({currency}) — {direction} {abs(period_pct):.1f}% over period",
        fontsize=11,
        pad=12,
    )
    ax.set_ylabel(currency, fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.spines[["top", "right"]].set_visible(False)

    fig.tight_layout()

    # Render to in-memory buffer
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)  # free memory
    buf.seek(0)

    logger.info(
        f"Portfolio {currency} chart built with {len(chronological)} data points."
    )
    return buf


def build_pie_chart(summary: dict, grouping: str = "platform") -> io.BytesIO:
    """
    Build a donut chart showing allocation by platform or asset class.

    Parameters
    ----------
    summary : dict  as returned by Aggregator.get_portfolio_summary()
    grouping : str
        Either "platform" or "asset_class".

    Returns
    -------
    io.BytesIO — PNG image buffer (position reset to 0).
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError(
            "matplotlib is not installed. Run: pip install matplotlib"
        ) from exc

    grouping = grouping.lower()
    if grouping == "platform":
        labels_raw = ["Bybit", "OKX", "KuCoin", "T-Bank", "IBKR"]
        values_raw = [
            summary.get("bybit_usd", 0.0),
            summary.get("okx_usd", 0.0),
            summary.get("kucoin_usd", 0.0),
            summary.get("tbank_usd", 0.0),
            summary.get("ibkr_usd", 0.0),
        ]
        colors_raw = ["#4A90E2", "#6C5CE7", "#00B894", "#F39C12", "#27AE60"]
        title = "Portfolio allocation by platform"
    elif grouping == "asset_class":
        labels_raw = ["Crypto", "Stocks", "T-Bank"]
        values_raw = [
            summary.get("crypto_usd", 0.0),
            summary.get("ibkr_usd", 0.0),
            summary.get("tbank_usd", 0.0),
        ]
        colors_raw = ["#4A90E2", "#27AE60", "#F39C12"]
        title = "Portfolio allocation by asset class"
    else:
        raise ValueError("Unsupported allocation grouping.")

    # Drop zero-value segments
    data = [(l, v, c) for l, v, c in zip(labels_raw, values_raw, colors_raw) if v > 0]

    if not data:
        raise ValueError("All platform balances are zero — nothing to plot.")

    labels, values, colors = zip(*data)
    total = sum(values)

    def autopct(pct):
        return f"{pct:.0f}%" if pct >= 7 else ""

    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=150)
    wedges, texts, autotexts = ax.pie(
        values,
        colors=colors,
        autopct=autopct,
        startangle=90,
        counterclock=False,
        pctdistance=0.78,
        wedgeprops=dict(width=0.42, linewidth=2, edgecolor="white"),
    )

    for at in autotexts:
        at.set_fontsize(9)
        at.set_fontweight("bold")
        at.set_color("white")

    ax.text(
        0,
        0.04,
        "TOTAL",
        ha="center",
        va="center",
        fontsize=9,
        color="#666666",
    )
    ax.text(
        0,
        -0.08,
        f"${total:,.0f}".replace(",", " "),
        ha="center",
        va="center",
        fontsize=13,
        fontweight="bold",
    )
    legend_labels = [
        f"{label}  ${value:,.0f}  ({value / total:.1%})".replace(",", " ")
        for label, value in zip(labels, values)
    ]
    ax.legend(
        wedges,
        legend_labels,
        loc="center left",
        bbox_to_anchor=(1.0, 0.5),
        frameon=False,
        fontsize=9,
    )
    ax.set_title(title, fontsize=11, pad=14)

    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)

    logger.info("Allocation chart built (%s): %s", grouping, dict(zip(labels, values)))
    return buf
