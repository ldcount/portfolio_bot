"""
chart.py — in-memory portfolio charts using matplotlib.

Public API:
    build_portfolio_chart(entries)  -> io.BytesIO  (line chart, last 30 days)
    build_pie_chart(summary)        -> io.BytesIO  (pie chart, current allocation)
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

    # Sample every nth point (keep first and last to anchor the chart properly)
    if len(chronological) > 3:
        sampled = chronological[::2]
        # Always include the most recent point
        if chronological[-1] not in sampled:
            sampled.append(chronological[-1])
    else:
        sampled = chronological

    # Parse dates and currency values
    dates = [datetime.strptime(e["date"], "%d-%m-%Y") for e in sampled]
    values = [e[currency] for e in sampled]

    # --- Build the figure ---
    fig, ax = plt.subplots(figsize=(10, 5), dpi=120)

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

    # Annotate each plotted point with its value
    for d, v in zip(dates, values):
        ax.annotate(
            f"{symbol}{v:,.0f}".replace(",", " "),
            xy=(d, v),
            xytext=(0, 8),
            textcoords="offset points",
            ha="center",
            fontsize=7,
            color="#333333",
        )

    # X-axis: format as DD-Mon
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    fig.autofmt_xdate(rotation=30, ha="right")

    # Y-axis: compact currency formatting (e.g. $42 000 / ₽42 000)
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda val, _: f"{symbol}{val:,.0f}".replace(",", " "))
    )

    ax.set_title(f"Portfolio ({currency}) — last 30 days", fontsize=11, pad=10)
    ax.set_xlabel("Date", fontsize=9)
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
        f"Portfolio {currency} chart built with {len(sampled)} data points."
    )
    return buf


def build_pie_chart(summary: dict) -> io.BytesIO:
    """
    Build a pie chart showing current portfolio allocation by platform.

    Parameters
    ----------
    summary : dict  as returned by Aggregator.get_portfolio_summary()

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

    crypto_usd = summary.get("crypto_usd", 0.0)
    ibkr_usd = summary.get("ibkr_usd", 0.0)
    tbank_usd = summary.get("tbank_usd", 0.0)

    labels_raw = ["Crypto (Bybit+OKX)", "IBKR", "T-Bank"]
    values_raw = [crypto_usd, ibkr_usd, tbank_usd]
    colors_raw = ["#4A90D9", "#27AE60", "#E67E22"]

    # Drop zero-value segments
    data = [(l, v, c) for l, v, c in zip(labels_raw, values_raw, colors_raw) if v > 0]

    if not data:
        raise ValueError("All platform balances are zero — nothing to plot.")

    labels, values, colors = zip(*data)
    total = sum(values)

    def autopct(pct):
        amount = pct / 100 * total
        return f"{pct:.1f}%\n${amount:,.0f}".replace(",", " ")

    fig, ax = plt.subplots(figsize=(7, 5), dpi=120)
    wedges, texts, autotexts = ax.pie(
        values,
        labels=labels,
        colors=colors,
        autopct=autopct,
        startangle=140,
        pctdistance=0.75,
        wedgeprops=dict(linewidth=1.5, edgecolor="white"),
    )

    for t in texts:
        t.set_fontsize(10)
    for at in autotexts:
        at.set_fontsize(8)
        at.set_color("white")

    ax.set_title(
        f"Portfolio allocation  —  ${total:,.0f} total".replace(",", " "),
        fontsize=11,
        pad=14,
    )

    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)

    logger.info(f"Pie chart built: {dict(zip(labels, values))}")
    return buf
