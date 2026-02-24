"""
chart.py — generates an in-memory portfolio USD line chart using matplotlib.

Usage:
    from app.chart import build_portfolio_chart
    buf = build_portfolio_chart(entries)   # entries from history_manager.get_history()
    await context.bot.send_photo(chat_id=chat_id, photo=buf)
"""

import io
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def build_portfolio_chart(entries: list[dict]) -> io.BytesIO:
    """
    Build a portfolio-USD line chart from history entries and return it
    as an in-memory PNG BytesIO buffer ready for Telegram send_photo().

    Parameters
    ----------
    entries : list of dicts with keys "date" (DD-MM-YYYY), "USD", "RUB"
              Expected newest-first (as returned by history_manager.get_history).

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

    # Entries arrive newest-first — reverse for chronological order on the x-axis
    chronological = list(reversed(entries))

    # Sample every nth point (keep first and last to anchor the chart properly)
    if len(chronological) > 3:
        sampled = chronological[::1]
        # Always include the most recent point
        if chronological[-1] not in sampled:
            sampled.append(chronological[-1])
    else:
        sampled = chronological

    # Parse dates and USD values
    dates = [datetime.strptime(e["date"], "%d-%m-%Y") for e in sampled]
    usd_values = [e["USD"] for e in sampled]

    # --- Build the figure ---
    fig, ax = plt.subplots(figsize=(9, 4), dpi=120)

    ax.plot(
        dates,
        usd_values,
        marker="o",
        markersize=5,
        linewidth=2,
        color="#4A90D9",
        markerfacecolor="#FFFFFF",
        markeredgecolor="#4A90D9",
        markeredgewidth=1.5,
    )

    # Annotate each plotted point with its value
    for d, v in zip(dates, usd_values):
        ax.annotate(
            f"${v:,.0f}".replace(",", " "),
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

    # Y-axis: compact dollar formatting (e.g. $42 000)
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda val, _: f"${val:,.0f}".replace(",", " "))
    )

    ax.set_title("Portfolio (USD) — last 30 days", fontsize=11, pad=10)
    ax.set_xlabel("Date", fontsize=9)
    ax.set_ylabel("USD", fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.spines[["top", "right"]].set_visible(False)

    fig.tight_layout()

    # Render to in-memory buffer
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)  # free memory
    buf.seek(0)

    logger.info(f"Portfolio chart built with {len(sampled)} data points.")
    return buf
