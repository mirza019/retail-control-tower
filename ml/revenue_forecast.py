"""Simple baseline revenue forecast utilities."""

from __future__ import annotations

import pandas as pd


def moving_average_forecast(series: pd.Series, window: int = 7) -> float:
    """Return next-value forecast using trailing moving average."""
    if len(series) == 0:
        raise ValueError("series must not be empty")
    w = min(window, len(series))
    return float(series.tail(w).mean())


def forecast_next_period(df: pd.DataFrame, value_col: str = "revenue", window: int = 7) -> dict:
    """Forecast a single next-period value from time series dataframe."""
    if value_col not in df.columns:
        raise ValueError(f"missing column: {value_col}")
    return {"forecast": moving_average_forecast(df[value_col], window=window)}
