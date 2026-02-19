"""Simple z-score anomaly detection for KPI time series."""

from __future__ import annotations

import pandas as pd


def detect_anomalies(df: pd.DataFrame, value_col: str = "revenue", z_threshold: float = 3.0) -> pd.DataFrame:
    """Flag outliers where absolute z-score exceeds threshold."""
    out = df.copy()
    if value_col not in out.columns:
        raise ValueError(f"missing column: {value_col}")

    std = out[value_col].std(ddof=0)
    if std == 0 or pd.isna(std):
        out["z_score"] = 0.0
        out["is_anomaly"] = False
        return out

    mean = out[value_col].mean()
    out["z_score"] = (out[value_col] - mean) / std
    out["is_anomaly"] = out["z_score"].abs() >= z_threshold
    return out
