"""Starter churn model training utilities."""

from __future__ import annotations

import pandas as pd


def label_churn(features: pd.DataFrame, recency_threshold_days: int = 90) -> pd.DataFrame:
    """Create a simple churn label from recency days."""
    out = features.copy()
    if "recency_days" not in out.columns:
        raise ValueError("features must include recency_days")
    out["is_churn"] = (out["recency_days"] >= recency_threshold_days).astype(int)
    return out
