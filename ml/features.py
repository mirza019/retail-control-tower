"""Feature engineering helpers for retail-control-tower ML tasks."""

from __future__ import annotations

import pandas as pd


def build_customer_features(orders: pd.DataFrame, order_items: pd.DataFrame) -> pd.DataFrame:
    """Build customer-level aggregate features from order and item tables."""
    merged = order_items.merge(orders[["order_id", "customer_id", "order_date"]], on="order_id", how="inner")
    merged["revenue"] = merged["price"].fillna(0) + merged["freight_value"].fillna(0)

    agg = (
        merged.groupby("customer_id", as_index=False)
        .agg(
            total_revenue=("revenue", "sum"),
            order_count=("order_id", "nunique"),
            last_order_date=("order_date", "max"),
        )
    )

    return agg
