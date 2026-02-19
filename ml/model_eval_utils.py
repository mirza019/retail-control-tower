"""Reusable model evaluation helpers shared across notebooks and local code."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score


def eval_metrics(y_true: np.ndarray, y_score: np.ndarray, top_k_fraction: float) -> dict:
    base_rate = float(np.mean(y_true))
    if len(np.unique(y_true)) < 2:
        auc = np.nan
        pr_auc = np.nan
    else:
        auc = float(roc_auc_score(y_true, y_score))
        pr_auc = float(average_precision_score(y_true, y_score))

    k = max(1, int(len(y_score) * top_k_fraction))
    top_idx = np.argsort(-y_score)[:k]
    top_rate = float(np.mean(y_true[top_idx])) if len(top_idx) else 0.0

    return {
        "auc": auc,
        "pr_auc": pr_auc,
        "precision_at_k": top_rate,
        "lift_at_k": (float(top_rate / base_rate) if base_rate > 0 else np.nan),
        "positive_rate": base_rate,
        "sample_size": float(len(y_true)),
    }


def choose_threshold(y_true: np.ndarray, y_score: np.ndarray) -> float:
    best_t, best_f1 = 0.5, -1.0
    for t in np.linspace(0.05, 0.95, 19):
        y_pred = (y_score >= t).astype(int)
        tp = np.sum((y_pred == 1) & (y_true == 1))
        fp = np.sum((y_pred == 1) & (y_true == 0))
        fn = np.sum((y_pred == 0) & (y_true == 1))
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
        if f1 > best_f1:
            best_f1, best_t = f1, float(t)
    return best_t


def decile_diagnostics(df_split: pd.DataFrame, y_score: np.ndarray) -> pd.DataFrame:
    tmp = df_split[["snapshot_date", "target_1"]].copy().reset_index(drop=True)
    tmp["score"] = y_score
    tmp["decile"] = pd.qcut(tmp["score"], 10, labels=False, duplicates="drop")
    tmp["decile"] = tmp["decile"].astype(int) + 1
    out = (
        tmp.groupby("decile", as_index=False)
        .agg(
            min_score=("score", "min"),
            max_score=("score", "max"),
            avg_score=("score", "mean"),
            positive_rate=("target_1", "mean"),
            sample_size=("target_1", "size"),
        )
        .sort_values("decile", ascending=False)
    )
    return out


def model_score_for_selection(cv_folds: list, holdout_metrics: dict) -> float:
    cv_lift = np.nanmean([f["metrics"].get("lift_at_k", np.nan) for f in cv_folds])
    holdout_lift = holdout_metrics.get("lift_at_k", np.nan)
    holdout_auc = holdout_metrics.get("auc", np.nan)
    return float((0.5 * cv_lift) + (0.4 * holdout_lift) + (0.1 * holdout_auc))

