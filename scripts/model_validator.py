"""Model validator: compare model performance against naive baseline and gate deployment."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error


def validate_predictions(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_naive: np.ndarray,
    mae_threshold: float,
    rmse_threshold: float,
) -> dict:
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    naive_mae = mean_absolute_error(y_true, y_naive)
    naive_rmse = np.sqrt(mean_squared_error(y_true, y_naive))

    passed = (mae <= mae_threshold) and (rmse <= rmse_threshold)
    return {
        "mae": mae,
        "rmse": rmse,
        "naive_mae": naive_mae,
        "naive_rmse": naive_rmse,
        "passed": passed,
    }


def validate_from_csv(
    csv_path: Path,
    pred_col: str,
    true_col: str,
    naive_col: str,
    mae_threshold: float,
    rmse_threshold: float,
) -> dict:
    df = pd.read_csv(csv_path)
    y_true = df[true_col].values
    y_pred = df[pred_col].values
    y_naive = df[naive_col].values
    return validate_predictions(y_true, y_pred, y_naive, mae_threshold, rmse_threshold)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate model vs naive baseline")
    parser.add_argument("--csv", required=True, help="CSV with columns: true,pred,naive (or specified)")
    parser.add_argument("--trueCol", default="true")
    parser.add_argument("--predCol", default="pred")
    parser.add_argument("--naiveCol", default="naive")
    parser.add_argument("--maeThreshold", type=float, default=5.0)
    parser.add_argument("--rmseThreshold", type=float, default=8.0)
    args = parser.parse_args()

    metrics = validate_from_csv(
        Path(args.csv),
        pred_col=args.predCol,
        true_col=args.trueCol,
        naive_col=args.naiveCol,
        mae_threshold=args.maeThreshold,
        rmse_threshold=args.rmseThreshold,
    )
    print(metrics)
