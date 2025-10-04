"""Generate per-store forecast templates keyed off historical order dates."""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Optional

import pandas as pd

import mission_orders as mo

SCHEDULE_CONFIG = {
    'monday': {
        'stores': {"Sam's", 'Wal-Mart', 'Kaysville', "Harmon's", 'Farmington', "Bowman's"},
        'lead_days': 6,  # order placed previous Tuesday
    },
    'thursday': {
        'stores': {"Sam's", 'Wal-Mart', 'Kaysville', "Harmon's", 'Farmington'},
        'lead_days': 3,  # order placed previous Monday
    },
    'bowmans': {
        'stores': {"Bowman's"},
        'lead_days': 3,  # Tuesday order for Friday delivery (counted with Monday truck)
    },
}

STORE_ALLOWED_SAPS = {
    "Sam's": {11371, 19577, 38381, 39782},
}


def _allowed_sku(store: str, sap: int) -> bool:
    allowed = STORE_ALLOWED_SAPS.get(store)
    if allowed is None:
        return True
    return sap in allowed


def _floor_cases(value: float) -> int:
    if pd.isna(value):
        return 0
    return max(math.floor(value + 1e-9), 0)


def _select_reference_orders(
    orders: pd.DataFrame,
    stores: set[str],
    desired_order_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.Timestamp]:
    subset = orders[(orders['store'].isin(stores)) & (orders['order_date'] == desired_order_date)]
    if not subset.empty:
        return subset.copy(), desired_order_date

    prior_dates = (
        orders.loc[orders['store'].isin(stores), 'order_date']
        .dropna()
        .unique()
    )
    prior_dates = [pd.Timestamp(d) for d in prior_dates if pd.Timestamp(d) <= desired_order_date]
    if not prior_dates:
        raise ValueError(
            f'No historical orders found on or before {desired_order_date.date()} for stores {sorted(stores)}.'
        )

    fallback_date = max(prior_dates)
    subset = orders[(orders['store'].isin(stores)) & (orders['order_date'] == fallback_date)].copy()
    if subset.empty:
        raise ValueError(
            f'Unexpected empty result when using fallback order_date {fallback_date.date()}.'
        )

    return subset, fallback_date


def generate_forecast(
    orders_csv: Path,
    stock_csv: Path,
    forecast_date: pd.Timestamp,
    schedule: str = 'monday',
    history_order_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    schedule = schedule.lower()
    if schedule not in SCHEDULE_CONFIG:
        raise ValueError(f"Schedule '{schedule}' is not supported.")

    config = SCHEDULE_CONFIG[schedule]

    orders = mo.load_order_history(str(orders_csv))
    stock = mo.load_store_stock(str(stock_csv))
    orders = mo.annotate_with_stock(orders, stock)

    orders = orders[orders['active'] == 1].copy()

    if history_order_date is None:
        history_order_date = forecast_date - pd.Timedelta(days=config['lead_days'])

    reference_orders, resolved_order_date = _select_reference_orders(
        orders=orders,
        stores=config['stores'],
        desired_order_date=history_order_date,
    )

    reference_orders = reference_orders[
        reference_orders.apply(lambda row: _allowed_sku(row['store'], row['sap']), axis=1)
    ].copy()
    if reference_orders.empty:
        raise ValueError('All rows filtered out by SKU restrictions; review STORE_ALLOWED_SAPS.')

    reference_orders['forecast_date'] = forecast_date
    reference_orders['history_order_date'] = resolved_order_date

    reference_orders['recommended_cases'] = reference_orders['cases'].apply(_floor_cases)
    reference_orders['tray'] = pd.to_numeric(reference_orders['tray'], errors='coerce').fillna(0)
    reference_orders['recommended_units'] = reference_orders['recommended_cases'] * reference_orders['tray']

    reference_orders['last_order_cases'] = reference_orders['cases']
    reference_orders['last_order_units'] = reference_orders['cases'] * reference_orders['tray']

    reference_orders['actual_cases'] = pd.NA
    reference_orders['actual_units'] = pd.NA
    reference_orders['delta_cases'] = pd.NA
    reference_orders['delta_units'] = pd.NA
    reference_orders['notes'] = ''

    columns = [
        'forecast_date',
        'history_order_date',
        'store',
        'sap',
        'product',
        'tray',
        'last_order_cases',
        'last_order_units',
        'recommended_cases',
        'recommended_units',
        'actual_cases',
        'actual_units',
        'delta_cases',
        'delta_units',
        'notes',
    ]

    result = reference_orders[columns].sort_values(['store', 'sap']).reset_index(drop=True)
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Create forecast template for upcoming order.')
    parser.add_argument('--orders', default='data/daily/current_orders.csv', help='Orders CSV path')
    parser.add_argument('--stock', default='data/daily/storeStock.csv', help='Store stock CSV path')
    parser.add_argument('--forecast-date', required=True, help='Delivery date to forecast (YYYY-MM-DD)')
    parser.add_argument('--schedule', default='monday', choices=list(SCHEDULE_CONFIG.keys()), help='Delivery schedule to forecast')
    parser.add_argument('--history-order-date', help='Override historical order date (defaults based on schedule lead time)')
    parser.add_argument('--output-dir', default='forecasts/templates', help='Directory to store forecast CSV')
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    orders_path = Path(args.orders)
    stock_path = Path(args.stock)
    if not orders_path.exists():
        raise FileNotFoundError(f'Orders file not found: {orders_path}')
    if not stock_path.exists():
        raise FileNotFoundError(f'Stock file not found: {stock_path}')

    forecast_date = pd.Timestamp(args.forecast_date)
    history_order_date = pd.Timestamp(args.history_order_date) if args.history_order_date else None

    df = generate_forecast(
        orders_csv=orders_path,
        stock_csv=stock_path,
        forecast_date=forecast_date,
        schedule=args.schedule,
        history_order_date=history_order_date,
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f'forecast_{forecast_date.date()}_{args.schedule}.csv'
    df.to_csv(output_path, index=False)

    print(f'Forecast template written to {output_path}')
    print(df.head(10).to_string(index=False))


if __name__ == '__main__':
    main()
