import argparse
from pathlib import Path
import pandas as pd

STORE_ORDER = ["Sam's", 'Wal-Mart', 'Kaysville', "Harmon's", 'Farmington', "Bowman's"]
INFO_COLUMNS = ['SAP', 'Product', 'SKU', 'Count', 'Tray']

def build_order_form(forecast_path: Path, base_order_path: Path, output_path: Path) -> None:
    forecast = pd.read_csv(forecast_path)
    forecast['sap'] = forecast['sap'].astype(int)

    base = pd.read_csv(base_order_path, header=[0, 1])
    info_cols = base.loc[:, base.columns.get_level_values(0).isin(INFO_COLUMNS)].copy()
    info_cols.columns = INFO_COLUMNS
    info_cols['SAP'] = pd.to_numeric(info_cols['SAP'], errors='coerce').astype('Int64')

    pivot_units = forecast.pivot_table(index='sap', columns='store', values='recommended_units', fill_value=0)
    total_cases = forecast.groupby('sap')['recommended_cases'].sum()

    out = info_cols.copy()
    for store in STORE_ORDER:
        if store in pivot_units.columns:
            mapping = pivot_units[store]
        else:
            mapping = pd.Series(0, index=pivot_units.index)
        out[store] = info_cols['SAP'].map(mapping).fillna(0)
    out['Order'] = info_cols['SAP'].map(total_cases).fillna(0)

    date_str = pd.to_datetime(forecast['forecast_date'].iloc[0]).strftime('%m/%d/%Y')
    arrays = [
        INFO_COLUMNS + STORE_ORDER + ['Order'],
        [''] * len(INFO_COLUMNS) + [date_str] * len(STORE_ORDER) + ['']
    ]
    out.columns = pd.MultiIndex.from_arrays(arrays)
    out.to_csv(output_path, index=False)


def main():
    parser = argparse.ArgumentParser(description='Export forecast to order-form layout')
    parser.add_argument('--template', required=True, help='Path to forecast template CSV')
    parser.add_argument('--base', default='data/daily/current_orders.csv', help='Reference order-form CSV for info columns')
    parser.add_argument('--output', required=True, help='Output CSV path')
    args = parser.parse_args()

    build_order_form(Path(args.template), Path(args.base), Path(args.output))


if __name__ == '__main__':
    main()
