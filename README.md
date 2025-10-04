# Mission Order Forecasting Toolkit

A lightweight toolkit for generating twice-weekly order forecasts, comparing them with actual shipments, and keeping historical inputs organized. The repo is structured so you can drop in your own order exports and promo sheets without committing personal data.

## Prerequisites

- Python 3.10+
- Install dependencies once:
  `ash
  pip install -r requirements.txt
  `

## Repository Layout

`
project-root/
├─ data/
│  ├─ daily/              # overwrite with the latest order & stock CSVs
│  ├─ historical/         # archive timestamped snapshots (optional)
│  └─ processed/          # reserved for tidy/parquet outputs
├─ promos/
│  ├─ raw/                # drop weekly promo PDFs here
│  ├─ parsed/             # future parsed promo CSVs
│  └─ dropbox/            # placeholder for automated downloads
├─ forecasts/
│  ├─ templates/          # generated forecast templates (per schedule/date)
│  ├─ actuals/            # templates filled with actual orders & deltas
│  └─ comparisons/        # optional summary reports
├─ scripts/               # forecasting + analysis tooling
├─ config/                # future store/schedule settings
├─ logs/                  # optional run logs
├─ requirements.txt
└─ README.md
`

## Daily Workflow

1. **Update inputs**
   - Copy your latest order export into data/daily/current_orders.csv.
   - Update data/daily/storeStock.csv if assortments change.
   - (Optional) archive the old files under data/historical/ for safekeeping.

2. **Add promo sheets**
   - Drop new PDFs into promos/raw/. The scripts pull every *.pdf in that folder.

3. **Generate forecasts**
   `ash
   python scripts/generate_forecast.py --forecast-date YYYY-MM-DD --schedule monday
   python scripts/generate_forecast.py --forecast-date YYYY-MM-DD --schedule thursday
   `
   Schedules:
   - monday includes Sam's, Wal-Mart, Kaysville, Harmon's, Farmington, and Bowman's.
   - 	hursday covers the Monday order pull for the Thursday drop.
   - (Optional) owmans schedule if you ever want it separate.

4. **Convert to order-form layout**
   `ash
   python scripts/export_forecast_to_order_form.py \
       --template forecasts/templates/forecast_YYYY-MM-DD_monday.csv \
       --output   forecasts/templates/orderform_YYYY-MM-DD_monday.csv
   `
   The output mirrors your original order spreadsheet (column headers, per-store unit columns, total cases).

5. **Paste into your spreadsheet & adjust**
   - Open the order-form CSV, copy all, and paste into your workbook.
   - Make manual tweaks as usual before submitting the order.

6. **Log actuals for model learning**
   - After the order is final, edit the template to add ctual_cases / ctual_units.
   - Save it under orecasts/actuals/ (e.g. orecast_YYYY-MM-DD_monday_with_actuals.csv).
   - These files are the basis for comparisons and future model training.

## Comparing Model vs Actual

Use your preferred workflow (spreadsheet or Python) to merge a template with actuals. Example snippet:
`python
import pandas as pd
forecast = pd.read_csv('forecasts/templates/forecast_2025-10-06_monday.csv')
actual   = pd.read_csv('data/daily/current_orders.csv', header=[0,1])
# merge, compute deltas, and save to forecasts/actuals/
`
The repo already contains orecasts/actuals/forecast_2025-10-06_monday_with_actuals.csv as a reference output.

## Baseline Modeling

scripts/baseline_model.py stitches together tidy orders, store stock, and promo features:
`ash
python scripts/baseline_model.py \
    --orders data/daily/current_orders.csv \
    --promos "promos/raw/*.pdf"
`
It reports MAE/RMSE against a naive “repeat last order” baseline and the feature importances driving the current model.

## Notes for Contributors

- Do **not** commit personal data; .gitignore already excludes the daily inputs and generated forecasts.
- Store/schedule settings will eventually live under config/. Until then, adjust constants directly in scripts/generate_forecast.py.
- Feel free to add helper scripts (e.g., automated promo downloads) under scripts/ or scheduled jobs logging to logs/.

## Next Ideas

- Automate promo PDF parsing into promos/parsed/.
- Add holiday/seasonality feature configs under config/.
- Build a small utility to ingest actuals and update the training set automatically.
- Extend the README with screenshots or spreadsheet macros if needed.

Happy forecasting!
