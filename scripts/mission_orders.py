import pandas as pd

PROMO_ACCOUNT_RULES = [
    ("HARMON", {"Harmon's"}),
    ("SMITH", {"Kaysville", "Farmington"}),
]

def _stores_for_account(account):
    if pd.isna(account) or account == '':
        return None

    account_upper = str(account).upper()
    matched = set()
    for keyword, stores in PROMO_ACCOUNT_RULES:
        if keyword in account_upper:
            matched.update(stores)

    return matched or None





META_MAP = {
    'SAP': 'sap',
    'Product': 'product',
    'SKU': 'sku',
    'Count': 'case_count',
    'Tray': 'tray',
}

META_COLS = list(META_MAP.values())

STORE_NAME_MAP = {
    'Walmart': 'Wal-Mart',
    'Kaysville Smiths': 'Kaysville',
    "Harmon's": "Harmon's",
    'Farmington Smiths': 'Farmington',
    "Sam's": "Sam's",
    "Bowman's": "Bowman's",
}


def _repair_columns(df: pd.DataFrame) -> pd.DataFrame:
    clean_keys = []


    current_date = None

    for raw_store, raw_date in df.columns:
        store = raw_store.strip() if isinstance(raw_store, str) else raw_store
        store = META_MAP.get(store, store)

        if store in META_COLS:
            clean_keys.append((store, None))
            current_date = None
            continue

        if isinstance(raw_date, str):
            candidate = raw_date.strip()
            if candidate and not candidate.startswith('Unnamed') and candidate not in {'0', 'nan', 'NaN'}:
                parsed = pd.to_datetime(candidate, errors='coerce')
                if pd.notna(parsed):
                    current_date = parsed.normalize()
        elif pd.notna(raw_date):
            current_date = pd.to_datetime(raw_date).normalize()

        clean_keys.append((store, current_date))

    df.columns = pd.MultiIndex.from_tuples(clean_keys, names=['column', 'delivery_date'])
    return df


def _compute_cases(row: pd.Series) -> float:
    units = row.get('units', 0)
    tray = row.get('tray')

    if row.get('store') == 'Order':
        return units

    if pd.isna(tray) or tray == 0:
        return units

    return units / tray


def _infer_order_date(row: pd.Series) -> pd.Timestamp:
    delivery = row.get('delivery_date')
    if pd.isna(delivery):
        return pd.NaT

    weekday = delivery.dayofweek

    if weekday == 0:
        return delivery - pd.Timedelta(days=6)
    if weekday == 3:
        return delivery - pd.Timedelta(days=3)

    return pd.NaT


def load_order_history(path: str) -> pd.DataFrame:
    raw = pd.read_csv(path, header=[0, 1])
    raw = _repair_columns(raw)

    meta_keys = [(name, None) for name in META_COLS]


    tidy = (
        raw.set_index(meta_keys)
        .stack(['column', 'delivery_date'], future_stack=True)
        .reset_index(name='units')
    )

    rename_map = {(name, None): name for name in META_COLS}
    rename_map['column'] = 'store'
    tidy = tidy.rename(columns=rename_map)

    tidy['delivery_date'] = pd.to_datetime(tidy['delivery_date'], errors='coerce')
    tidy = tidy.dropna(subset=['delivery_date'])

    tidy['sap'] = pd.to_numeric(tidy['sap'], errors='coerce')
    tidy = tidy.dropna(subset=['sap'])
    tidy['sap'] = tidy['sap'].astype('int64')

    tidy['product'] = tidy['product'].astype(str).str.strip()
    tidy['sku'] = tidy['sku'].astype(str).str.strip()

    tidy['case_count'] = pd.to_numeric(tidy['case_count'], errors='coerce')
    tidy['tray'] = pd.to_numeric(tidy['tray'], errors='coerce')
    tidy['units'] = pd.to_numeric(tidy['units'], errors='coerce').fillna(0)

    tidy['cases'] = tidy.apply(_compute_cases, axis=1)
    tidy['order_date'] = tidy.apply(_infer_order_date, axis=1)
    tidy['lead_time_days'] = (tidy['delivery_date'] - tidy['order_date']).dt.days

    return tidy


def load_store_stock(path: str) -> pd.DataFrame:
    stock = pd.read_csv(path)
    stock = stock.dropna(axis=0, how='all').dropna(axis=1, how='all')

    columns = list(stock.columns)
    if len(columns) < 2:
        raise ValueError('Expected at least two columns (sap, product) in stock file.')

    stock = stock.rename(columns={columns[0]: 'sap', columns[1]: 'product'})
    stock = stock[stock['sap'].notna()]


    stock['sap'] = pd.to_numeric(stock['sap'], errors='coerce')
    stock = stock.dropna(subset=['sap'])
    stock['sap'] = stock['sap'].astype('int64')
    stock['product'] = stock['product'].astype(str).str.strip()

    store_cols = [col for col in stock.columns if col not in {'sap', 'product'}]


    cleaned = {}
    for col in store_cols:
        name = STORE_NAME_MAP.get(col.strip(), col.strip())
        cleaned[col] = name
    stock = stock.rename(columns=cleaned)

    store_cols = [col for col in stock.columns if col not in {'sap', 'product'}]


    stock = stock.melt(id_vars=['sap', 'product'], value_vars=store_cols, var_name='store', value_name='active')

    stock['store'] = stock['store'].astype(str).str.strip()
    stock['active'] = pd.to_numeric(stock['active'], errors='coerce').fillna(0).astype(int)

    return stock


def annotate_with_stock(orders: pd.DataFrame, stock: pd.DataFrame) -> pd.DataFrame:
    merged = orders.merge(stock, on=['sap', 'product', 'store'], how='left')
    merged.loc[merged['store'] == 'Order', 'active'] = pd.NA
    return merged

def annotate_with_promotions(orders: pd.DataFrame, promotions: pd.DataFrame) -> pd.DataFrame:
    if promotions is None or promotions.empty:
        enriched = orders.copy()
        enriched['promo_active'] = False
        enriched['promo_account'] = pd.NA
        enriched['promo_price_text'] = pd.NA
        enriched['promo_description'] = pd.NA
        return enriched

    promo_cols = ['sap_code', 'promo_start', 'promo_end', 'ad_date', 'price_text', 'description', 'account']


    promo_frame = promotions[promo_cols].copy()

    promo_frame['sap'] = pd.to_numeric(promo_frame['sap_code'], errors='coerce')
    promo_frame = promo_frame.dropna(subset=['sap'])
    promo_frame['sap'] = promo_frame['sap'].astype(int)

    merged = orders.merge(
        promo_frame.drop(columns=['sap_code']),
        on='sap',
        how='left',
    )

    active_mask = (
        merged['promo_start'].notna()
        & (merged['delivery_date'] >= merged['promo_start'])
        & (merged['delivery_date'] <= merged['promo_end'])
    )

    filtered = merged[active_mask | merged['promo_start'].isna()].copy()

    filtered['promo_active'] = active_mask.loc[filtered.index]


    filtered = filtered.rename(
        columns={
            'account': 'promo_account',
            'price_text': 'promo_price_text',
            'description': 'promo_description',
        }
    )

    filtered['promo_allowed_stores'] = filtered['promo_account'].apply(_stores_for_account)

    promo_rows = filtered['promo_account'].notna()
    applies_mask = filtered.apply(
        lambda row: isinstance(row['promo_allowed_stores'], set) and row['store'] in row['promo_allowed_stores'],
        axis=1,
    )
    applies_mask &= promo_rows
    not_applicable = promo_rows & ~applies_mask

    filtered.loc[applies_mask, 'promo_active'] = filtered.loc[applies_mask, 'promo_active'].fillna(False).astype(bool)
    filtered.loc[not_applicable, 'promo_active'] = False

    cleared_cols = ['promo_account', 'promo_price_text', 'promo_description', 'promo_start', 'promo_end', 'ad_date']
    for col in cleared_cols:
        filtered.loc[not_applicable, col] = pd.NA

    filtered['promo_active'] = filtered['promo_active'].fillna(False).astype(bool)
    filtered = filtered.drop(columns=['promo_allowed_stores'])

    return filtered
