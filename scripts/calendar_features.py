"""Populate the calendar_features table with date metadata.

This script generates calendar feature data for a range of dates, including:
- Day of week, week of year, month, quarter
- First/last weekend of month flags
- Holiday indicators (US holidays)
- Days until/since next holiday

Usage:
    python scripts/calendar_features.py --start 2024-01-01 --end 2026-12-31
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

from db_schema import get_connection


# US Federal Holidays (fixed dates - approximate for floating holidays)
US_HOLIDAYS = {
    # 2024
    '2024-01-01': "New Year's Day",
    '2024-01-15': "Martin Luther King Jr. Day",
    '2024-02-19': "Presidents' Day",
    '2024-05-27': "Memorial Day",
    '2024-06-19': "Juneteenth",
    '2024-07-04': "Independence Day",
    '2024-09-02': "Labor Day",
    '2024-10-14': "Columbus Day",
    '2024-11-11': "Veterans Day",
    '2024-11-28': "Thanksgiving",
    '2024-12-25': "Christmas Day",
    
    # 2025
    '2025-01-01': "New Year's Day",
    '2025-01-20': "Martin Luther King Jr. Day",
    '2025-02-17': "Presidents' Day",
    '2025-05-26': "Memorial Day",
    '2025-06-19': "Juneteenth",
    '2025-07-04': "Independence Day",
    '2025-09-01': "Labor Day",
    '2025-10-13': "Columbus Day",
    '2025-11-11': "Veterans Day",
    '2025-11-27': "Thanksgiving",
    '2025-12-25': "Christmas Day",
    
    # 2026
    '2026-01-01': "New Year's Day",
    '2026-01-19': "Martin Luther King Jr. Day",
    '2026-02-16': "Presidents' Day",
    '2026-05-25': "Memorial Day",
    '2026-06-19': "Juneteenth",
    '2026-07-04': "Independence Day (Observed)",
    '2026-09-07': "Labor Day",
    '2026-10-12': "Columbus Day",
    '2026-11-11': "Veterans Day",
    '2026-11-26': "Thanksgiving",
    '2026-12-25': "Christmas Day",
}


def is_first_weekend_of_month(date: datetime) -> bool:
    """Check if date is the first Saturday or Sunday of the month."""
    if date.weekday() not in (5, 6):  # Saturday=5, Sunday=6
        return False
    # First weekend means day <= 7
    return date.day <= 7


def get_first_weekend_of_month(date: datetime) -> datetime:
    """Get the first Saturday of the given date's month."""
    first_of_month = date.replace(day=1)
    # Find first Saturday (weekday 5)
    days_until_saturday = (5 - first_of_month.weekday()) % 7
    return first_of_month + timedelta(days=days_until_saturday)


def days_until_first_weekend(date: datetime) -> int:
    """Calculate days from this date until the first weekend of the month.
    
    Returns:
        - Positive number: days until first weekend
        - 0: date IS on first weekend
        - Negative number: first weekend already passed this month
    """
    first_saturday = get_first_weekend_of_month(date)
    return (first_saturday - date).days


def covers_first_weekend(date: datetime, coverage_days: int = 4) -> bool:
    """Check if a delivery on this date will have stock on shelves for first weekend.
    
    Args:
        date: The delivery date
        coverage_days: How many days the stock typically lasts (default 4 for 2x/week delivery)
    
    Returns:
        True if the delivery's stock will cover the first weekend of the month.
    """
    first_saturday = get_first_weekend_of_month(date)
    first_sunday = first_saturday + timedelta(days=1)
    
    # Stock covers from delivery date through delivery_date + coverage_days
    coverage_end = date + timedelta(days=coverage_days)
    
    # Check if either Saturday or Sunday of first weekend falls within coverage
    return date <= first_saturday <= coverage_end or date <= first_sunday <= coverage_end


def is_last_weekend_of_month(date: datetime) -> bool:
    """Check if date is the last Saturday or Sunday of the month."""
    if date.weekday() not in (5, 6):
        return False
    
    # Check if next week is in a different month
    next_week = date + timedelta(days=7)
    return next_week.month != date.month


def get_holiday_distances(date: datetime, holidays: dict) -> tuple:
    """Calculate days until next holiday and days since last holiday.
    
    Returns:
        Tuple of (days_until_next, days_since_last)
    """
    date_str = date.strftime('%Y-%m-%d')
    holiday_dates = sorted([datetime.strptime(d, '%Y-%m-%d') for d in holidays.keys()])
    
    # Days until next holiday
    days_until = None
    for hd in holiday_dates:
        if hd >= date:
            days_until = (hd - date).days
            break
    
    # Days since last holiday
    days_since = None
    for hd in reversed(holiday_dates):
        if hd <= date:
            days_since = (date - hd).days
            break
    
    return (days_until, days_since)


def is_holiday_week(date: datetime, holidays: dict) -> bool:
    """Check if date is within a week containing a holiday."""
    # Check if any day in the same week (Mon-Sun) is a holiday
    start_of_week = date - timedelta(days=date.weekday())
    
    for i in range(7):
        check_date = start_of_week + timedelta(days=i)
        if check_date.strftime('%Y-%m-%d') in holidays:
            return True
    
    return False


def generate_calendar_features(start_date: datetime, end_date: datetime) -> list:
    """Generate calendar features for a date range.
    
    Returns:
        List of tuples ready for database insertion.
    """
    features = []
    current = start_date
    
    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')
        
        dow = current.weekday()  # 0=Mon, 6=Sun
        week = current.isocalendar()[1]
        month = current.month
        quarter = (month - 1) // 3 + 1
        year = current.year
        
        is_weekend = dow >= 5
        first_weekend = is_first_weekend_of_month(current)
        last_weekend = is_last_weekend_of_month(current)
        month_start = current.day <= 3
        month_end = current.day >= 28  # Simple approximation
        
        is_holiday = date_str in US_HOLIDAYS
        holiday_name = US_HOLIDAYS.get(date_str)
        holiday_week = is_holiday_week(current, US_HOLIDAYS)
        
        days_until, days_since = get_holiday_distances(current, US_HOLIDAYS)
        
        # New schedule-aware features
        days_to_first_wknd = days_until_first_weekend(current)
        covers_first_wknd = covers_first_weekend(current, coverage_days=4)
        
        features.append((
            date_str,
            dow,
            week,
            month,
            quarter,
            year,
            is_weekend,
            first_weekend,
            last_weekend,
            month_start,
            month_end,
            is_holiday,
            holiday_name,
            holiday_week,
            days_until,
            days_since,
            days_to_first_wknd,
            covers_first_wknd,
        ))
        
        current += timedelta(days=1)
    
    return features


def populate_calendar_table(conn, start_date: datetime, end_date: datetime) -> int:
    """Populate the calendar_features table.
    
    Returns:
        Number of rows inserted.
    """
    print(f"Generating calendar features from {start_date.date()} to {end_date.date()}...")
    
    features = generate_calendar_features(start_date, end_date)
    
    print(f"Inserting {len(features)} calendar rows...")
    
    # Use INSERT OR REPLACE pattern
    conn.executemany("""
        INSERT INTO calendar_features (
            date, day_of_week, week_of_year, month, quarter, year,
            is_weekend, is_first_weekend_of_month, is_last_weekend_of_month,
            is_month_start, is_month_end, is_holiday, holiday_name,
            is_holiday_week, days_until_next_holiday, days_since_last_holiday,
            days_until_first_weekend, covers_first_weekend
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (date) DO UPDATE SET
            is_holiday = EXCLUDED.is_holiday,
            holiday_name = EXCLUDED.holiday_name,
            is_holiday_week = EXCLUDED.is_holiday_week,
            days_until_next_holiday = EXCLUDED.days_until_next_holiday,
            days_since_last_holiday = EXCLUDED.days_since_last_holiday,
            days_until_first_weekend = EXCLUDED.days_until_first_weekend,
            covers_first_weekend = EXCLUDED.covers_first_weekend
    """, features)
    
    return len(features)


def print_calendar_summary(conn):
    """Print summary of calendar features table."""
    # Total rows
    total = conn.execute("SELECT COUNT(*) FROM calendar_features").fetchone()[0]
    
    # Date range
    date_range = conn.execute("""
        SELECT MIN(date), MAX(date) FROM calendar_features
    """).fetchone()
    
    # Holidays count
    holidays = conn.execute("""
        SELECT COUNT(*) FROM calendar_features WHERE is_holiday = TRUE
    """).fetchone()[0]
    
    # First weekends count
    first_weekends = conn.execute("""
        SELECT COUNT(*) FROM calendar_features WHERE is_first_weekend_of_month = TRUE
    """).fetchone()[0]
    
    print("\n📅 Calendar Features Summary")
    print("=" * 40)
    print(f"   Total dates: {total:,}")
    print(f"   Date range: {date_range[0]} to {date_range[1]}")
    print(f"   Holidays: {holidays}")
    print(f"   First weekends: {first_weekends}")
    
    # Sample holidays
    print("\n   Sample holidays:")
    holidays_sample = conn.execute("""
        SELECT date, holiday_name 
        FROM calendar_features 
        WHERE is_holiday = TRUE 
        ORDER BY date 
        LIMIT 10
    """).fetchall()
    
    for h in holidays_sample:
        print(f"      {h[0]}: {h[1]}")


def main():
    parser = argparse.ArgumentParser(description='Populate calendar features table')
    parser.add_argument('--db', default='data/analytics.duckdb', help='Path to database')
    parser.add_argument('--start', default='2024-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', default='2026-12-31', help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    # Resolve paths
    base_dir = Path(__file__).parent.parent
    db_path = base_dir / args.db
    
    start_date = datetime.strptime(args.start, '%Y-%m-%d')
    end_date = datetime.strptime(args.end, '%Y-%m-%d')
    
    print(f"📁 Database: {db_path}")
    
    conn = get_connection(db_path)
    
    rows = populate_calendar_table(conn, start_date, end_date)
    print(f"✅ Inserted {rows:,} calendar feature rows")
    
    print_calendar_summary(conn)
    
    conn.close()


if __name__ == '__main__':
    main()


