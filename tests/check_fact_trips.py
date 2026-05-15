import pandas as pd
import os
import glob
import re

STAR_SCHEMA_DIR = 'processed_data/gold/star_schema'

fact      = pd.read_parquet(os.path.join(STAR_SCHEMA_DIR, 'fact_trips.parquet'))
dim_date  = pd.read_parquet(os.path.join(STAR_SCHEMA_DIR, 'dim_date.parquet'))
dim_time  = pd.read_parquet(os.path.join(STAR_SCHEMA_DIR, 'dim_time.parquet'))
dim_vendor       = pd.read_parquet(os.path.join(STAR_SCHEMA_DIR, 'dim_vendor.parquet'))
dim_payment_type = pd.read_parquet(os.path.join(STAR_SCHEMA_DIR, 'dim_payment_type.parquet'))
dim_rate_code    = pd.read_parquet(os.path.join(STAR_SCHEMA_DIR, 'dim_rate_code.parquet'))

SEP = '=' * 60

# ── 1. ภาพรวม ──────────────────────────────────────────────
print(SEP)
print('1. OVERVIEW')
print(SEP)
print(f'Total trips      : {len(fact):,}')
print(f'Columns          : {list(fact.columns)}')
print(f'date_key range   : {fact["date_key"].min()} to {fact["date_key"].max()}')
print()

# ── 2. ตรวจ missing / null ──────────────────────────────────
print(SEP)
print('2. NULL CHECK')
print(SEP)
nulls = fact.isnull().sum()
nulls = nulls[nulls > 0]
if nulls.empty:
    print('No null values found.')
else:
    print(nulls)
print()

# ── 3. FK integrity ─────────────────────────────────────────
print(SEP)
print('3. FK INTEGRITY')
print(SEP)

checks = {
    'date_key':     (fact['date_key'],     dim_date['date_key']),
    'time_key':     (fact['time_key'],     dim_time['time_key']),
    'vendor_key':   (fact['vendor_key'],   dim_vendor['vendor_key']),
    'payment_key':  (fact['payment_key'],  dim_payment_type['payment_key']),
    'rate_code_key':(fact['rate_code_key'],dim_rate_code['rate_code_key']),
}
for fk, (fact_col, dim_col) in checks.items():
    orphans = ~fact_col.isin(dim_col)
    status = 'OK' if orphans.sum() == 0 else f'WARN: {orphans.sum():,} orphan rows'
    print(f'  {fk:<16}: {status}')
print()

# ── 4. measure sanity ───────────────────────────────────────
print(SEP)
print('4. MEASURE SANITY')
print(SEP)
neg_fare     = (fact['fare_amount'] < 0).sum()
neg_dist     = (fact['trip_distance'] < 0).sum()
neg_total    = (fact['total_amount'] < 0).sum()
neg_duration = (fact['trip_duration_minutes'] < 0).sum()
zero_dist    = (fact['trip_distance'] == 0).sum()

print(f'  fare_amount < 0        : {neg_fare:,}')
print(f'  trip_distance < 0      : {neg_dist:,}')
print(f'  trip_distance = 0      : {zero_dist:,}')
print(f'  total_amount < 0       : {neg_total:,}')
print(f'  trip_duration_min < 0  : {neg_duration:,}')
print()

# ── 5. year check ──────────────────────────────────────────
print(SEP)
print('5. YEAR CHECK (vs source filenames)')
print(SEP)

# ดึงปีที่ถูกต้องจากชื่อไฟล์ silver
silver_dir = 'processed_data/silver'
silver_files = glob.glob(os.path.join(silver_dir, '*.parquet'))
valid_years = set()
for f in silver_files:
    m = re.search(r'(\d{4})-\d{2}', os.path.basename(f))
    if m:
        valid_years.add(int(m.group(1)))

print(f'  Source files cover years : {sorted(valid_years)}')
print()

fact['year'] = fact['date_key'] // 10000
year_counts = fact['year'].value_counts().sort_index()

for year, count in year_counts.items():
    tag = 'OK' if year in valid_years else 'WARN: not in source files'
    print(f'  {year}: {count:,} trips  [{tag}]')

invalid_mask = ~fact['year'].isin(valid_years)
invalid_count = invalid_mask.sum()
print()
if invalid_count > 0:
    print(f'  Total invalid-year trips : {invalid_count:,}')
    print()
    print('  Sample records with unexpected year:')
    sample = (
        fact[invalid_mask][['trip_id', 'date_key', 'fare_amount', 'trip_distance']]
        .head(10)
    )
    print(sample.to_string(index=False))
else:
    print('  All trips match source file years.')
fact.drop(columns=['year'], inplace=True)
print()

# ── 6. สถิติสรุป ────────────────────────────────────────────
print(SEP)
print('6. SUMMARY STATS')
print(SEP)
for col in ['fare_amount', 'trip_distance', 'passenger_count', 'trip_duration_minutes']:
    s = fact[col]
    print(f'  {col}')
    print(f'    mean={s.mean():.2f}  std={s.std():.2f}  min={s.min():.2f}  max={s.max():.2f}')
print()

# ── 6. top 5 วันที่มี trip เยอะสุด (join dim_date) ─────────
print(SEP)
print('7. TOP 5 BUSIEST DATES')
print(SEP)
top_dates = (
    fact.groupby('date_key')
    .agg(trips=('trip_id', 'count'), revenue=('total_amount', 'sum'))
    .merge(dim_date[['date_key', 'full_date', 'day_name']], on='date_key')
    .sort_values('trips', ascending=False)
    .head(5)
    [['full_date', 'day_name', 'trips', 'revenue']]
)
top_dates['revenue'] = top_dates['revenue'].round(2)
print(top_dates.to_string(index=False))
print()

# ── 7. trips แต่ละ vendor ───────────────────────────────────
print(SEP)
print('8. TRIPS BY VENDOR')
print(SEP)
by_vendor = (
    fact.groupby('vendor_key')
    .agg(trips=('trip_id', 'count'), avg_fare=('fare_amount', 'mean'))
    .merge(dim_vendor[['vendor_key', 'vendor_name']], on='vendor_key')
    [['vendor_name', 'trips', 'avg_fare']]
)
by_vendor['avg_fare'] = by_vendor['avg_fare'].round(2)
print(by_vendor.to_string(index=False))
print()

# ── 8. trips แต่ละ payment type ────────────────────────────
print(SEP)
print('9. TRIPS BY PAYMENT TYPE')
print(SEP)
by_payment = (
    fact.groupby('payment_key')
    .agg(trips=('trip_id', 'count'))
    .merge(dim_payment_type[['payment_key', 'payment_description']], on='payment_key')
    [['payment_description', 'trips']]
    .sort_values('trips', ascending=False)
)
print(by_payment.to_string(index=False))
