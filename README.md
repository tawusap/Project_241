# Yellow Taxi Data Pipeline

ETL pipeline สำหรับประมวลผลข้อมูล NYC Yellow Taxi โดยใช้ Apache Airflow รับข้อมูล CSV ดิบ ผ่านการ validate ทำความสะอาด และแปลงเป็น Star Schema เก็บในรูปแบบ Parquet

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Apache Airflow 3.2.0 |
| Executor | CeleryExecutor |
| Message Broker | Redis 7.2 |
| Metadata Database | PostgreSQL 16 |
| Data Processing | Python, Pandas |
| Storage Format | Apache Parquet (Snappy) |
| Containerization | Docker Compose |

---

## Project Structure

```
Project_2/
├── dags/
│   ├── dag_yellow_taxi_pipeline.py        # Airflow DAG — task definitions และ dependencies
│   └── pipeline/                          # Business logic แยกตาม concern
│       ├── config.py                      # Constants, paths, error maps
│       ├── validation.py                  # validate_and_split
│       ├── storage.py                     # save_clean_data, save_quarantine_data
│       ├── star_schema.py                 # create_star_schema (Gold layer)
│       └── reporting.py                   # generate_report
├── tests/
│   ├── helpers.py                         # Shared test utilities
│   ├── test_validation.py                 # Unit tests: get_csv_files, validate_and_split
│   ├── test_storage.py                    # Unit tests: save_clean_data, save_quarantine_data
│   ├── test_reporting.py                  # Unit tests: generate_report
│   └── check_fact_trips.py               # สคริปต์ตรวจสอบ fact table หลัง pipeline รัน
├── archive/
│   ├── raw/                               # ไฟล์ CSV ต้นทาง (2019-2020)
│   └── test_raw/                          # ไฟล์ CSV สำหรับ development
├── processed_data/
│   ├── silver/                            # ข้อมูล clean (Parquet)
│   ├── quarantine/                        # ข้อมูลที่ผิด validation แยกตามประเภท
│   │   ├── invalid_pickup_datetime/
│   │   ├── invalid_dropoff_datetime/
│   │   ├── invalid_fare/
│   │   ├── invalid_distance/
│   │   ├── invalid_passenger_count/
│   │   ├── invalid_time_sequence/
│   │   ├── invalid_year/
│   │   └── multiple_errors/
│   └── gold/
│       └── star_schema/                   # Dimension & Fact tables (Parquet)
├── config/
│   └── airflow.cfg
├── Dockerfile
├── docker-compose.yaml
└── requirements.txt
```

---

## Data Architecture

ใช้ **Medallion Architecture** แบ่งข้อมูลออกเป็น 3 ชั้น

```
Raw Layer          Silver Layer              Gold Layer
archive/raw/  -->  processed_data/silver/  -->  processed_data/gold/star_schema/
   (CSV)              (clean Parquet)              (Star Schema Parquet)
                           |
                           +--> processed_data/quarantine/
                                   (invalid records)
```

---

## Pipeline

DAG ID: `csv_to_parquet_converter` | Schedule: `@weekly` | Chunk size: 100,000 rows

### Task Flow

```
get_csv_files
      |
      v
validate_and_split
      |                    |
      v                    v
save_clean_data     save_quarantine_data
      |                    |
      v                    |
create_star_schema <-------+
      |
      v
generate_report
```

### Task Descriptions

**`get_csv_files`**
สแกนหาไฟล์ `yellow_tripdata_*.csv` ใน `archive/raw/` แล้วส่งรายชื่อไฟล์ผ่าน XCom ไปยัง task ถัดไป

**`validate_and_split`**
อ่าน CSV ทีละ 100,000 แถว (chunked streaming) เพื่อควบคุม memory usage แล้วตรวจสอบแต่ละแถวด้วย validation rules ทั้งหมด แถวที่ผ่านทุก rule จะถูกเขียนเป็น clean Parquet ส่วนแถวที่ fail จะถูกเขียนเป็น quarantine Parquet พร้อม column `error_reason`

**`save_clean_data`**
ย้าย clean Parquet จาก `/tmp/clean/` ไปเก็บที่ Silver layer (`processed_data/silver/`) ตั้งชื่อไฟล์ว่า `yellow_tripdata_YYYY-MM_silver.parquet`

**`save_quarantine_data`**
จัดกลุ่ม quarantine records ตาม `error_reason` แล้วบันทึกแยกเป็น subfolder ตามประเภทข้อผิดพลาด แต่ละ folder เก็บเฉพาะ column ที่เกี่ยวข้องกับ error นั้น ๆ

**`create_star_schema`**
แปลง Silver layer เป็น Star Schema โดยใช้วิธี **2-pass streaming** เพื่อป้องกัน OOM บนข้อมูลขนาดใหญ่
- **Pass 1** — อ่านแค่ `tpep_pickup_datetime` จากทุกไฟล์เพื่อสร้าง `dim_date`
- **Pass 2** — สร้าง `fact_trips` ทีละ batch ผ่าน `ParquetWriter` โดยไม่โหลดข้อมูลทั้งหมดเข้า RAM พร้อมกัน

**`generate_report`**
สรุปสถิติของ run ได้แก่ จำนวนแถว clean/quarantine, เปอร์เซ็นต์ data quality และ row count ของ Star Schema tables ถ้า clean records ต่ำกว่า **80%** จะ trigger alert

---

## Validation Rules

แถวจะถูกส่งไป quarantine ถ้าไม่ผ่าน rule ข้อใดข้อหนึ่ง

| Rule | เงื่อนไข | Error Folder |
|------|---------|-------------|
| Pickup datetime | ไม่เป็น null และ parse เป็น datetime ได้ | `invalid_pickup_datetime` |
| Dropoff datetime | ไม่เป็น null และ parse เป็น datetime ได้ | `invalid_dropoff_datetime` |
| Fare amount | `fare_amount > 0` | `invalid_fare` |
| Trip distance | `trip_distance > 0` | `invalid_distance` |
| Passenger count | เป็น NULL หรือ `> 0` (ค่า 0 ถือว่าผิด) | `invalid_passenger_count` |
| Time sequence | `dropoff_time > pickup_time` | `invalid_time_sequence` |
| Year consistency | ปีของ pickup ต้องตรงกับปีในชื่อไฟล์ เช่น ไฟล์ `2019-01` ต้องมีแค่ปี 2019 | `invalid_year` |
| Multiple failures | fail 2 rule ขึ้นไปในแถวเดียว | `multiple_errors` |

---

## Quarantine

แต่ละ folder ใน `quarantine/` เก็บเฉพาะ column ที่ใช้วิเคราะห์ error นั้น

| Folder | Columns Kept |
|--------|-------------|
| `invalid_pickup_datetime` | source_file, error_reason, tpep_pickup_datetime |
| `invalid_dropoff_datetime` | source_file, error_reason, tpep_dropoff_datetime |
| `invalid_fare` | source_file, error_reason, fare_amount |
| `invalid_distance` | source_file, error_reason, trip_distance |
| `invalid_passenger_count` | source_file, error_reason, passenger_count |
| `invalid_time_sequence` | source_file, error_reason, tpep_pickup_datetime, tpep_dropoff_datetime |
| `invalid_year` | source_file, error_reason, tpep_pickup_datetime |
| `multiple_errors` | source_file, error_reason, tpep_pickup_datetime, tpep_dropoff_datetime, fare_amount, trip_distance, passenger_count |

---

## Star Schema

### Diagram

```
             dim_vendor
             dim_payment_type
             dim_rate_code
dim_date --> fact_trips <-- dim_time
```

### Dimension Tables

**`dim_date`** — 1 row ต่อวันที่ที่พบในข้อมูล สร้างจาก `tpep_pickup_datetime`

| Column | Type | Description |
|--------|------|-------------|
| date_key | int | Surrogate key (YYYYMMDD) |
| full_date | date | วันที่จริง |
| year, month, day | int | ส่วนประกอบวันที่ |
| day_of_week | int | 0=จันทร์ … 6=อาทิตย์ |
| day_name | str | ชื่อวันภาษาอังกฤษ |
| month_name | str | ชื่อเดือนภาษาอังกฤษ |
| quarter | int | ไตรมาส (1–4) |
| is_weekend | bool | เป็น Sat/Sun หรือไม่ |

**`dim_time`** — 24 rows (รายชั่วโมง)

| Column | Type | Description |
|--------|------|-------------|
| time_key | int | ชั่วโมง (0–23) |
| hour | int | ชั่วโมง |
| time_period | str | night (0-5) / morning (6-11) / afternoon (12-17) / evening (18-23) |

**`dim_vendor`** — 3 rows

| vendor_key | vendor_id | vendor_name |
|-----------|-----------|-------------|
| 1 | 1 | Creative Mobile Technologies |
| 2 | 2 | VeriFone Inc. |
| 0 | 0 | Unknown |

**`dim_payment_type`** — 7 rows

| payment_key | payment_type_code | payment_description |
|------------|-------------------|---------------------|
| 1 | 1 | Credit card |
| 2 | 2 | Cash |
| 3 | 3 | No charge |
| 4 | 4 | Dispute |
| 5 | 5 | Unknown |
| 6 | 6 | Voided trip |
| 0 | 0 | Not recorded |

**`dim_rate_code`** — 7 rows

| rate_code_key | rate_code_id | rate_code_description |
|--------------|--------------|----------------------|
| 1 | 1 | Standard rate |
| 2 | 2 | JFK |
| 3 | 3 | Newark |
| 4 | 4 | Nassau or Westchester |
| 5 | 5 | Negotiated fare |
| 6 | 6 | Group ride |
| 0 | 0 | Not recorded |

### Fact Table — `fact_trips`

1 row = 1 เที่ยวแท็กซี่

| Column | Type | Description |
|--------|------|-------------|
| trip_id | int | Surrogate PK |
| date_key | int | FK → dim_date |
| time_key | int | FK → dim_time (ชั่วโมง pickup) |
| vendor_key | int | FK → dim_vendor |
| payment_key | int | FK → dim_payment_type |
| rate_code_key | int | FK → dim_rate_code |
| PULocationID | int | โซน pickup |
| DOLocationID | int | โซน dropoff |
| passenger_count | int | จำนวนผู้โดยสาร |
| trip_distance | float | ระยะทาง (miles) |
| fare_amount | float | ค่าโดยสารพื้นฐาน ($) |
| extra | float | ค่าธรรมเนียมเพิ่มเติม ($) |
| mta_tax | float | MTA tax ($) |
| tip_amount | float | Tip ($) |
| tolls_amount | float | ค่าผ่านทาง ($) |
| improvement_surcharge | float | Improvement surcharge ($) |
| congestion_surcharge | float | Congestion surcharge ($) |
| total_amount | float | ยอดรวมทั้งหมด ($) |
| trip_duration_minutes | float | ระยะเวลาเดินทาง (นาที) คำนวณจาก dropoff − pickup |

---

## Testing

**Unit Tests** — แบ่งตาม pipeline module ไม่ต้องรัน Airflow จริง

| ไฟล์ | ครอบคลุม |
|------|---------|
| `tests/test_validation.py` | `get_csv_files`, `validate_and_split` |
| `tests/test_storage.py` | `save_clean_data`, `save_quarantine_data` |
| `tests/test_reporting.py` | `generate_report` (alert threshold, percentages) |
| `tests/helpers.py` | shared utilities: `_make_context`, `_sample_df` |

รันทั้งหมด:
```bash
python -m pytest tests/test_validation.py tests/test_storage.py tests/test_reporting.py -v
```

**Data Quality Check** (`tests/check_fact_trips.py`)

ตรวจสอบความสมบูรณ์ของ Star Schema หลังรัน pipeline จริง ครอบคลุม 9 หัวข้อ:
1. ภาพรวม (row count, columns, date range)
2. Null values ทุก column
3. FK integrity ระหว่าง fact และ dimension tables
4. Measure sanity (ไม่มีค่าติดลบ)
5. Year consistency (ปีใน fact ตรงกับชื่อไฟล์ต้นทาง)
6. Summary statistics (mean, std, min, max)
7. Top 5 วันที่มี trip มากสุด
8. Trip breakdown by vendor
9. Trip breakdown by payment type

```bash
python tests/check_fact_trips.py
```

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| pandas | >=2.0.0 | Data processing |
| pyarrow | >=14.0.0 | Parquet I/O |
| psycopg2-binary | >=2.9.0 | PostgreSQL connection |
