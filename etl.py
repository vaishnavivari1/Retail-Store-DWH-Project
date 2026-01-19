#!/usr/bin/env python3
"""
etl/load_warehouse.py  (updated EXPECTED_COLUMNS + date parsing + numeric cleaning + robust insert)
- Reads config from .env
- Runs SQL scripts (01,02) to create schemas/tables
- Combines raw sales CSVs into stg.SalesRaw using pyodbc fast_executemany (chunked)
- Runs ETL SQL scripts (03,04,05)
- Runs index creation (06) and validation (07)
"""

import os
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyodbc
from dotenv import load_dotenv

# ----------------------------
# Config / Logging
# ----------------------------
load_dotenv()

from logger_setup import get_logger
LOG = get_logger("etl.log")


SQL_SERVER = os.getenv("SERVER")
SQL_DATABASE = os.getenv("DATABASE")
SQL_TRUSTED_CONNECTION = os.getenv("SQL_TRUSTED_CONNECTION", "yes").lower()
SQL_DRIVER = os.getenv("DRIVER")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATA_FOLDER = Path(os.getenv("CSV_FOLDER", ".")).expanduser().resolve()
SQL_FOLDER = Path(os.getenv("SQL_FOLDER", ".")).expanduser().resolve()

if not DATA_FOLDER.exists():
    raise FileNotFoundError(f"DATA_FOLDER does not exist: {DATA_FOLDER}")
if not SQL_FOLDER.exists():
    raise FileNotFoundError(f"SQL_FOLDER does not exist: {SQL_FOLDER}")

# ----------------------------
# Staging schema column order (must match CSVs)
# ----------------------------
EXPECTED_COLUMNS = [
    "OrderDate","OrderID","StoreID","CustomerID","ProductID","Quantity",
    "OrderAmount","DiscountAmount","ShippingCost","TotalAmount",
    "StoreName","StoreType","StoreOpeningDate","StoreAddress","StoreCity",
    "StoreState","StoreZipCode","StoreCountry","StoreRegion","StoreManagerName",
    "FirstName","LastName","Gender","DOB","Email","CustomerAddress","CustomerCity",
    "CustomerState","CustomerZipCode","CustomerCountry","LoyalityProgramID",
    "ProductName","Category","Brand","UnitPrice",
    "ProgramName","TierLevel","PointsMultiplier","AnnualFee",
    "SourceFile"
]

# ensure EXPECTED_COLUMNS has no duplicates
if len(EXPECTED_COLUMNS) != len(set(EXPECTED_COLUMNS)):
    dupes = [c for c in EXPECTED_COLUMNS if EXPECTED_COLUMNS.count(c) > 1]
    raise ValueError(f"EXPECTED_COLUMNS contains duplicate entries: {set(dupes)}")

# ----------------------------
# Build pyodbc connection string
# ----------------------------
def build_pyodbc_conn_str():
    drv = SQL_DRIVER
    if SQL_TRUSTED_CONNECTION == "yes":
        return f"DRIVER={{{drv}}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};Trusted_Connection=yes;"
    else:
        if not DB_USER or not DB_PASSWORD:
            raise ValueError("SQL_USER and SQL_PASSWORD must be set when SQL_TRUSTED_CONNECTION=no")
        return f"DRIVER={{{drv}}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={DB_USER};PWD={DB_PASSWORD};"

PYODBC_CONN_STR = build_pyodbc_conn_str()

# ----------------------------
# Utilities
# ----------------------------
def run_sql_file(conn, filepath):
    """Run a .sql file; supports GO separators."""
    LOG.info("Executing SQL file: %s", filepath.name)
    text = filepath.read_text(encoding="utf-8")
    batches = []
    current = []
    for line in text.splitlines():
        if line.strip().upper() == "GO":
            if current:
                batches.append("\n".join(current))
                current = []
        else:
            current.append(line)
    if current:
        batches.append("\n".join(current))

    cur = conn.cursor()
    try:
        for batch in batches:
            stmt = batch.strip()
            if stmt:
                cur.execute(stmt)
        conn.commit()
        LOG.info("Completed: %s", filepath.name)
    except Exception:
        conn.rollback()
        LOG.exception("SQL error in %s", filepath.name)
        raise
    finally:
        cur.close()

def chunked_iterable(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

# numeric cleaning helper
def _clean_numeric_series(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str).str.strip()
    s = s.replace({"": None})
    # remove commas, currency signs, percent
    s = pd.Series(s).str.replace(r"[,\$\%]", "", regex=True)
    # convert (123) style to -123
    s = s.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    return pd.to_numeric(s.replace({"": None}), errors="coerce")

# ----------------------------
# Main ETL steps
# ----------------------------
def load_and_combine_csvs_to_staging(conn, batch_size = 50000):
    # Original script looked for raw_offline_retail_sales_*.csv; keep fallback to sales_*.csv
    files = sorted(DATA_FOLDER.glob("raw_offline_retail_sales_*.csv"))
    if not files:
        alt = sorted(DATA_FOLDER.glob("sales_*.csv"))
        files = alt

    if not files:
        raise FileNotFoundError(f"No CSV files found in {DATA_FOLDER} matching 'raw_offline_retail_sales_*.csv' or 'sales_*.csv'")
    LOG.info("Found %d files to load.", len(files))
    dfs = []
    for f in files:
        LOG.info("[CSV] Reading %s", f.name)
        # read CSV as strings
        df = pd.read_csv(f, dtype=str, keep_default_na=False)

        # normalize header names: strip and remove BOM
        df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]

        # Keep only expected columns that exist in CSV to be robust
        missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns and c != "SourceFile"]
        if missing_cols:
            raise ValueError(f"CSV missing expected columns: {missing_cols} in file {f.name}. Found columns: {list(df.columns)[:50]}")

        # Add SourceFile
        df["SourceFile"] = f.name

        # Reindex to expected columns (for absent optional columns pandas will fill with None)
        for c in EXPECTED_COLUMNS:
            if c not in df.columns:
                df[c] = None
        df = df[EXPECTED_COLUMNS]
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    LOG.info("[CSV] Combined rows: %d", len(combined))

    # Convert some columns to numeric types where applicable
    numeric_cols = ["Quantity","OrderAmount","DiscountAmount","ShippingCost","TotalAmount","UnitPrice","PointsMultiplier","AnnualFee"]
    for col in numeric_cols:
        if col in combined.columns:
            combined[col] = _clean_numeric_series(combined[col])

    # Convert dates (dayfirst=True to handle dd-mm-yyyy like Excel sample)
    date_cols = ["OrderDate","StoreOpeningDate","DOB"]
    for col in date_cols:
        if col in combined.columns:
            combined[col] = pd.to_datetime(combined[col], errors="coerce", dayfirst=True).dt.date

    # Ensure column ordering
    combined = combined[EXPECTED_COLUMNS]

    col_list = ", ".join(f"[{c}]" for c in EXPECTED_COLUMNS)
    placeholders = ", ".join("?" for _ in EXPECTED_COLUMNS)
    insert_sql = f"INSERT INTO stg.SalesRaw ({col_list}) VALUES ({placeholders})"

    rows = list(map(tuple, combined.to_numpy()))
    LOG.info("Preparing to insert %d rows into stg.SalesRaw", len(rows))

    cur = conn.cursor()
    cur.fast_executemany = True

    try:
        # Try fast batch insert
        for batch in chunked_iterable(rows, batch_size):
            cur.executemany(insert_sql, batch)
            conn.commit()
            LOG.info("Inserted batch of %d rows", len(batch))
        LOG.info("All rows inserted into stg.SalesRaw (fast path)")
    except Exception as batch_err:
        # Batch failed: fallback to per-row insert to find offending rows
        conn.rollback()
        LOG.error("Batch insert failed: %s. Falling back to per-row insert.", batch_err)
        bad_rows = []
        cur = conn.cursor()
        cur.fast_executemany = False
        for i, row in enumerate(rows):
            try:
                cur.execute(insert_sql, row)
                if (i % 1000) == 0:
                    conn.commit()
            except Exception as e:
                conn.rollback()
                src_file = "unknown"
                try:
                    src_file = row[EXPECTED_COLUMNS.index("SourceFile")]
                except Exception:
                    pass
                LOG.error("Row insert failed at index %d (source=%s): %s", i, src_file, e)
                # capture numeric values for debugging
                numeric_values = {c: row[EXPECTED_COLUMNS.index(c)] for c in numeric_cols if c in EXPECTED_COLUMNS}
                LOG.error("Numeric values in failing row: %s", numeric_values)
                bad_rows.append((i, src_file, e, numeric_values))
                if len(bad_rows) >= 10:
                    LOG.error("Collected %d bad rows — stopping further attempts.", len(bad_rows))
                    break
        if bad_rows:
            idx, src, err_obj, vals = bad_rows[0]
            raise RuntimeError(f"Insert failed. First bad row index {idx} from file {src}. Error: {err_obj}. Numeric values: {vals}")
        else:
            LOG.exception("Batch insert failed but no bad rows discovered in fallback. Re-raising.")
            raise
    finally:
        cur.close()

def run_validation(conn: pyodbc.Connection):
    LOG.info("Running validation queries.")
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(1) FROM stg.SalesRaw;")
        sr = cur.fetchone()[0]
        LOG.info("stg.SalesRaw rows: %d", sr)

        cur.execute("SELECT COUNT(1) FROM dw.FactSales;")
        fs = cur.fetchone()[0]
        LOG.info("dw.FactSales rows: %d", fs)
    except Exception:
        LOG.exception("Validation query failed")
        raise
    finally:
        cur.close()

def main():
    LOG.info("Starting ETL process")
    conn = pyodbc.connect(PYODBC_CONN_STR)
    conn.autocommit = False

    try:
        # Create schemas & tables
        run_sql_file(conn, SQL_FOLDER / "01_create_schemas.sql")
        run_sql_file(conn, SQL_FOLDER / "02_create_staging_and_dw_tables.sql")

        # Load CSVs -> stg.SalesRaw
        load_and_combine_csvs_to_staging(conn, batch_size=50000)

        # Run dimension & fact loads
        run_sql_file(conn, SQL_FOLDER / "03_load_dim_date.sql")
        run_sql_file(conn, SQL_FOLDER / "04_load_dimensions.sql")
        run_sql_file(conn, SQL_FOLDER / "05_load_fact_sales.sql")

        # create indexes
        run_sql_file(conn, SQL_FOLDER / "06_create_indexes.sql")

        # simple validation
        run_sql_file(conn, SQL_FOLDER / "07_validation.sql")
        run_validation(conn)

        LOG.info("ETL completed successfully")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
