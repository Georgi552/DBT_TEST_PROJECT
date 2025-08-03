import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import requests
import argparse
import sys
import json

DEFAULT_CSV_URL = "https://gist.githubusercontent.com/fm-dp/e5beb68cf717dc6a8db91250e9320b9e/raw/c442fe6feb1f73f9f9db04171aee6fe02505ceb8/homework.csv"

def download_csv(url, local_path="homework.csv"):
    r = requests.get(url)
    r.raise_for_status()
    with open(local_path, "wb") as f:
        f.write(r.content)
    return local_path

def clean_and_upper(col: str) -> str:
    cleaned = col.strip().lower().replace(" ", "_").replace("-", "_").replace('"', "")
    return cleaned.upper()

def create_table_if_not_exists(conn, table_name: str):
    cs = conn.cursor()
    try:
        cs.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            CUSTOMER_ID STRING,
            CUSTOMER_NAME STRING,
            CUSTOMER_PHONE STRING,
            CUSTOMER_EMAIL STRING,
            ORDER_ID STRING,
            ORDER_DATE STRING,
            ORDER_TOTAL FLOAT,
            ORDER_ITEMS VARIANT,
            RAW_REST VARIANT
        )
        """)
    finally:
        cs.close()

def main():
    parser = argparse.ArgumentParser(description="Load homework.csv into Snowflake (single main table)")
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--role", default=None)
    parser.add_argument("--csv_url", default=DEFAULT_CSV_URL)
    parser.add_argument("--table", default="HOMEWORK_RAW")
    args = parser.parse_args()

    print("1. Downloading CSV...")
    try:
        csv_path = download_csv(args.csv_url)
    except Exception as e:
        print("Error downloading:", e)
        sys.exit(1)

    print("2. Reading CSV with pandas...")
    df = pd.read_csv(csv_path)

    print("DEBUG: original columns:", df.columns.tolist())
    df.columns = [clean_and_upper(c) for c in df.columns]
    print("DEBUG: cleaned columns:", df.columns.tolist())

    required = [
        "CUSTOMER_ID",
        "CUSTOMER_NAME",
        "CUSTOMER_PHONE",
        "CUSTOMER_EMAIL",
        "ORDER_ID",
        "ORDER_DATE",
        "ORDER_TOTAL",
        "ORDER_ITEMS",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print("Missing expected columns:", missing)

    # Keep ORDER_ITEMS as-is (string), but attempt to validate JSON for VARIANT later in Snowflake if needed
    if "ORDER_ITEMS" in df.columns:
        df["ORDER_ITEMS"] = df["ORDER_ITEMS"].astype(str)
    else:
        print("Missing ORDER_ITEMS, it will be NULL in target table")

    others = [c for c in df.columns if c not in required]
    if others:
        df["RAW_REST"] = df[others].to_dict(orient="records")
    else:
        df["RAW_REST"] = None

    final_cols = required + ["RAW_REST"]
    existing = [c for c in final_cols if c in df.columns]
    df = df[existing]

    print("3. Connecting to Snowflake...")
    conn = None
    try:
        conn_kwargs = {
            "user": args.user,
            "password": args.password,
            "account": args.account,
            "warehouse": args.warehouse,
            "database": args.database,
            "schema": args.schema,
        }
        if args.role:
            conn_kwargs["role"] = args.role

        conn = snowflake.connector.connect(**conn_kwargs)
        target_table = args.table.upper()

        print(f"4. Creating table {target_table} if not exists...")
        create_table_if_not_exists(conn, target_table)

        print(f"5. Truncating {target_table}...")
        cs = conn.cursor()
        try:
            cs.execute(f"TRUNCATE TABLE {target_table}")
        finally:
            cs.close()

        print(f"6. Uploading into {target_table}...")
        success, nchunks, nrows, _ = write_pandas(conn, df, target_table, auto_create_table=False)
        if not success:
            print("Upload unsuccessful")
            return
        print(f"Uploaded {nrows} rows into {nchunks} chunks.")
    except Exception as e:
        print("Error during processing:", e)
        sys.exit(1)
    finally:
        if conn:
            conn.close()

    print("Done.")

if __name__ == "__main__":
    main()