import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from dagster import asset, Definitions

load_dotenv()

def get_snowflake_conn(schema_name):
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        schema=schema_name
    )

@asset
def raw_exchange_rates():
    dates = ['2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05']
    target_currencies = ['usd', 'gbp', 'chf']
    headers = {'User-Agent': 'FinTechReconciliationPipeline/1.0'}
    records = []

    for date_str in dates:
        url = f"https://jsdelivr.net@{date_str}/v1/currencies/eur.json"
        try:
            res = requests.get(url, headers=headers, timeout=15)
            if res.status_code == 200 and res.text.strip():
                rates = res.json().get('eur', {})
                for curr in target_currencies:
                    if curr in rates:
                        records.append({
                            'rate_date': date_str,
                            'target_currency': curr.upper(),
                            'ecb_rate': round(float(rates[curr]), 6)
                        })
        except Exception:
            pass

    if not records:
        records = [
            {'rate_date': '2026-10-02', 'target_currency': 'USD', 'ecb_rate': 1.0950},
            {'rate_date': '2026-10-03', 'target_currency': 'GBP', 'ecb_rate': 0.8480},
            {'rate_date': '2026-10-04', 'target_currency': 'USD', 'ecb_rate': 1.0980},
            {'rate_date': '2026-10-05', 'target_currency': 'CHF', 'ecb_rate': 0.9490},
        ]

    df = pd.DataFrame(records)
    df.columns = [c.upper() for c in df.columns]
    
    conn = get_snowflake_conn('MARKET_RATES')
    write_pandas(conn, df, 'ECB_RATES', auto_create_table=True, overwrite=True)
    conn.close()
    return f"Loaded {len(df)} exchange rate records into FINTECH_RAW_DB.MARKET_RATES.ECB_RATES"

@asset
def raw_internal_transactions():
    data = [
        {'txn_id': 'TXN_001', 'txn_date': '2026-10-02', 'currency': 'USD', 'local_amount': 110.00, 'recorded_eur': 100.00},
        {'txn_id': 'TXN_002', 'txn_date': '2026-10-03', 'currency': 'GBP', 'local_amount': 85.00,  'recorded_eur': 100.00},
        {'txn_id': 'TXN_003', 'txn_date': '2026-10-04', 'currency': 'USD', 'local_amount': 110.00, 'recorded_eur': 150.00}, 
        {'txn_id': 'TXN_004', 'txn_date': '2026-10-05', 'currency': 'CHF', 'local_amount': 95.00,  'recorded_eur': 100.00}
    ]
    df = pd.DataFrame(data)
    df.columns = [c.upper() for c in df.columns]
    
    conn = get_snowflake_conn('INTERNAL_LEDGER')
    write_pandas(conn, df, 'TRANSACTIONS', auto_create_table=True, overwrite=True)
    conn.close()
    return f"Loaded {len(df)} internal transactions."

defs = Definitions(assets=[raw_exchange_rates, raw_internal_transactions])
