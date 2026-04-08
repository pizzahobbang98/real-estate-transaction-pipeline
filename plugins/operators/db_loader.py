import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)


def get_conn():
   
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", 5433))
    return psycopg2.connect(
        host=host,
        port=port,
        dbname="realestate_db",
        user="realestate",
        password="realestate"
    )

def load_csv_to_db(csv_path: str):
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    trade_df = df[df["data_type"] == "매매"].copy()
    rent_df  = df[df["data_type"] == "전월세"].copy()

    conn = get_conn()
    cur  = conn.cursor()

    trade_rows = [
        (
            row.lawd_cd, row.deal_ymd, row.apt_name, row.umd_nm,
            int(row.deal_amount) if pd.notna(row.deal_amount) else None,
            float(row.area_sqm)  if pd.notna(row.area_sqm)   else None,
            int(row.floor)       if pd.notna(row.floor)       else None,
            int(row.build_year)  if pd.notna(row.build_year)  else None,
            int(row.deal_year)   if pd.notna(row.deal_year)   else None,
            int(row.deal_month)  if pd.notna(row.deal_month)  else None,
            int(row.deal_day)    if pd.notna(row.deal_day)    else None,
            row.dealing_gbn, row.cancel_deal, row.region_name,
        )
        for row in trade_df.itertuples()
    ]

    execute_values(cur, """
        INSERT INTO apt_trade
            (lawd_cd, deal_ymd, apt_name, umd_nm, deal_amount,
             area_sqm, floor, build_year, deal_year, deal_month,
             deal_day, dealing_gbn, cancel_deal, region_name)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, trade_rows)

    logger.info(f"apt_trade 적재 완료: {len(trade_rows)}건")

    rent_rows = [
        (
            row.lawd_cd, row.deal_ymd, row.apt_name, row.umd_nm,
            int(row.deposit)      if pd.notna(row.deposit)      else None,
            int(row.monthly_rent) if pd.notna(row.monthly_rent) else None,
            float(row.area_sqm)   if pd.notna(row.area_sqm)    else None,
            int(row.floor)        if pd.notna(row.floor)        else None,
            int(row.build_year)   if pd.notna(row.build_year)   else None,
            int(row.deal_year)    if pd.notna(row.deal_year)    else None,
            int(row.deal_month)   if pd.notna(row.deal_month)   else None,
            int(row.deal_day)     if pd.notna(row.deal_day)     else None,
            row.region_name,
        )
        for row in rent_df.itertuples()
    ]

    execute_values(cur, """
        INSERT INTO apt_rent
            (lawd_cd, deal_ymd, apt_name, umd_nm, deposit,
             monthly_rent, area_sqm, floor, build_year,
             deal_year, deal_month, deal_day, region_name)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, rent_rows)

    logger.info(f"apt_rent 적재 완료: {len(rent_rows)}건")

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_csv_to_db("output_202501.csv")
    print("적재 완료!")