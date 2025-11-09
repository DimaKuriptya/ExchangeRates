from upload_to_gcp import upload_df_to_gcs_parquet
from fetch_data import fetch_api_data

if __name__ == '__main__':
    df, date = fetch_api_data()
    upload_df_to_gcs_parquet(df, date)
