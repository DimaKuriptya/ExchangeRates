import os
from io import BytesIO

import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account


def upload_df_to_gcs_parquet(
    df: pd.DataFrame,
    date: str
):
    load_dotenv()
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    bucket_name = os.getenv('GCP_BUCKET_NAME')
    project_id = os.getenv('GCP_PROJECT_ID')

    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = storage.Client(credentials=credentials, project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f'exchange_rates_{date}.parquet')

    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)  

    blob.upload_from_file(buffer, content_type='application/octet-stream')
    
    print(f"Successfully uploaded to GCS://{bucket_name}/exchange_rates_{date}.parquet")
