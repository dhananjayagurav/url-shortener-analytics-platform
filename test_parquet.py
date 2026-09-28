from url_shortener_analytics.config import get_settings
from url_shortener_analytics.object_store import get_s3_client
import pandas as pd
import io

settings = get_settings()
s3 = get_s3_client(settings)

response = s3.get_object(Bucket=settings.minio_bucket, Key="bronze/urls/ingestion_date=2026-09-25/urls.parquet")
df = pd.read_parquet(io.BytesIO(response["Body"].read()))
print(df.head())
