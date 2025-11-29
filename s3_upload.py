import boto3
import json
from config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    AWS_REGION,
    S3_BUCKET,
    S3_FILE
)

def upload_to_s3(data):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_FILE,
        Body=json.dumps(data)
    )

    return "Data uploaded to S3 successfully!"
