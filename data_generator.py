import csv
import io
import time
from datetime import datetime

import boto3

# RustFS connection
S3_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "dataops"
SECRET_KEY = "Ankara06"

BUCKET_NAME = "datasets"
PREFIX = "iot-temp-input/"

# Dataset
SOURCE_FILE = "data/IOT-temp.csv"

# Streaming configuration
BATCH_SIZE = 10
INTERVAL_SECONDS = 5

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1",
)

with open(SOURCE_FILE, "r", encoding="utf-8-sig", newline="") as file:

    reader = csv.DictReader(file)

    fieldnames = list(reader.fieldnames) + ["gentime_time"]

    batch = []
    batch_number = 0

    for row in reader:

        row["gentime_time"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )

        batch.append(row)

        if len(batch) >= BATCH_SIZE:

            output = io.StringIO()

            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(batch)

            object_key = f"{PREFIX}batch_{batch_number:05d}.csv"

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=object_key,
                Body=output.getvalue().encode("utf-8"),
                ContentType="text/csv",
            )

            print(
                f"Uploaded {object_key} "
                f"({len(batch)} records)"
            )

            batch.clear()
            batch_number += 1

            time.sleep(INTERVAL_SECONDS)

print("Data generation completed.")