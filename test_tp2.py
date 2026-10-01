import json
import boto3
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError

with open("credentials.json", "r", encoding="utf-8") as f:
    credentials = json.load(f)

s3 = boto3.client(
    "s3",
    endpoint_url=credentials["url"],
    aws_access_key_id=credentials["accessKey"],
    aws_secret_access_key=credentials["secretKey"]
)

# 1. Lister les objets
print("--- Objets du bucket tp2 ---")

response = s3.list_objects_v2(Bucket="tp2")

for obj in response.get("Contents", []):
    print(obj["Key"])


# 2. Lire ventes.csv
print("\n--- Lecture de raw/ventes.csv ---")

response = s3.get_object(
    Bucket="tp2",
    Key="raw/ventes.csv"
)

print(response["Body"].read().decode("utf-8"))


# 3. Tester l'écriture
print("\n--- Test écriture dans RAW ---")

try:
    s3.upload_file(
        "test.txt",
        "tp2",
        "raw/test.txt"
    )
    print("RAW : upload autorisé")

except (ClientError, S3UploadFailedError):
    print("RAW : upload refusé")


print("\n--- Test écriture dans PROCESSED ---")

try:
    s3.upload_file(
        "test.txt",
        "tp2",
        "processed/test.txt"
    )
    print("PROCESSED : upload autorisé")

except (ClientError, S3UploadFailedError):
    print("PROCESSED : upload refusé")


print("\n--- Test écriture dans CURATED ---")

try:
    s3.upload_file(
        "test.txt",
        "tp2",
        "curated/test.txt"
    )
    print("CURATED : upload autorisé")

except (ClientError, S3UploadFailedError):
    print("CURATED : upload refusé")