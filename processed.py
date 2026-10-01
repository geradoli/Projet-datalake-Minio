from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim, lower, regexp_replace, when, lit, current_timestamp
from pyspark.sql.types import IntegerType, DoubleType
from io import BytesIO
import json
import os
import tempfile
from pathlib import Path

import boto3
import pandas as pd

CREDENTIALS_PATH = "credentials.json"
with open(CREDENTIALS_PATH, "r") as f:
    creds = json.load(f)

MINIO_URL  = creds["url"]
ACCESS_KEY = creds["accessKey"]
SECRET_KEY = creds["secretKey"]

spark = (
    SparkSession.builder
    .appName("RAW_to_PROCESSED_FR")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    # Buffer in memory: avoids the NativeIO / local-disk check that fails on Windows
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "4")
    .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
    .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

print("Spark session créée avec succès")
print(f"MinIO : {MINIO_URL}")

BUCKET           = "tp2"
RAW_BUCKET       = "tp2/raw"
PROCESSED_BUCKET = "tp2/processed"


def read_file(spark, path, fmt, **options):
    fmt = fmt.lower()
    if fmt == "csv":
        df = (spark.read.option("header", "true").option("inferSchema", "true")
              .option("mode", "PERMISSIVE").csv(path))
    elif fmt == "json":
        # multiLine=true handles JSON arrays and pretty-printed files
        df = spark.read.option("multiLine", "true").json(path)
        if df.columns == ["_corrupt_record"]:
            raise ValueError(
                f"{path} could not be parsed as JSON (only _corrupt_record found). "
                "Check that the file is valid JSON."
            )
    elif fmt == "parquet":
        df = spark.read.parquet(path)
    elif fmt == "xml":
        df = (spark.read.format("xml")
              .option("rowTag", options.get("rowTag", "produit")).load(path))
    else:
        raise ValueError(f"Format non supporté : {fmt}")
    return df


TMP_KEY = "processed/_tmp/magasins_from_excel.csv"   # prefix the credentials can write to
_tmp_local_path = None


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )


def read_excel_from_minio(spark, key):
    """Spark can't read .xlsx natively. Download with boto3, parse with pandas,
    save as a temporary CSV and let Spark's JVM read that CSV (no Python worker,
    which crashes on Python 3.13 with Spark 3.5.1).
    The CSV goes to MinIO under processed/_tmp/; if that is refused, a local
    temp file is used instead."""
    global _tmp_local_path
    s3 = _s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    pdf = pd.read_excel(BytesIO(obj["Body"].read()), dtype=str)
    pdf.columns = [str(c) for c in pdf.columns]
    csv_bytes = pdf.to_csv(index=False).encode("utf-8")

    try:
        s3.put_object(Bucket=BUCKET, Key=TMP_KEY, Body=csv_bytes)
        csv_path = f"s3a://{BUCKET}/{TMP_KEY}"
        print(f"  CSV temporaire écrit dans MinIO : {csv_path}")
    except Exception as e:
        print(f"  [warn] écriture MinIO refusée ({e}); utilisation d'un fichier local")
        fd, _tmp_local_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "wb") as f:
            f.write(csv_bytes)
        csv_path = Path(_tmp_local_path).as_uri()

    return (spark.read
            .option("header", "true")
            .option("inferSchema", "false")   # keep everything as string
            .option("multiLine", "true")
            .option("encoding", "UTF-8")
            .csv(csv_path))


def delete_tmp_csv():
    try:
        _s3_client().delete_object(Bucket=BUCKET, Key=TMP_KEY)
    except Exception as e:
        print(f"  [info] suppression du CSV temporaire MinIO ignorée : {e}")
    if _tmp_local_path and os.path.exists(_tmp_local_path):
        try:
            os.remove(_tmp_local_path)
        except OSError:
            pass


def clean_column_names(df):
    for old_name in df.columns:
        new_name = old_name.strip().lower().replace(" ", "_").replace("-", "_").replace(".", "_")
        df = df.withColumnRenamed(old_name, new_name)
    return df


def standardize_string_columns(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(regexp_replace(col(c), r"[\r\n\t]+", " ")))
    return df


def normalize_ville(df, col_name="ville"):
    if col_name not in df.columns:
        print(f"  [warn] colonne '{col_name}' absente, normalisation ignorée. Colonnes : {df.columns}")
        return df
    return df.withColumn(
        col_name,
        when(col(col_name).isNull(), lit(None))
        .otherwise(trim(regexp_replace(lower(col(col_name)), r"\s+", " ")))
    )


# VENTES
print("\n=== Traitement ventes ===")
df_ventes = read_file(spark, f"s3a://{RAW_BUCKET}/ventes/ventes.csv", "csv")
df_ventes = clean_column_names(df_ventes)
df_ventes = standardize_string_columns(df_ventes)
df_ventes = (
    df_ventes
    .withColumn("date_vente", to_date(col("date_vente")))
    .withColumn("quantite", col("quantite").cast(IntegerType()))
    .filter(col("id_vente").isNotNull())
    .filter(col("quantite") > 0)
    .filter(col("date_vente").isNotNull())
    .dropDuplicates(["id_vente"])
    .withColumn("date_traitement", current_timestamp())
)
df_ventes.write.mode("overwrite").parquet(f"s3a://{PROCESSED_BUCKET}/ventes/")

# CLIENTS
print("\n=== Traitement clients ===")
df_clients = read_file(spark, f"s3a://{RAW_BUCKET}/clients/clients.json", "json")
df_clients = clean_column_names(df_clients)
df_clients = standardize_string_columns(df_clients)
df_clients = normalize_ville(df_clients, "ville")
df_clients = (
    df_clients
    .withColumn("segment", when(col("segment").isNull(), lit("Inconnu")).otherwise(col("segment")))
    .filter(col("id_client").isNotNull())
    .dropDuplicates(["id_client"])
    .withColumn("date_traitement", current_timestamp())
)
df_clients.write.mode("overwrite").parquet(f"s3a://{PROCESSED_BUCKET}/clients/")

# PRODUITS (needs spark-xml on Spark 3.5.1)
print("\n=== Traitement produits ===")
df_produits = read_file(spark, f"s3a://{RAW_BUCKET}/produits/produits.xml", "xml", rowTag="produit")
df_produits = clean_column_names(df_produits)
df_produits = standardize_string_columns(df_produits)
df_produits = (
    df_produits
    .withColumn("categorie", when(col("categorie").isNull(), lit("Inconnu")).otherwise(col("categorie")))
    .withColumn("prix_unitaire", col("prix_unitaire").cast(DoubleType()))
    .filter(col("id_produit").isNotNull())
    .dropDuplicates(["id_produit"])
    .withColumn("date_traitement", current_timestamp())
)
df_produits.write.mode("overwrite").parquet(f"s3a://{PROCESSED_BUCKET}/produits/")

# MAGASINS (xlsx -> pandas -> temp CSV in MinIO -> Spark)
print("\n=== Traitement magasins ===")
df_magasins = read_excel_from_minio(spark, "raw/magasins/magasins.xlsx")
df_magasins = clean_column_names(df_magasins)
print(f"  Colonnes magasins : {df_magasins.columns}")
df_magasins = standardize_string_columns(df_magasins)
df_magasins = normalize_ville(df_magasins, "ville")
df_magasins = (
    df_magasins
    .filter(col("id_magasin").isNotNull())
    .dropDuplicates(["id_magasin"])
    .withColumn("date_traitement", current_timestamp())
)
df_magasins.write.mode("overwrite").parquet(f"s3a://{PROCESSED_BUCKET}/magasins/")
delete_tmp_csv()

print("\nZone PROCESSED construite avec succès !")
spark.stop()