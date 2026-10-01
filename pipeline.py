"""
Pipeline complet : RAW -> PROCESSED -> CURATED

RAW
 -> lecture des données
 -> contrôles
 -> nettoyage
 -> standardisation
PROCESSED
 -> jointures / enrichissement
 -> agrégations
CURATED
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, trim, lower, regexp_replace, when, lit, current_timestamp,
    count, sum as spark_sum, round as spark_round, coalesce,
)
from pyspark.sql.types import IntegerType, DoubleType
from io import BytesIO
from pathlib import Path
import json
import os
import tempfile
import time

import boto3
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent

with open(BASE_DIR / "credentials.json", "r") as f:
    creds = json.load(f)

MINIO_URL  = creds["url"]
ACCESS_KEY = creds["accessKey"]
SECRET_KEY = creds["secretKey"]

BUCKET    = "tp2"
RAW       = f"s3a://{BUCKET}/raw"
PROCESSED = f"s3a://{BUCKET}/processed"
CURATED   = f"s3a://{BUCKET}/curated"
TMP_KEY   = "processed/_tmp/magasins_from_excel.csv"

JARS = [
    BASE_DIR / "jars" / "hadoop-aws-3.3.4.jar",
    BASE_DIR / "jars" / "aws-java-sdk-bundle-1.12.261.jar",
]
for j in JARS:
    if not j.exists():
        raise SystemExit(f"Jar introuvable : {j}")

spark = (
    SparkSession.builder
    .appName("PIPELINE_RAW_PROCESSED_CURATED")
    .config("spark.jars", ",".join(j.as_uri() for j in JARS))
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    # Buffer en mémoire : évite l'erreur NativeIO sous Windows
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "4")
    .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
    .config("spark.hadoop.fs.s3a.impl.disable.cache", "true")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# 1. Lecture des données RAW
# ---------------------------------------------------------------------------
def s3_client():
    return boto3.client(
        "s3", endpoint_url=MINIO_URL,
        aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,
    )


_tmp_local_path = None


def read_excel(key):
    """xlsx -> pandas -> CSV temporaire -> lecture par la JVM Spark.
    (Évite spark.createDataFrame sur objets Python : les workers Python
    plantent avec Python 3.13 + Spark 3.5.1.)"""
    global _tmp_local_path
    s3 = s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    pdf = pd.read_excel(BytesIO(obj["Body"].read()), dtype=str)
    pdf.columns = [str(c) for c in pdf.columns]
    csv_bytes = pdf.to_csv(index=False).encode("utf-8")
    try:
        s3.put_object(Bucket=BUCKET, Key=TMP_KEY, Body=csv_bytes)
        path = f"s3a://{BUCKET}/{TMP_KEY}"
    except Exception as e:
        log(f"  [warn] écriture MinIO refusée ({e}) -> fichier local temporaire")
        fd, _tmp_local_path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "wb") as f:
            f.write(csv_bytes)
        path = Path(_tmp_local_path).as_uri()
    return (spark.read.option("header", "true").option("inferSchema", "false")
            .option("multiLine", "true").option("encoding", "UTF-8").csv(path))


def cleanup_tmp():
    try:
        s3_client().delete_object(Bucket=BUCKET, Key=TMP_KEY)
    except Exception as e:
        log(f"  [info] suppression du CSV temporaire ignorée : {e}")
    if _tmp_local_path and os.path.exists(_tmp_local_path):
        try:
            os.remove(_tmp_local_path)
        except OSError:
            pass


def read_raw():
    log("Lecture RAW")
    ventes = (spark.read.option("header", "true").option("inferSchema", "true")
              .option("mode", "PERMISSIVE").csv(f"{RAW}/ventes/ventes.csv"))
    clients = spark.read.option("multiLine", "true").json(f"{RAW}/clients/clients.json")
    produits = (spark.read.format("xml").option("rowTag", "produit")
                .load(f"{RAW}/produits/produits.xml"))
    magasins = read_excel("raw/magasins/magasins.xlsx")
    return ventes, clients, produits, magasins


# ---------------------------------------------------------------------------
# 2. Contrôles
# ---------------------------------------------------------------------------
def controle(df, nom, cle, colonnes_attendues=()):
    """Contrôles de base. Arrête le pipeline si le fichier est inutilisable."""
    cols = [c.strip().lower() for c in df.columns]
    if cols == ["_corrupt_record"]:
        raise SystemExit(f"[{nom}] fichier illisible (_corrupt_record)")
    manquantes = [c for c in colonnes_attendues if c not in cols]
    if manquantes:
        raise SystemExit(f"[{nom}] colonnes manquantes : {manquantes} (présentes : {cols})")
    n = df.count()
    if n == 0:
        raise SystemExit(f"[{nom}] aucune ligne lue")
    cle_col = next(c for c in df.columns if c.strip().lower() == cle)
    nulls = df.filter(col(cle_col).isNull()).count()
    doublons = n - df.select(cle_col).distinct().count()
    log(f"  contrôle {nom}: {n} lignes | clé '{cle}' nulle: {nulls} | doublons de clé: {doublons}")
    return n


# ---------------------------------------------------------------------------
# 3. Nettoyage / standardisation
# ---------------------------------------------------------------------------
def clean_column_names(df):
    for old in df.columns:
        new = old.strip().lower().replace(" ", "_").replace("-", "_").replace(".", "_")
        df = df.withColumnRenamed(old, new)
    return df


def standardize_strings(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(regexp_replace(col(c), r"[\r\n\t]+", " ")))
    return df


def normalize_ville(df, c="ville"):
    if c not in df.columns:
        log(f"  [warn] colonne '{c}' absente")
        return df
    return df.withColumn(
        c, when(col(c).isNull(), lit(None))
        .otherwise(trim(regexp_replace(lower(col(c)), r"\s+", " "))))


def to_processed(ventes, clients, produits, magasins):
    log("Nettoyage / standardisation -> PROCESSED")

    ventes = standardize_strings(clean_column_names(ventes))
    n_avant = ventes.count()
    ventes = (
        ventes
        .withColumn("date_vente", to_date(col("date_vente")))
        .withColumn("quantite", col("quantite").cast(IntegerType()))
        .filter(col("id_vente").isNotNull())
        .filter(col("quantite") > 0)
        .filter(col("date_vente").isNotNull())
        .dropDuplicates(["id_vente"])
        .withColumn("date_traitement", current_timestamp())
    )
    log(f"  ventes: {n_avant} -> {ventes.count()} lignes (rejets: lignes invalides ou doublons)")

    clients = normalize_ville(standardize_strings(clean_column_names(clients)))
    clients = (
        clients
        .withColumn("segment", when(col("segment").isNull(), lit("Inconnu")).otherwise(col("segment")))
        .filter(col("id_client").isNotNull())
        .dropDuplicates(["id_client"])
        .withColumn("date_traitement", current_timestamp())
    )

    produits = standardize_strings(clean_column_names(produits))
    produits = (
        produits
        .withColumn("categorie", when(col("categorie").isNull(), lit("Inconnu")).otherwise(col("categorie")))
        .withColumn("prix_unitaire", col("prix_unitaire").cast(DoubleType()))
        .filter(col("id_produit").isNotNull())
        .dropDuplicates(["id_produit"])
        .withColumn("date_traitement", current_timestamp())
    )

    magasins = normalize_ville(standardize_strings(clean_column_names(magasins)))
    log(f"  colonnes magasins : {magasins.columns}")
    magasins = (
        magasins
        .filter(col("id_magasin").isNotNull())
        .dropDuplicates(["id_magasin"])
        .withColumn("date_traitement", current_timestamp())
    )

    for nom, df in [("ventes", ventes), ("clients", clients),
                    ("produits", produits), ("magasins", magasins)]:
        df.write.mode("overwrite").parquet(f"{PROCESSED}/{nom}/")
        log(f"  écrit {PROCESSED}/{nom}/")


# ---------------------------------------------------------------------------
# 4. Jointures / enrichissement (repart de PROCESSED)
# ---------------------------------------------------------------------------
def enrichir():
    log("Jointures / enrichissement")
    ventes   = spark.read.parquet(f"{PROCESSED}/ventes/").drop("date_traitement")
    clients  = spark.read.parquet(f"{PROCESSED}/clients/")
    produits = spark.read.parquet(f"{PROCESSED}/produits/")
    magasins = spark.read.parquet(f"{PROCESSED}/magasins/")

    clients = clients.select("id_client", col("nom").alias("nom_client"),
                             "segment", col("ville").alias("ville_client"))
    produits = produits.select("id_produit", "nom_produit", "categorie", "prix_unitaire")

    mag_cols = []
    for c in magasins.columns:
        if c in ("id_magasin", "date_traitement"):
            continue
        mag_cols.append(col(c).alias(c if "magasin" in c else f"{c}_magasin"))
    magasins = magasins.select("id_magasin", *mag_cols)

    enrichies = (
        ventes
        .join(clients,  "id_client",  "left")
        .join(produits, "id_produit", "left")
        .join(magasins, "id_magasin", "left")
        .withColumn("montant", spark_round(col("quantite") * col("prix_unitaire"), 2))
        .withColumn("date_traitement", current_timestamp())
    )

    n_v, n_e = ventes.count(), enrichies.count()
    log(f"  ventes: {n_v} | enrichies: {n_e}"
        + ("" if n_v == n_e else "  [warn] doublons de clé dans une table de référence ?"))
    log(f"  sans client: {enrichies.filter(col('nom_client').isNull()).count()} | "
        f"sans produit: {enrichies.filter(col('nom_produit').isNull()).count()}")

    enrichies.write.mode("overwrite").parquet(f"{PROCESSED}/ventes_enrichies/")
    log(f"  écrit {PROCESSED}/ventes_enrichies/")


# ---------------------------------------------------------------------------
# 5. Agrégations -> CURATED
# ---------------------------------------------------------------------------
def indicateurs(df, dimension):
    return (
        df.groupBy(dimension)
        .agg(count("id_vente").alias("nombre_ventes"),
             spark_sum("quantite").alias("quantite_totale"),
             spark_round(spark_sum("montant"), 2).alias("chiffre_affaires"))
        .orderBy(col("chiffre_affaires").desc())
    )


def to_curated():
    log("Agrégations -> CURATED")
    ventes = spark.read.parquet(f"{PROCESSED}/ventes_enrichies/")

    if "ville_magasin" in ventes.columns:
        ville_col = "ville_magasin"
    elif "ville_client" in ventes.columns:
        ville_col = "ville_client"
    else:
        raise SystemExit("Aucune colonne ville dans ventes_enrichies")
    log(f"  ville utilisée : {ville_col}")

    par_ville = indicateurs(
        ventes.withColumn("ville", coalesce(col(ville_col), lit("Inconnue"))), "ville")
    par_ville.write.mode("overwrite").parquet(f"{CURATED}/activite_par_ville/")
    par_ville.show(truncate=False)

    par_categorie = indicateurs(
        ventes.withColumn("categorie", coalesce(col("categorie"), lit("Inconnu"))), "categorie")
    par_categorie.write.mode("overwrite").parquet(f"{CURATED}/activite_par_categorie/")
    par_categorie.show(truncate=False)

    log(f"  écrit {CURATED}/activite_par_ville/ et {CURATED}/activite_par_categorie/")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def main():
    t0 = time.time()
    try:
        ventes, clients, produits, magasins = read_raw()

        log("Contrôles RAW")
        controle(ventes,   "ventes",   "id_vente",   ("id_vente", "date_vente", "quantite",
                                                       "id_client", "id_produit", "id_magasin"))
        controle(clients,  "clients",  "id_client",  ("id_client",))
        controle(produits, "produits", "id_produit", ("id_produit", "prix_unitaire"))
        controle(magasins, "magasins", "id_magasin", ("id_magasin",))

        to_processed(ventes, clients, produits, magasins)
        enrichir()
        to_curated()
        log(f"Pipeline terminé avec succès en {time.time() - t0:.1f} s")
    finally:
        cleanup_tmp()
        spark.stop()


if __name__ == "__main__":
    main()
