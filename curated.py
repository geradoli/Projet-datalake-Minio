from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, round as spark_round, coalesce, lit
import json
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Zone CURATED : on repart de PROCESSED (ventes_enrichies), jamais de RAW.
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

with open(BASE_DIR / "credentials.json", "r") as f:
    creds = json.load(f)

MINIO_URL  = creds["url"]
ACCESS_KEY = creds["accessKey"]
SECRET_KEY = creds["secretKey"]

# Jars S3A déclarés ici : fonctionne avec `python curated.py` ou spark-submit.
JARS = [
    BASE_DIR / "jars" / "hadoop-aws-3.3.4.jar",
    BASE_DIR / "jars" / "aws-java-sdk-bundle-1.12.261.jar",
]
for j in JARS:
    if not j.exists():
        raise SystemExit(f"Jar introuvable : {j}")

spark = (
    SparkSession.builder
    .appName("PROCESSED_to_CURATED")
    .config("spark.jars", ",".join(j.as_uri() for j in JARS))
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
    .getOrCreate()
)

PROCESSED = "s3a://tp2/processed"
CURATED   = "s3a://tp2/curated"

ventes = spark.read.parquet(f"{PROCESSED}/ventes_enrichies/")
print("Colonnes ventes_enrichies :", ventes.columns)


def indicateurs(df, dimension):
    """Nombre de ventes, quantité totale et chiffre d'affaires par dimension."""
    return (
        df.groupBy(dimension)
        .agg(
            count("id_vente").alias("nombre_ventes"),
            spark_sum("quantite").alias("quantite_totale"),
            spark_round(spark_sum("montant"), 2).alias("chiffre_affaires"),
        )
        .orderBy(col("chiffre_affaires").desc())
    )


# --- Analyse 1 : activité par ville -----------------------------------------
# Ville du magasin où la vente a eu lieu ; à défaut, ville du client.
if "ville_magasin" in ventes.columns:
    colonne_ville = "ville_magasin"
elif "ville_client" in ventes.columns:
    colonne_ville = "ville_client"
else:
    raise SystemExit("Aucune colonne ville_magasin / ville_client dans ventes_enrichies")
print(f"\nAnalyse 1 : ville utilisée = {colonne_ville}")

par_ville = indicateurs(
    ventes.withColumn("ville", coalesce(col(colonne_ville), lit("Inconnue"))),
    "ville",
)
par_ville.show(truncate=False)
par_ville.write.mode("overwrite").parquet(f"{CURATED}/activite_par_ville/")

# --- Analyse 2 : activité par catégorie de produits -------------------------
par_categorie = indicateurs(
    ventes.withColumn("categorie", coalesce(col("categorie"), lit("Inconnu"))),
    "categorie",
)
par_categorie.show(truncate=False)
par_categorie.write.mode("overwrite").parquet(f"{CURATED}/activite_par_categorie/")

print("\nZone CURATED construite :")
print(f"  {CURATED}/activite_par_ville/")
print(f"  {CURATED}/activite_par_categorie/")

spark.stop()
