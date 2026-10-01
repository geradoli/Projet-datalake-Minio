from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, current_timestamp
import json

# ---------------------------------------------------------------------------
# Clés de jointure
#   VENTES.id_client  -> CLIENTS.id_client
#   VENTES.id_produit -> PRODUITS.id_produit
#   VENTES.id_magasin -> MAGASINS.id_magasin
# ---------------------------------------------------------------------------

with open("credentials.json", "r") as f:
    creds = json.load(f)

MINIO_URL  = creds["url"]
ACCESS_KEY = creds["accessKey"]
SECRET_KEY = creds["secretKey"]

spark = (
    SparkSession.builder
    .appName("PROCESSED_ventes_enrichies")
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

# --- Lecture des tables PROCESSED -------------------------------------------
ventes   = spark.read.parquet(f"{PROCESSED}/ventes/")
clients  = spark.read.parquet(f"{PROCESSED}/clients/")
produits = spark.read.parquet(f"{PROCESSED}/produits/")
magasins = spark.read.parquet(f"{PROCESSED}/magasins/")

print("Colonnes ventes   :", ventes.columns)
print("Colonnes clients  :", clients.columns)
print("Colonnes produits :", produits.columns)
print("Colonnes magasins :", magasins.columns)

# --- Préparation : colonnes utiles, noms non ambigus -------------------------
ventes = ventes.drop("date_traitement")

clients = clients.select(
    "id_client",
    col("nom").alias("nom_client"),
    "segment",
    col("ville").alias("ville_client"),
)

produits = produits.select(
    "id_produit",
    "nom_produit",
    "categorie",
    "prix_unitaire",
)

# Les colonnes de magasins dépendent du fichier Excel : on les préfixe
# automatiquement (ville -> ville_magasin, nom -> nom_magasin, ...).
magasins_cols = []
for c in magasins.columns:
    if c in ("id_magasin", "date_traitement"):
        continue
    new_name = c if "magasin" in c else f"{c}_magasin"
    magasins_cols.append(col(c).alias(new_name))
magasins = magasins.select("id_magasin", *magasins_cols)

# --- Jointures (left join : on conserve toutes les ventes) -------------------
enrichies = (
    ventes
    .join(clients,  on="id_client",  how="left")
    .join(produits, on="id_produit", how="left")
    .join(magasins, on="id_magasin", how="left")
    # montant = quantite x prix_unitaire
    .withColumn("montant", spark_round(col("quantite") * col("prix_unitaire"), 2))
    .withColumn("date_traitement", current_timestamp())
)

# Ordre lisible des colonnes
ordre = (
    ["id_vente", "date_vente",
     "id_client", "nom_client", "segment", "ville_client",
     "id_produit", "nom_produit", "categorie",
     "id_magasin"]
    + [c for c in magasins.columns if c != "id_magasin"]
    + ["quantite", "prix_unitaire", "montant", "date_traitement"]
)
enrichies = enrichies.select(*ordre)

# --- Contrôles qualité -------------------------------------------------------
n_ventes = ventes.count()
n_enrichies = enrichies.count()
print(f"\nLignes ventes : {n_ventes} | lignes enrichies : {n_enrichies}")
if n_ventes != n_enrichies:
    print("  [warn] le nombre de lignes diffère : doublons de clé dans une table de référence ?")

print("Ventes sans correspondance :")
print("  client  :", enrichies.filter(col("nom_client").isNull()).count())
print("  produit :", enrichies.filter(col("nom_produit").isNull()).count())
cols_magasin = [c for c in magasins.columns if c != "id_magasin"]
if cols_magasin:
    print("  magasin :", enrichies.filter(col(cols_magasin[0]).isNull()).count())

enrichies.show(10, truncate=False)

# --- Écriture dans PROCESSED -------------------------------------------------
enrichies.write.mode("overwrite").parquet(f"{PROCESSED}/ventes_enrichies/")
print("\nVentes enrichies écrites dans s3a://tp2/processed/ventes_enrichies/")

spark.stop()
