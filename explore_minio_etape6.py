"""
TP2 - Etape 6 : exploration de MinIO avec Python (API compatible S3).

Le programme :
  1. se connecte a MinIO (credentials lus depuis l'environnement / .env) ;
  2. verifie l'acces au bucket tp2 ;
  3. liste les objets et les regroupe par zone (raw / processed / curated) ;
  4. affiche les prefixes (pseudo-dossiers) de chaque zone ;
  5. recupere les metadonnees de quelques objets (head_object) ;
  6. verifie la lecture dans RAW (lecture des premieres lignes du CSV).

Aucun chemin Windows n'est utilise : tout passe par des cles S3 (ex. raw/ventes/ventes.csv).
Aucune ecriture n'est faite ici.

Configuration (fichier credentials.json a la racine, NON livre dans le ZIP) :
  {
    "url": "http://127.0.0.1:9000",
    "accessKey": "...",
    "secretKey": "...",
    "api": "s3v4",
    "path": "auto"
  }

Le bucket utilise par le TP est : tp2.
"""

import json
import sys
from pathlib import Path
from collections import defaultdict

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

ZONES = ["raw", "processed", "curated"]
SAMPLE_KEY = "raw/ventes/ventes.csv"

# Sources attendues dans RAW (cles S3 exactes, pas de chemin Windows)
EXPECTED_RAW_FILES = [
    "raw/ventes/ventes.csv",
    "raw/clients/clients.json",
    "raw/produits/produits.xml",
    "raw/magasins/magasins.xlsx",
]
IMAGES_PREFIX = "raw/images_produits/"


def get_config():
    """Lit les credentials MinIO depuis credentials.json."""
    credentials_path = Path(__file__).with_name("credentials.json")

    if not credentials_path.exists():
        sys.exit(
            f"[ERREUR] Fichier credentials.json introuvable : {credentials_path}"
        )

    try:
        with credentials_path.open("r", encoding="utf-8") as f:
            credentials = json.load(f)
    except json.JSONDecodeError as e:
        sys.exit(f"[ERREUR] credentials.json n'est pas un JSON valide : {e}")
    except OSError as e:
        sys.exit(f"[ERREUR] Impossible de lire credentials.json : {e}")

    required = ("url", "accessKey", "secretKey")
    missing = [key for key in required if not credentials.get(key)]

    if missing:
        sys.exit(
            "[ERREUR] Champs manquants dans credentials.json : "
            + ", ".join(missing)
        )

    return {
        "endpoint": credentials["url"],
        "access_key": credentials["accessKey"],
        "secret_key": credentials["secretKey"],
        "bucket": "tp2",
    }


def get_client(cfg):
    """Cree un client S3 pointe vers MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=cfg["endpoint"],
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name="us-east-1",  # valeur arbitraire, MinIO l'ignore
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def check_bucket(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"[OK] Bucket '{bucket}' accessible.")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        sys.exit(f"[ERREUR] Acces au bucket '{bucket}' impossible (code {code}).")


def list_all_objects(s3, bucket, prefix=""):
    """Liste tous les objets (pagination geree)."""
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


def zone_of(key):
    """La zone est le premier segment de la cle (ex. 'raw' pour raw/ventes/ventes.csv)."""
    first = key.split("/", 1)[0]
    return first if first in ZONES else "autre"


def human_size(n):
    for unit in ("o", "Ko", "Mo", "Go"):
        if n < 1024 or unit == "Go":
            return f"{n:.0f} {unit}" if unit == "o" else f"{n:.1f} {unit}"
        n /= 1024


def show_objects_by_zone(objects):
    by_zone = defaultdict(list)
    for obj in objects:
        by_zone[zone_of(obj["Key"])].append(obj)

    print("\n=== Objets par zone ===")
    for zone in ZONES + ["autre"]:
        objs = by_zone.get(zone, [])
        if not objs and zone == "autre":
            continue
        total = sum(o["Size"] for o in objs)
        print(f"- {zone:<10} : {len(objs):>4} objet(s), {human_size(total)}")
    return by_zone


def show_prefixes(s3, bucket):
    """Affiche les prefixes de 1er et 2e niveau (les 'dossiers' logiques)."""
    print("\n=== Prefixes (pseudo-dossiers) ===")
    for zone in ZONES:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{zone}/", Delimiter="/")
        subs = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        print(f"{zone}/")
        if not subs:
            print("   (aucun sous-prefixe)")
        for sp in subs:
            print(f"   {sp}")


def show_object_info(s3, bucket, keys):
    print("\n=== Informations sur quelques objets (head_object) ===")
    for key in keys:
        try:
            h = s3.head_object(Bucket=bucket, Key=key)
            print(f"Cle            : {key}")
            print(f"  Taille       : {human_size(h['ContentLength'])}")
            print(f"  Content-Type : {h.get('ContentType')}")
            print(f"  Modifie le   : {h['LastModified']}")
            print(f"  ETag         : {h['ETag']}")
        except ClientError as e:
            print(f"Cle {key} : erreur ({e.response['Error']['Code']})")


def pick_samples(by_zone):
    """Un objet par sous-prefixe de RAW (csv, json, xml, xlsx, image...)."""
    seen, samples = set(), []
    for obj in by_zone.get("raw", []):
        parts = obj["Key"].split("/")
        sub = parts[1] if len(parts) > 2 else obj["Key"]
        if sub not in seen:
            seen.add(sub)
            samples.append(obj["Key"])
    return samples


def check_raw_read(s3, bucket, key=SAMPLE_KEY, n_lines=5):
    """Teste la lecture dans RAW : on ne telecharge que les premiers octets (Range)."""
    print(f"\n=== Verification de l'acces en lecture a RAW ({key}) ===")
    try:
        resp = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-2047")
        text = resp["Body"].read().decode("utf-8", errors="replace")
        lines = text.splitlines()[:n_lines]
        print("[OK] Lecture autorisee. Premieres lignes :")
        for line in lines:
            print(f"   {line}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            print(f"[ATTENTION] L'objet {key} n'existe pas encore (RAW non alimentee ?).")
        else:
            print(f"[ERREUR] Lecture refusee ou impossible (code {code}).")


def check_raw_sources(s3, bucket):
    """Verifie la presence de chaque source attendue dans RAW."""
    print("\n=== Sources attendues dans RAW ===")
    for key in EXPECTED_RAW_FILES:
        try:
            h = s3.head_object(Bucket=bucket, Key=key)
            print(f"[OK]       {key} ({human_size(h['ContentLength'])})")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            label = "ABSENT" if code in ("404", "NoSuchKey", "NotFound") else f"ERREUR {code}"
            print(f"[{label}] {key}")

    images = [
        o for o in list_all_objects(s3, bucket, IMAGES_PREFIX)
        if not o["Key"].endswith("/")
    ]
    exts = sorted({o["Key"].rsplit(".", 1)[-1].lower() for o in images if "." in o["Key"]})
    print(f"[{'OK' if images else 'ABSENT'}]       {IMAGES_PREFIX} : {len(images)} image(s) {exts}")


def main():
    cfg = get_config()
    s3 = get_client(cfg)
    print(f"Connexion a {cfg['endpoint']} ...")

    try:
        check_bucket(s3, cfg["bucket"])
        objects = list_all_objects(s3, cfg["bucket"])
    except EndpointConnectionError:
        sys.exit("[ERREUR] MinIO injoignable : verifiez MINIO_ENDPOINT et que le serveur tourne.")

    print(f"Nombre total d'objets dans '{cfg['bucket']}' : {len(objects)}")
    by_zone = show_objects_by_zone(objects)
    show_prefixes(s3, cfg["bucket"])
    show_object_info(s3, cfg["bucket"], pick_samples(by_zone)[:5])
    check_raw_sources(s3, cfg["bucket"])
    check_raw_read(s3, cfg["bucket"])


if __name__ == "__main__":
    main()
