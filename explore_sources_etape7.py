"""
TP2 - Etape 7 : exploration des sources de RAW (aucune correction, aucune ecriture).

Pour chaque source (CSV, JSON, XML, Excel) le programme lit l'objet directement dans MinIO
(cle S3, en memoire, sans chemin Windows) puis affiche :
  - format et schema, colonnes et types (dtype pandas + type infere sur les valeurs) ;
  - nombre d'enregistrements ;
  - valeurs manquantes (vides, espaces, 'null' textuels) ;
  - doublons (lignes identiques, doublons de cle, doublons apres normalisation) ;
  - valeurs incoherentes : espaces superflus, variantes de casse, nombres invalides,
    negatifs / zeros / valeurs extremes, dates invalides / futures / formats multiples ;
  - coherence referentielle ventes -> clients / produits / magasins ;
  - images : inventaire et correspondance avec les produits.
Un rapport texte est enregistre (par defaut exploration_report.txt) pour alimenter le README.

Usage :  python src/explore_sources.py [--out exploration_report.txt]
"""

import argparse
import csv
import io
import json
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter

import pandas as pd

SOURCES = [  # (nom, cle S3, format)
    ("ventes", "raw/ventes/ventes.csv", "csv"),
    ("clients", "raw/clients/clients.json", "json"),
    ("produits", "raw/produits/produits.xml", "xml"),
    ("magasins", "raw/magasins/magasins.xlsx", "xlsx"),
]
IMAGES_PREFIX = "raw/images_produits/"
NULL_TOKENS = {"null", "none", "nan", "n/a", "na", "nd", "-", "?", "#n/a", "#null"}
DATE_RE = re.compile(r"^\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}")
MAX_EX = 5
ID_RE = re.compile(r"(^id$|^id[_ .-]|[_ .-]id$|^code|^ref|^n[o°]$)")


# ----------------------------------------------------------------------------- rapport
class Report:
    def __init__(self):
        self.lines = []

    def p(self, text=""):
        print(text)
        self.lines.append(text)

    def h(self, text, level=1):
        self.p()
        self.p("#" * level + " " + text)

    def save(self, path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(self.lines) + "\n")


def examples(values, n=MAX_EX):
    vals = list(dict.fromkeys(str(v) for v in values))[:n]
    return ", ".join(repr(v) for v in vals)


# ----------------------------------------------------------------------------- chargeurs
def load_csv(data):
    info = []
    text, enc = None, None
    for enc in ("utf-8-sig", "latin-1"):
        try:
            text = data.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    info.append(f"Format : CSV | encodage : {enc}")
    try:
        delim = csv.Sniffer().sniff(text[:4096], delimiters=",;\t|").delimiter
    except csv.Error:
        delim = ","
    info.append(f"Separateur : {delim!r}")
    df = pd.read_csv(io.StringIO(text), sep=delim, dtype=str, keep_default_na=False,
                     on_bad_lines="skip")
    dtypes = {str(c): str(t) for c, t in
              pd.read_csv(io.StringIO(text), sep=delim, on_bad_lines="skip").dtypes.items()}
    n_lines = len([ln for ln in text.splitlines() if ln.strip()]) - 1
    if n_lines != len(df):
        info.append(f"ATTENTION : {n_lines} lignes de donnees dans le fichier pour {len(df)} "
                    f"enregistrements lus (lignes mal formees ? a verifier).")
    return [("", df, dtypes)], info


def load_json(data):
    text = data.decode("utf-8-sig")
    try:
        obj, fmt = json.loads(text), "JSON classique"
    except json.JSONDecodeError:
        obj, fmt = [json.loads(ln) for ln in text.splitlines() if ln.strip()], "JSON Lines"
    info = [f"Format : {fmt}"]
    if isinstance(obj, dict):
        lists = [(k, v) for k, v in obj.items() if isinstance(v, list)]
        if lists:
            key, obj = max(lists, key=lambda kv: len(kv[1]))
            info.append(f"Liste d'enregistrements trouvee sous la cle '{key}'")
        else:
            obj = [obj]
    recs = [r for r in obj if isinstance(r, dict)]
    total = len(recs)
    freq = Counter(k for r in recs for k in r)
    partial = {k: v for k, v in freq.items() if v < total}
    if partial:
        info.append("ATTENTION schema heterogene - cles absentes de certains objets : "
                    + ", ".join(f"{k} ({v}/{total})" for k, v in partial.items()))
    mixed = []
    for k in freq:
        types = {type(r[k]).__name__ for r in recs if k in r and r[k] is not None}
        if len(types) > 1:
            mixed.append(f"{k} {sorted(types)}")
    if mixed:
        info.append("ATTENTION types JSON melanges pour une meme cle : " + "; ".join(mixed))
    nested = [k for k in freq if any(isinstance(r.get(k), (dict, list)) for r in recs)]
    if nested:
        info.append(f"Champs imbriques (aplatis avec '.') : {nested}")
    return [("", pd.json_normalize(recs), None)], info


def _local(tag):
    return tag.split("}")[-1]


def _flatten(el, prefix=""):
    out = {}

    def add(k, v):
        out[k] = out[k] + "|" + v if k in out else v

    for k, v in el.attrib.items():
        add(f"{prefix}@{_local(k)}", v)
    for child in el:
        name = f"{prefix}{_local(child.tag)}"
        if len(child) or child.attrib:
            for k, v in _flatten(child, name + ".").items():
                add(k, v)
            if child.text and child.text.strip():
                add(name, child.text.strip())
        else:
            add(name, (child.text or "").strip())
    return out


def load_xml(data):
    root = ET.fromstring(data)
    parent = {c: p for p in root.iter() for c in p}

    def depth(e):
        d = 0
        while e in parent:
            e, d = parent[e], d + 1
        return d

    counts = Counter(el.tag for el in root.iter() if el is not root)
    cands = [t for t, n in counts.items() if n > 1 and
             any((len(e) or e.attrib) for e in root.iter(t))]
    if cands:
        tag = min(cands, key=lambda t: (min(depth(e) for e in root.iter(t)), -counts[t]))
        elems = list(root.iter(tag))
    else:
        elems = list(root)
    info = [f"Format : XML | racine <{_local(root.tag)}> | "
            f"element d'enregistrement : <{_local(elems[0].tag) if elems else '?'}>"]
    recs = [_flatten(e) for e in elems]
    freq = Counter(k for r in recs for k in r)
    partial = {k: v for k, v in freq.items() if v < len(recs)}
    if partial:
        info.append("ATTENTION balises absentes de certains enregistrements : "
                    + ", ".join(f"{k} ({v}/{len(recs)})" for k, v in partial.items()))
    return [("", pd.DataFrame(recs), None)], info


def load_xlsx(data):
    sheets = pd.read_excel(io.BytesIO(data), sheet_name=None)
    info = [f"Format : Excel | feuilles : {list(sheets)}"]
    frames = []
    for name, df in sheets.items():
        unnamed = [c for c in df.columns if str(c).startswith("Unnamed")]
        if df.shape[1] and len(unnamed) >= df.shape[1] / 2:
            info.append(f"ATTENTION feuille '{name}' : en-tetes probablement absents ou "
                        f"decales (colonnes 'Unnamed').")
        frames.append((name, df, None))
    return frames, info


LOADERS = {"csv": load_csv, "json": load_json, "xml": load_xml, "xlsx": load_xlsx}


# ----------------------------------------------------------------------------- outils
def cell(v):
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def to_str_df(df):
    return df.apply(lambda col: col.map(cell)).astype(object)


def to_num(s):
    return pd.to_numeric(s.str.replace(r"\s", "", regex=True).str.replace(",", ".", regex=False),
                         errors="coerce")


def to_date(s):
    """ISO (AAAA-MM-JJ) lu tel quel ; les autres formats sont lus jour/mois/annee."""
    iso = s.str.match(r"^\d{4}-\d{1,2}-\d{1,2}")
    parts = []
    if iso.any():
        parts.append(pd.to_datetime(s[iso], format="ISO8601", errors="coerce"))
    if (~iso).any():
        other = s[~iso]
        try:
            parts.append(pd.to_datetime(other, errors="coerce", format="mixed", dayfirst=True))
        except (TypeError, ValueError):
            parts.append(pd.to_datetime(other, errors="coerce", dayfirst=True))
    return pd.concat(parts).reindex(s.index)


def infer_type(name, s):
    s = s.str.strip()
    if s.empty:
        return "vide"
    if s.str.fullmatch(r"[+-]?\d+").all():
        return "entier"
    num = to_num(s)
    ok_num = num.notna().mean()
    if ok_num == 1:
        return "decimal"
    if "date" in name.lower() or s.str.match(DATE_RE).mean() >= 0.8:
        if "date" in name.lower() or to_date(s).notna().mean() >= 0.8:
            return "date"
    if ok_num >= 0.8:
        return "numerique"
    return "texte"


def norm(s):
    return s.str.strip().str.upper()


# ----------------------------------------------------------------------------- profilage
def profile(label, df, rep, dtypes=None):
    rep.h(f"Source : {label}", 2)
    df = df.copy()
    an = []
    bad_names = [c for c in df.columns if str(c) != str(c).strip()]
    df.columns = [str(c) for c in df.columns]
    n = len(df)
    rep.p(f"Enregistrements : {n} | Colonnes : {df.shape[1]}")
    if bad_names:
        an.append(f"Noms de colonnes avec espaces superflus : {bad_names}")
    if n == 0:
        an.append("Aucun enregistrement")
        _print_anomalies(rep, an)
        return None

    sdf = to_str_df(df)
    miss = sdf.apply(lambda c: c.str.strip() == "")
    dt = dtypes or {c: str(t) for c, t in df.dtypes.items()}
    types = {}

    rep.p(f"\n{'colonne':<24}{'dtype pandas':<14}{'type infere':<13}{'manquants':<16}{'distincts'}")
    for c in sdf.columns:
        vals = sdf[c][~miss[c]]
        types[c] = infer_type(c, vals)
        nm = int(miss[c].sum())
        rep.p(f"{c:<24.23}{dt.get(c, '?'):<14.13}{types[c]:<13}"
              f"{nm:>5} ({nm / n:5.1%})   {vals.nunique():>8}")
    rep.p("\nApercu (3 premieres lignes) :")
    rep.p(df.head(3).to_string(max_colwidth=30))

    # --- valeurs manquantes
    for c in sdf.columns:
        nm = int(miss[c].sum())
        if nm:
            an.append(f"{c} : {nm} valeur(s) manquante(s) ({nm / n:.1%})")
        v = sdf[c][~miss[c]]
        nulls = v.str.strip().str.lower().isin(NULL_TOKENS)
        if nulls.any():
            an.append(f"{c} : {int(nulls.sum())} valeur(s) 'null' textuelle(s) ({examples(v[nulls])})")

    # --- doublons
    exact = int(sdf.duplicated().sum())
    if exact:
        an.append(f"{exact} ligne(s) entierement dupliquee(s)")
    normed = int(sdf.apply(lambda c: c.str.strip().str.lower()).duplicated().sum())
    if normed > exact:
        an.append(f"{normed - exact} doublon(s) supplementaire(s) apres normalisation (espaces/casse)")
    cands = [c for c in sdf.columns if ID_RE.search(c.lstrip("@").lower())]
    best = None
    for c in cands:
        v = sdf[c][~miss[c]]
        if len(v) and (best is None or v.nunique() / len(v) > best[1]):
            best = (c, v.nunique() / len(v))
    key = best[0] if best and best[1] >= 0.5 else None
    if key:
        rep.p(f"\nCle candidate detectee : '{key}'")
        k = sdf[key].str.strip()
        dup_rows = sdf[k.duplicated(keep=False) & ~miss[key]]
        if len(dup_rows):
            extra = int(k[~miss[key]].duplicated().sum())
            conflicts = int(dup_rows.drop_duplicates()[key].str.strip().duplicated().sum())
            an.append(f"Cle '{key}' : {extra} ligne(s) en trop pour des cles repetees "
                      f"({extra - conflicts} exactes, {conflicts} avec contenu different)")

    # --- espaces / casse
    for c in sdf.columns:
        v = sdf[c][~miss[c]]
        ws = (v != v.str.strip()) | v.str.contains(r"\s{2,}")
        if ws.any():
            an.append(f"{c} : {int(ws.sum())} valeur(s) avec espaces superflus ({examples(v[ws])})")
        if types[c] == "texte" and v.nunique() <= 1000:
            stripped = v.str.strip()
            grp = stripped.groupby(stripped.str.lower()).nunique()
            bad = grp[grp > 1]
            if len(bad):
                ex = [sorted(set(stripped[stripped.str.lower() == k]))for k in bad.index[:3]]
                an.append(f"{c} : {len(bad)} valeur(s) avec variantes de casse (ex. {ex})")

    # --- numeriques
    for c in sdf.columns:
        if types[c] not in ("entier", "decimal", "numerique"):
            continue
        v = sdf[c][~miss[c]]
        num = to_num(v)
        invalid = v[num.isna()]
        if len(invalid):
            an.append(f"{c} : {len(invalid)} valeur(s) numerique(s) invalide(s) ({examples(invalid)})")
        x = num.dropna()
        if x.empty:
            continue
        rep.p(f"\nStats {c} : min={x.min():g} max={x.max():g} moy={x.mean():.2f} "
              f"mediane={x.median():g}")
        if (x < 0).any():
            an.append(f"{c} : {int((x < 0).sum())} valeur(s) negative(s) (min {x.min():g})")
        if re.search(r"quant|prix|price|montant|amount|qty", c.lower()) and (x == 0).any():
            an.append(f"{c} : {int((x == 0).sum())} valeur(s) egale(s) a 0")
        q1, q3 = x.quantile(.25), x.quantile(.75)
        iqr = q3 - q1
        if iqr > 0:
            out = x[(x > q3 + 3 * iqr) | (x < q1 - 3 * iqr)]
            if len(out):
                an.append(f"{c} : {len(out)} valeur(s) extreme(s) suspecte(s) ({examples(out)})")

    # --- dates
    for c in sdf.columns:
        if types[c] != "date":
            continue
        v = sdf[c][~miss[c]].str.strip()
        d = to_date(v)
        if d.isna().any():
            an.append(f"{c} : {int(d.isna().sum())} date(s) invalide(s) ({examples(v[d.isna()])})")
        pats = Counter(re.sub(r"[A-Za-z]", "A", re.sub(r"\d", "9", x)) for x in v)
        if len(pats) > 1:
            an.append(f"{c} : formats de date multiples {dict(pats.most_common(4))}")
        if d.notna().any():
            rep.p(f"\nPlage de dates {c} : {d.min().date()} -> {d.max().date()}")
            fut = d[d > pd.Timestamp.now()]
            if len(fut):
                an.append(f"{c} : {len(fut)} date(s) dans le futur (max {fut.max().date()})")

    # --- e-mails
    for c in sdf.columns:
        if "mail" in c.lower():
            v = sdf[c][~miss[c]].str.strip()
            bad = v[~v.str.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+")]
            if len(bad):
                an.append(f"{c} : {len(bad)} adresse(s) invalide(s) ({examples(bad)})")

    # --- modalites des colonnes texte a faible cardinalite
    for c in sdf.columns:
        v = sdf[c][~miss[c]]
        if types[c] == "texte" and 1 < v.nunique() <= 12 and not ID_RE.search(c.lstrip("@").lower()):
            rep.p(f"\nModalites {c} : " + ", ".join(f"{k!r}x{n_}" for k, n_ in v.value_counts().items()))

    _print_anomalies(rep, an)
    return {"sdf": sdf, "miss": miss, "types": types, "key": key, "anomalies": an}


def _print_anomalies(rep, an):
    rep.p("\nAnomalies detectees :" if an else "\nAucune anomalie detectee automatiquement.")
    for a in an:
        rep.p(f"  - {a}")


# ----------------------------------------------------------------------------- croisements
def cross_check(profs, rep):
    rep.h("Coherence referentielle : ventes -> clients / produits / magasins", 2)
    an = []
    fact = profs.get("ventes")
    if fact is None:
        rep.p("Table ventes indisponible.")
        return an
    for dim in ("clients", "produits", "magasins"):
        d = profs.get(dim)
        if d is None:
            continue
        stem = dim.rstrip("s")
        best = None
        for fc in fact["sdf"].columns:
            fv = norm(fact["sdf"][fc][~fact["miss"][fc]])
            if fv.nunique() < 2:
                continue
            for dc in d["sdf"].columns:
                dv = norm(d["sdf"][dc][~d["miss"][dc]])
                if dv.empty or dv.nunique() / len(dv) < 0.5:
                    continue
                related = stem in fc.lower() or fc.lower() == dc.lower()
                if not related and fact["types"][fc] != "texte":
                    continue
                ov = fv.isin(set(dv)).mean()
                if ov >= 0.5 and (best is None or (related, ov) > best[0]):
                    best = ((related, ov), fc, dc)
        if best is None:
            rep.p(f"{dim} : aucune cle de jointure detectee automatiquement -> a identifier manuellement.")
            continue
        _, fc, dc = best
        rep.p(f"{dim} : jointure probable ventes.{fc} -> {dim}.{dc}")
        fv_raw = fact["sdf"][fc][~fact["miss"][fc]].str.strip()
        dv_raw = d["sdf"][dc][~d["miss"][dc]].str.strip()
        orph = fv_raw[~norm(fv_raw).isin(set(norm(dv_raw)))]
        orph_strict = fv_raw[~fv_raw.isin(set(dv_raw))]
        if len(orph):
            an.append(f"ventes.{fc} : {len(orph)} vente(s) sans correspondance dans {dim} "
                      f"({orph.nunique()} valeur(s) : {examples(orph)})")
        if len(orph_strict) > len(orph):
            an.append(f"ventes.{fc} : {len(orph_strict) - len(orph)} vente(s) ne correspondent "
                      f"a {dim}.{dc} qu'apres correction de casse/espaces")
        unused = set(norm(dv_raw)) - set(norm(fv_raw))
        rep.p(f"   {len(unused)} element(s) de {dim} sans aucune vente (information)")
    _print_anomalies(rep, an)
    return an


def check_images(images, profs, rep):
    rep.h("Images produits (conservees dans RAW, non traitees)", 2)
    an = []
    objs = [o for o in images if not o["Key"].endswith("/")]
    rep.p(f"Nombre d'images : {len(objs)}")
    if not objs:
        an.append("Aucune image trouvee sous " + IMAGES_PREFIX)
        _print_anomalies(rep, an)
        return an
    names = [o["Key"].rsplit("/", 1)[-1] for o in objs]
    exts = Counter(n.rsplit(".", 1)[-1].lower() if "." in n else "(aucune)" for n in names)
    rep.p(f"Extensions : {dict(exts)}")
    sizes = [o["Size"] for o in objs]
    rep.p(f"Tailles : min {min(sizes)} o, max {max(sizes)} o")
    if any(s == 0 for s in sizes):
        an.append(f"{sum(s == 0 for s in sizes)} image(s) vide(s) (0 octet)")
    odd = [n for n in names if n.rsplit(".", 1)[-1].lower() not in ("jpg", "jpeg", "png")]
    if odd:
        an.append(f"Fichiers qui ne sont pas des images JPG/PNG : {examples(odd)}")
    stems = {n.rsplit(".", 1)[0].strip().upper() for n in names}
    prod = profs.get("produits")
    if prod:
        best = None
        for c in prod["sdf"].columns:
            v = set(norm(prod["sdf"][c][~prod["miss"][c]]))
            ov = len(stems & v) / len(stems)
            if ov > 0 and (best is None or ov > best[0]):
                best = (ov, c, v)
        if best:
            _, c, v = best
            rep.p(f"Correspondance noms d'images <-> produits.{c}")
            if v - stems:
                an.append(f"{len(v - stems)} produit(s) sans image ({examples(sorted(v - stems))})")
            if stems - v:
                an.append(f"{len(stems - v)} image(s) sans produit ({examples(sorted(stems - v))})")
        else:
            rep.p("Aucune correspondance trouvee entre noms d'images et colonnes de produits.")
    _print_anomalies(rep, an)
    return an


# ----------------------------------------------------------------------------- orchestration
def run(datasets, images, rep):
    profs, summary = {}, {}
    for name, (key, fmt, data) in datasets.items():
        rep.h(f"{name.upper()}  ({key})", 1)
        rep.p(f"Taille de l'objet : {len(data)} octets")
        try:
            frames, info = LOADERS[fmt](data)
        except Exception as e:  # lecture impossible = anomalie en soi
            rep.p(f"ERREUR de lecture ({type(e).__name__}) : {e}")
            summary[name] = [f"Lecture impossible : {e}"]
            continue
        for line in info:
            rep.p(line)
        summary[name] = [i for i in info if i.startswith("ATTENTION")]
        for suffix, df, dtypes in frames:
            label = name if len(frames) == 1 else f"{name}[{suffix}]"
            res = profile(label, df, rep, dtypes)
            if res:
                summary.setdefault(label, []).extend(res["anomalies"])
                profs.setdefault(name, res)
    summary["coherence referentielle"] = cross_check(profs, rep)
    summary["images"] = check_images(images, profs, rep)

    rep.h("SYNTHESE DES ANOMALIES (a reporter dans le README)", 1)
    for label, an in summary.items():
        if an:
            rep.p(f"\n{label} :")
            for a in an:
                rep.p(f"  - {a}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="exploration_report.txt")
    args = ap.parse_args()

    from botocore.exceptions import ClientError
    import boto3

    # Lecture de la configuration MinIO depuis credentials.json.
    # Le fichier doit rester local et ne doit pas être inclus dans le ZIP du TP.
    credentials_path = "credentials.json"

    with open(credentials_path, "r", encoding="utf-8") as f:
        credentials = json.load(f)

    endpoint = credentials["url"]
    access_key = credentials["accessKey"]
    secret_key = credentials["secretKey"]
    bucket = "tp2"

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    def list_all_objects(s3, bucket, prefix):
        paginator = s3.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects.extend(page.get("Contents", []))
        return objects
    rep = Report()
    datasets = {}
    for name, key, fmt in SOURCES:
        try:
            datasets[name] = (key, fmt, s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        except ClientError as e:
            rep.p(f"[ERREUR] {key} illisible ({e.response['Error']['Code']})")
    images = list_all_objects(s3, bucket, IMAGES_PREFIX)
    run(datasets, images, rep)
    rep.save(args.out)
    print(f"\nRapport enregistre dans {args.out}")


if __name__ == "__main__":
    sys.exit(main())
