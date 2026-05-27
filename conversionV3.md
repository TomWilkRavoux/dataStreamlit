# conversionV3.py — Documentation

## Vue d'ensemble

Script de conversion ETL qui transforme **1 000 fichiers JSON** (32 GB au total) en **2 000 fichiers Parquet** compressés. Chaque fichier JSON source produit deux fichiers Parquet : un pour les playlists, un pour les tracks.

```
data/
  mpd.slice.0-999.json        (≈ 32 MB)
  mpd.slice.1000-1999.json
  ...  × 1000

outputParquetV3Test/
  mpd.slice.0-999_playlists.parquet
  mpd.slice.0-999_tracks.parquet
  mpd.slice.1000-1999_playlists.parquet
  mpd.slice.1000-1999_tracks.parquet
  ...  × 2000
```

---

## Structure des données source

Chaque fichier JSON suit ce schéma :

```json
{
  "info": { ... },
  "playlists": [
    {
      "pid": 0,
      "name": "...",
      "collaborative": "false",
      "modified_at": 1614000000,
      "num_tracks": 52,
      "num_albums": 40,
      "num_followers": 1,
      "num_edits": 6,
      "duration_ms": 11200000,
      "num_artists": 34,
      "tracks": [
        {
          "pos": 0,
          "artist_name": "...",
          "track_uri": "spotify:track:...",
          "artist_uri": "spotify:artist:...",
          "track_name": "...",
          "album_uri": "spotify:album:...",
          "duration_ms": 215000,
          "album_name": "..."
        },
        ...
      ]
    },
    ...  × 1000 playlists par fichier
  ]
}
```

Les tracks sont **imbriquées** dans chaque playlist. Le script les aplatit en deux tables normalisées reliées par `pid`.

---

## Pipeline de traitement

```
┌─────────────────────────────────────────────────────────────────┐
│  main()                                                         │
│                                                                 │
│  1. Découverte des 1000 fichiers JSON                           │
│  2. Soumission de toutes les tâches au ProcessPoolExecutor      │
│  3. Collecte des résultats au fil de leur complétion            │
└──────────────────────┬──────────────────────────────────────────┘
                       │  8 workers en parallèle
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
   process_file()  process_file()  ...   (×8 simultanément)
         │
         ├─ 1. Lecture du fichier JSON en mode binaire (rb)
         │
         ├─ 2. Parsing orjson → dict Python
         │
         ├─ 3. Aplatissement en deux structures column-oriented
         │       playlists : dict{ colonne → liste de valeurs }
         │       tracks    : dict{ colonne → liste de valeurs }
         │
         ├─ 4. Création des DataFrames Polars
         │
         ├─ 5. Écriture des deux fichiers Parquet (compression lz4)
         │
         └─ 6. Retour du stem (nom du fichier) → main
```

---

## Choix techniques et justifications

### 1. `ProcessPoolExecutor` — parallélisme multi-process

**Pourquoi pas `ThreadPoolExecutor` ?**
Python possède le GIL (*Global Interpreter Lock*) qui empêche deux threads d'exécuter du bytecode Python simultanément. Le traitement de chaque fichier est **CPU-bound** (parsing JSON, construction de DataFrame) : les threads se bloqueraient mutuellement. Les processus, eux, ont chacun leur propre interpréteur et leur propre GIL, donc ils s'exécutent vraiment en parallèle.

**Pourquoi 8 workers ?**
Un benchmark a été effectué sur 80 fichiers représentatifs pour trouver le sweet spot :

| Workers | Temps mesuré | Extrapolation 1000 fichiers |
|---------|-------------|----------------------------|
| 4       | 4.9s        | ~61s                       |
| 6       | 3.7s        | ~46s                       |
| **8**   | **3.6s**    | **~45s** ✓                 |
| 10      | 3.6s        | ~45s                       |
| 12      | 3.7s        | ~46s (plus lent)           |

Au-delà de 8 workers, les processus entrent en **contention sur les I/O disque** : plusieurs processus lisent des fichiers de 32 MB simultanément et saturent la bande passante. Ajouter des workers ne sert à rien quand le goulot est le disque.

### 2. `orjson` — parsing JSON rapide

**Pourquoi pas le module `json` de la stdlib ?**
Le module `json` standard de Python est écrit partiellement en C mais reste limité. `orjson` est une librairie Rust compilée, optimisée pour la désérialisation :

| Parser  | Temps / fichier (32 MB) |
|---------|------------------------|
| `json`  | ~140 ms                |
| `orjson`| ~110 ms                |

**Pourquoi `open(path, "rb")` et non `path.read_bytes()` ?**
`path.read_bytes()` alloue un objet `bytes` de 32 MB en mémoire *avant* de le passer à orjson. `open(rb)` + `f.read()` utilise l'I/O bufférisé du système d'exploitation, plus efficace en pratique. Avec 8 workers en parallèle, la différence en pression mémoire est mesurable (~117ms vs ~110ms).

### 3. Structure column-oriented pour Polars

**Pourquoi pas une liste de dicts `[{"pid": 1, ...}, ...]` ?**
Polars est un moteur **colonnaire** : il stocke les données colonne par colonne en mémoire (comme Arrow). Lui fournir une liste de dicts (format *row-oriented*) force Polars à faire une transposition coûteuse via `from_dicts`.

En construisant directement un `dict` de listes :
```python
# Lent (row-oriented) :
rows = [{"pid": 1, "name": "a"}, {"pid": 2, "name": "b"}]
pl.DataFrame(rows)  # → Polars doit transposer

# Rapide (column-oriented) :
cols = {"pid": [1, 2], "name": ["a", "b"]}
pl.DataFrame(cols)  # → Polars consomme directement
```
Le profiling cProfile avait identifié `from_dicts` comme responsable de **27% du temps total** dans les versions antérieures.

### 4. Traitement dans les workers, pas dans le main

**Pourquoi le main process ne fait-il que dispatcher ?**
Dans les versions précédentes, les workers parsaient le JSON et renvoyaient les données au main process, qui construisait ensuite les DataFrames et écrivait les fichiers. Cela créait deux problèmes :

- **Sérialisation pickle** : les listes de dicts (~70 000 tracks par fichier) devaient traverser la frontière inter-process via pickle à chaque fichier → overhead IPC massif.
- **Goulot d'étranglement** : le main process écrivait les fichiers en série pendant que les workers attendaient.

En déplaçant `pl.DataFrame(...).write_parquet(...)` dans `process_file`, chaque worker écrit **lui-même** sur disque. Le main process reçoit uniquement un `str` (le nom du fichier, ~30 bytes). Zéro pickle de données volumineuses.

### 5. Compression `lz4`

**Pourquoi pas `snappy` (le défaut courant) ?**
Benchmark sur un fichier tracks représentatif (67 503 lignes) :

| Compression   | Temps d'écriture | Taille  |
|---------------|-----------------|---------|
| `uncompressed`| 12.5 ms         | 4.9 MB  |
| **`lz4`**     | **11.6 ms**     | 3.5 MB  |
| `snappy`      | 12.3 ms         | 3.4 MB  |
| `zstd`        | 17.5 ms         | 2.6 MB  |

`lz4` offre le meilleur équilibre vitesse/taille : légèrement plus rapide que snappy avec un fichier résultant de taille comparable.

### 6. Un seul répertoire de sortie plat

Les fichiers de sortie utilisent le suffixe `_playlists` / `_tracks` pour garder la correspondance avec le fichier source, sans créer deux sous-répertoires :

```
mpd.slice.0-999_playlists.parquet   ← playlists du fichier 0-999
mpd.slice.0-999_tracks.parquet      ← tracks du fichier 0-999
```

Cela simplifie la lecture ultérieure avec Polars ou DuckDB :
```python
# Lire toutes les tracks en une seule opération lazy
df = pl.scan_parquet("outputParquetV3Test/*_tracks.parquet")
```

---

## Dépendances

```
polars    # DataFrames colonnaires + écriture Parquet
orjson    # Parsing JSON rapide (Rust)
```

Installation :
```bash
pip install polars orjson
```

---

## Utilisation

```bash
python varianteThree/conversionV3.py
```

Sortie attendue :
```
Found 1000 JSON files — 8 workers
  100/1000 files written...
  200/1000 files written...
  ...
Done: 2000 parquet files
Done in 73.0s
```
