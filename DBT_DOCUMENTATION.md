# Documentation dbt-duckdb — Couche Gold Spotify

## Table des matières

1. [Pourquoi dbt ?](#1-pourquoi-dbt-)
2. [Architecture du pipeline](#2-architecture-du-pipeline)
3. [Installation et mise en place](#3-installation-et-mise-en-place)
4. [Problèmes rencontrés et solutions](#4-problèmes-rencontrés-et-solutions)
5. [Structure du projet dbt](#5-structure-du-projet-dbt)
6. [Les modèles en détail](#6-les-modèles-en-détail)
7. [Les tests](#7-les-tests)
8. [Commandes utiles](#8-commandes-utiles)
9. [Résultats obtenus](#9-résultats-obtenus)

---

## 1. Pourquoi dbt ?

### Le problème avant dbt

Le dashboard Streamlit interrogeait directement les vues Silver `v_tracks` (66M lignes) et `v_playlists` (1M lignes) à chaque chargement de page. Chaque graphique recalculait les agrégations à la volée sur la totalité du dataset, ce qui était lent et non reproductible.

### Ce que dbt apporte

| Besoin | Solution dbt |
|---|---|
| Agrégations pré-calculées | Tables Gold matérialisées dans DuckDB |
| Reproductibilité | Chaque modèle est un fichier SQL versionnable |
| Qualité des données | Tests automatiques à chaque `dbt run` |
| Traçabilité | DAG de dépendances entre modèles |
| Séparation des responsabilités | Silver = nettoyage, Gold = agrégation |

### Le principe de dbt en une phrase

> dbt prend du SQL ordinaire, le transforme en un DAG de dépendances, et matérialise chaque nœud comme une vue ou une table dans la base de données cible.

---

## 2. Architecture du pipeline

```
  [Bronze] JSON bruts (Spotify MPD)
  data/*.json (1 000 fichiers)
        │
        ▼  conversionV3.py (Bronze → Silver)
        │
  [Silver] Fichiers Parquet
  outputParquetV3/*.parquet (2 000 fichiers)
        │
        ▼  vues DuckDB (lecture des Parquet)
        │  v_tracks     (66 346 428 lignes)
        │  v_playlists  (1 000 000 lignes)
        │
        ▼  dbt Silver — nettoyage + colonnes calculées
        │  silver_tracks
        │  silver_playlists
        │
        ▼
  [Gold] tables matérialisées — agrégations (dbt)
  gold_top_artists       gold_top_tracks
  gold_playlist_stats    gold_duration_analysis
  gold_position_patterns
        │
        ▼
  [Dashboard] dashboard.py (Streamlit)
  lit les tables Gold → requêtes instantanées
```

---

## 3. Installation et mise en place

### Environnement

Le projet utilise un venv géré par **uv** situé dans `/home/snowgou/EPSI/M1/ETL&ELT/code/.venv`.

```bash
# Installation de dbt-duckdb dans le venv uv
cd "/home/snowgou/EPSI/M1/ETL&ELT/code"
uv pip install dbt-duckdb
```

Version installée : **dbt 1.11.9** avec l'adaptateur **dbt-duckdb 1.10.1**.

### Initialisation du projet

Le projet dbt a été créé manuellement (sans `dbt init`) pour avoir un contrôle total sur la structure. Les fichiers clés sont :

```
dbt_spotify/
├── profiles.yml      ← connexion DuckDB (NE PAS versionner en prod)
└── dbt_project.yml   ← config du projet (matérialisations, chemins)
```

### Lancer dbt

Toujours exécuter depuis le dossier `dbt_spotify/` et passer `--profiles-dir .` car `profiles.yml` est dans le projet (pas dans `~/.dbt/`) :

```bash
cd "/home/snowgou/EPSI/M1/ETL&ELT/code/varianteFour/dbt_spotify"

# Activer le venv (fish shell)
source /home/snowgou/EPSI/M1/ETL&ELT/code/.venv/bin/activate.fish

# Vérifier la connexion
dbt debug --profiles-dir .

# Compiler le SQL sans exécuter
dbt compile --profiles-dir .

# Créer tous les modèles
dbt run --profiles-dir .

# Lancer les tests
dbt test --profiles-dir .

# Tout en une commande (run + test)
dbt build --profiles-dir .
```

---

## 4. Problèmes rencontrés et solutions

### Problème 1 — Chemins relatifs dans les vues DuckDB

**Symptôme** : `IO Error: No files found that match the pattern "./output_demo/*_tracks.parquet"`

**Cause** : Les vues Silver originales (`v_tracks`, `v_playlists`) avaient été créées avec des chemins relatifs pointant vers `./output_demo/`. DuckDB résout ces chemins par rapport au **répertoire de travail au moment de l'exécution**. Quand dbt lance les requêtes depuis `dbt_spotify/`, il cherche `dbt_spotify/output_demo/` qui n'existe pas.

**Solution** : Recréer les vues avec des **chemins absolus** pointant vers `outputParquetV3/` :

```python
import duckdb
con = duckdb.connect('spotify_stream.duckdb')
base = '/home/snowgou/EPSI/M1/ETL&ELT/code/varianteFour'

con.execute("DROP VIEW IF EXISTS v_tracks")
con.execute(f"""
    CREATE VIEW v_tracks AS
    SELECT pid AS playlist_id, track_uri, track_name, artist_name,
           artist_uri, album_name, album_uri, duration_ms, pos AS position
    FROM read_parquet('{base}/outputParquetV3/*_tracks.parquet')
""")
```

### Problème 2 — Noms de colonnes différents du schéma prévu

**Symptôme** : `Binder Error: Referenced column "playlist_id" not found. Candidate bindings: "pid"`

**Cause** : Les fichiers Parquet utilisent des noms courts issus du JSON source (`pid`, `pos`, `name`) alors que le CLAUDE.md décrit le schéma avec des noms longs (`playlist_id`, `position`, `playlist_name`).

**Schéma réel des parquets** :

| Parquet `_tracks` | Parquet `_playlists` |
|---|---|
| `pid` → playlist_id | `pid` → playlist_id |
| `pos` → position | `name` → playlist_name |
| track_uri, track_name | num_tracks, num_albums |
| artist_name, artist_uri | num_followers, modified_at |
| album_name, album_uri | |
| duration_ms | |

**Solution** : Intégrer les alias directement dans la définition des vues Silver (voir Problème 1 ci-dessus — les deux corrections ont été faites en même temps).

### Problème 3 — OOM (Out of Memory) avec 4 threads en parallèle

**Symptôme** : Exit code 137 (SIGKILL) pendant `dbt build` sur les tables Gold.

**Cause** : DuckDB avec `threads: 4` exécute 4 modèles Gold simultanément. Sur 66M lignes, `gold_top_artists` (GROUP BY + COUNT DISTINCT) et `gold_playlist_stats` (JOIN 1M × 66M) consomment plusieurs GB de RAM en parallèle.

**Solution** : Réduire à `threads: 1` dans `profiles.yml`. Les 5 tables Gold se créent alors séquentiellement en ~3 minutes, sans saturer la mémoire.

```yaml
# profiles.yml
dbt_spotify:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ../spotify_stream.duckdb
      threads: 1   # ← critique sur gros datasets
```

---

## 5. Structure du projet dbt

```
dbt_spotify/
├── profiles.yml              ← connexion DuckDB (threads: 1)
├── dbt_project.yml           ← config: silver=view, gold=table
├── models/
│   ├── sources.yml           ← déclare v_tracks et v_playlists
│   ├── silver/
│   │   ├── silver_tracks.sql    (vue)
│   │   └── silver_playlists.sql (vue)
│   └── gold/
│       ├── gold_top_artists.sql       (table)
│       ├── gold_top_tracks.sql        (table)
│       ├── gold_playlist_stats.sql    (table)
│       ├── gold_duration_analysis.sql (table)
│       └── gold_position_patterns.sql (table)
├── tests/
│   ├── generic/              ← vide (tests built-in via sources.yml)
│   └── singular/
│       ├── test_duration_positive.sql
│       └── test_followers_non_negative.sql
├── macros/                   ← vide
└── target/                   ← généré par dbt (ne pas versionner)
```

### `dbt_project.yml` — matérialisations

```yaml
models:
  dbt_spotify:
    silver:
      +materialized: view    # pas de stockage, recalcul à la volée
    gold:
      +materialized: table   # stocké physiquement → requêtes rapides
```

### `profiles.yml` — connexion

```yaml
dbt_spotify:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ../spotify_stream.duckdb   # relatif à dbt_spotify/
      threads: 1
```

---

## 6. Les modèles en détail

### Silver — Nettoyage et enrichissement

#### `silver_tracks`
Source : `v_tracks` (66M lignes)
Apports :
- `TRIM()` sur les champs texte
- `duration_min` = duration_ms / 60000 (plus lisible)
- `duration_category` : short / standard / long / very_long
- Filtre : `track_uri IS NOT NULL AND duration_ms > 0`

#### `silver_playlists`
Source : `v_playlists` (1M lignes)
Apports :
- `album_diversity_ratio` = num_albums / num_tracks
- `popularity_tier` : no_followers / small / medium / large / viral
- `size_category` : mini / standard / large / mega

### Gold — Agrégations prêtes pour le dashboard

#### `gold_top_artists`
Agrège silver_tracks par `(artist_name, artist_uri)`.
Colonnes clés : `nb_playlists`, `nb_track_appearances`, `nb_unique_tracks`, `avg_duration_min`, `avg_position`.

#### `gold_top_tracks`
Agrège silver_tracks par `(track_uri, track_name, artist_name, album_name, duration_min, duration_category)`.
Colonnes clés : `occurrences`, `nb_playlists`, `avg_position`, `best_position`.

#### `gold_playlist_stats`
JOIN silver_playlists × silver_tracks.
Enrichit chaque playlist avec : `avg_track_duration_min`, `nb_distinct_artists`.
Hérite de : `popularity_tier`, `size_category`, `album_diversity_ratio`.

#### `gold_duration_analysis`
Histogramme pré-calculé par buckets de 30 secondes.
Colonnes : `bucket_sec`, `bucket_min`, `duration_category`, `nb_tracks`, `nb_artists`.

#### `gold_position_patterns`
Statistiques par position (0–50) dans la playlist.
Colonnes : `unique_tracks`, `unique_artists`, `avg_duration_min`, `total_appearances`.

---

## 7. Les tests

### Tests sur les sources (sources.yml)

| Test | Colonne | Résultat |
|---|---|---|
| not_null | v_tracks.playlist_id | PASS |
| not_null | v_tracks.track_uri | PASS |
| not_null | v_tracks.duration_ms | PASS |
| not_null | v_playlists.playlist_id | PASS |
| unique | v_playlists.playlist_id | PASS |
| not_null | v_playlists.num_followers | PASS |

### Tests singuliers (tests/singular/)

| Fichier | Ce qu'il vérifie | Résultat |
|---|---|---|
| test_duration_positive.sql | Aucune durée ≤ 0 dans silver_tracks | PASS |
| test_followers_non_negative.sql | Aucun num_followers < 0 dans silver_playlists | PASS |

**Total : 8/8 tests PASS**

---

## 8. Commandes utiles

```bash
# Recréer uniquement la couche Gold
dbt run --select gold --profiles-dir .

# Recréer un seul modèle
dbt run --select gold_top_artists --profiles-dir .

# Tester un seul modèle
dbt test --select silver_tracks --profiles-dir .

# Voir le DAG de dépendances (dans le terminal)
dbt ls --profiles-dir .

# Régénérer après ajout de nouveaux Parquet
dbt run --profiles-dir .   # silver est une vue → lit les nouveaux fichiers automatiquement

# En cas de problème de cache dbt
rm -rf target/
dbt run --profiles-dir .
```

---

## 9. Résultats obtenus

### Tables Gold dans spotify_stream.duckdb

| Table | Nb lignes | Temps de création |
|---|---|---|
| gold_duration_analysis | ~400 buckets | 3.6s |
| gold_position_patterns | 50 lignes | 5.7s |
| gold_playlist_stats | ~1M lignes | 10.5s |
| gold_top_artists | ~hundreds k | 45s |
| gold_top_tracks | ~millions | 107s |

**Temps total** : ~3 minutes (séquentiel, 1 thread)

### Gain pour le dashboard

Avant : chaque requête du dashboard parcourait 66M lignes.  
Après : le dashboard lit des tables pré-agrégées → **requêtes < 100ms**.
