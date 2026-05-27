# CLAUDE.md — Objectif 2 : Couche Gold avec dbt-duckdb

## Contexte du projet

Projet ETL/ELT EPSI M1 — Pipeline Spotify Million Playlist Dataset.

### Architecture actuelle
```
Bronze  → /outputParquetV3/          (1000 JSON bruts, déjà faits)
Silver  → /outputParquetV3/          (2000 fichiers Parquet : playlists + tracks, déjà faits)
Gold    → spotify_stream.duckdb      (vues v_tracks et v_playlists, déjà faites)
Dashboard → dashboard.py             (Streamlit, déjà fait, 100%)
```

### Schéma des vues existantes
**v_tracks** : playlist_id (int), track_uri (varchar), track_name (varchar),
artist_name (varchar), artist_uri (varchar), album_name (varchar),
album_uri (varchar), duration_ms (bigint), position (int)

**v_playlists** : playlist_id (int), playlist_name (varchar), num_tracks (int),
num_albums (int), num_followers (int), modified_at (bigint)

---

## Objectif

Mettre en place **dbt-duckdb** pour construire une couche Gold robuste
par-dessus les fichiers Parquet Silver existants.

La couche Gold doit produire des tables agrégées et testées, prêtes
pour le dashboard Streamlit.

---

## Ce qu'il faut faire

### 1. Installer dbt-duckdb

```bash
pip install dbt-duckdb
```

### 2. Initialiser le projet dbt

```bash
dbt init dbt_spotify
```

Répondre aux prompts :
- database : `duckdb`
- fichier duckdb : `../spotify_stream.duckdb`  (chemin relatif depuis dbt_spotify/)

### 3. Créer la structure de fichiers suivante

```
dbt_spotify/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── sources.yml               ← déclare v_tracks et v_playlists comme sources
│   ├── silver/
│   │   ├── silver_tracks.sql     ← nettoyage et typage des tracks
│   │   └── silver_playlists.sql  ← nettoyage et typage des playlists
│   └── gold/
│       ├── gold_top_artists.sql       ← top artistes agrégés
│       ├── gold_top_tracks.sql        ← top tracks agrégés
│       ├── gold_playlist_stats.sql    ← stats playlists
│       ├── gold_duration_analysis.sql ← analyse des durées
│       └── gold_position_patterns.sql ← patterns par position
├── tests/
│   ├── generic/                  ← tests dbt built-in (not_null, unique...)
│   └── singular/
│       ├── test_duration_positive.sql
│       └── test_followers_non_negative.sql
└── macros/
    └── (vide pour l'instant)
```

### 4. Contenu des fichiers à créer

#### `profiles.yml` (à la racine de dbt_spotify/)
```yaml
dbt_spotify:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ../spotify_stream.duckdb
      threads: 4
```

#### `dbt_project.yml`
```yaml
name: dbt_spotify
version: '1.0.0'
config-version: 2
profile: dbt_spotify

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

models:
  dbt_spotify:
    silver:
      +materialized: view
    gold:
      +materialized: table
```

#### `models/sources.yml`
```yaml
version: 2

sources:
  - name: spotify_stream
    database: spotify_stream
    schema: main
    tables:
      - name: v_tracks
        description: "Vue Silver — tracks par playlist"
        columns:
          - name: playlist_id
            tests: [not_null]
          - name: track_uri
            tests: [not_null]
          - name: duration_ms
            tests: [not_null]
      - name: v_playlists
        description: "Vue Silver — metadata playlists"
        columns:
          - name: playlist_id
            tests: [not_null, unique]
          - name: num_followers
            tests: [not_null]
```

#### `models/silver/silver_tracks.sql`
```sql
-- Nettoyage et typage de la couche tracks
SELECT
    playlist_id,
    track_uri,
    TRIM(track_name)                        AS track_name,
    TRIM(artist_name)                       AS artist_name,
    artist_uri,
    TRIM(album_name)                        AS album_name,
    album_uri,
    duration_ms,
    ROUND(duration_ms / 60000.0, 3)        AS duration_min,
    position,
    CASE
        WHEN duration_ms < 60000  THEN 'short'
        WHEN duration_ms < 210000 THEN 'standard'
        WHEN duration_ms < 360000 THEN 'long'
        ELSE 'very_long'
    END                                     AS duration_category
FROM {{ source('spotify_stream', 'v_tracks') }}
WHERE track_uri IS NOT NULL
  AND duration_ms > 0
  AND playlist_id IS NOT NULL
```

#### `models/silver/silver_playlists.sql`
```sql
-- Nettoyage et typage de la couche playlists
SELECT
    playlist_id,
    TRIM(playlist_name)                                      AS playlist_name,
    num_tracks,
    num_albums,
    num_followers,
    ROUND(num_albums * 1.0 / NULLIF(num_tracks, 0), 3)     AS album_diversity_ratio,
    CASE
        WHEN num_followers = 0    THEN 'no_followers'
        WHEN num_followers < 100  THEN 'small'
        WHEN num_followers < 1000 THEN 'medium'
        WHEN num_followers < 10000 THEN 'large'
        ELSE 'viral'
    END                                                      AS popularity_tier,
    CASE
        WHEN num_tracks < 20  THEN 'mini'
        WHEN num_tracks < 50  THEN 'standard'
        WHEN num_tracks < 100 THEN 'large'
        ELSE 'mega'
    END                                                      AS size_category,
    modified_at
FROM {{ source('spotify_stream', 'v_playlists') }}
WHERE playlist_id IS NOT NULL
```

#### `models/gold/gold_top_artists.sql`
```sql
-- Top artistes agrégés — prêt pour le dashboard
SELECT
    artist_name,
    artist_uri,
    COUNT(DISTINCT playlist_id)             AS nb_playlists,
    COUNT(*)                                AS nb_track_appearances,
    COUNT(DISTINCT track_uri)               AS nb_unique_tracks,
    COUNT(DISTINCT album_name)              AS nb_albums,
    ROUND(AVG(duration_min), 3)             AS avg_duration_min,
    ROUND(MIN(duration_min), 3)             AS min_duration_min,
    ROUND(MAX(duration_min), 3)             AS max_duration_min,
    ROUND(AVG(position), 1)                 AS avg_position
FROM {{ ref('silver_tracks') }}
WHERE artist_name IS NOT NULL
GROUP BY artist_name, artist_uri
ORDER BY nb_playlists DESC
```

#### `models/gold/gold_top_tracks.sql`
```sql
-- Top tracks agrégés — prêt pour le dashboard
SELECT
    track_uri,
    track_name,
    artist_name,
    album_name,
    duration_min,
    duration_category,
    COUNT(*)                    AS occurrences,
    COUNT(DISTINCT playlist_id) AS nb_playlists,
    ROUND(AVG(position), 1)     AS avg_position,
    MIN(position)               AS best_position
FROM {{ ref('silver_tracks') }}
WHERE track_uri IS NOT NULL
GROUP BY track_uri, track_name, artist_name, album_name, duration_min, duration_category
ORDER BY occurrences DESC
```

#### `models/gold/gold_playlist_stats.sql`
```sql
-- Stats playlists enrichies — prêt pour le dashboard
SELECT
    p.playlist_id,
    p.playlist_name,
    p.num_tracks,
    p.num_albums,
    p.num_followers,
    p.album_diversity_ratio,
    p.popularity_tier,
    p.size_category,
    ROUND(AVG(t.duration_min), 3)   AS avg_track_duration_min,
    COUNT(DISTINCT t.artist_name)   AS nb_distinct_artists
FROM {{ ref('silver_playlists') }} p
LEFT JOIN {{ ref('silver_tracks') }} t
    ON p.playlist_id = t.playlist_id
GROUP BY
    p.playlist_id, p.playlist_name, p.num_tracks, p.num_albums,
    p.num_followers, p.album_diversity_ratio, p.popularity_tier, p.size_category
ORDER BY p.num_followers DESC
```

#### `models/gold/gold_duration_analysis.sql`
```sql
-- Analyse des durées par bucket — pour l'histogramme du dashboard
SELECT
    FLOOR(duration_ms / 30000) * 30     AS bucket_sec,
    ROUND((FLOOR(duration_ms / 30000) * 30) / 60.0, 2) AS bucket_min,
    duration_category,
    COUNT(*)                             AS nb_tracks,
    COUNT(DISTINCT artist_name)          AS nb_artists
FROM {{ ref('silver_tracks') }}
WHERE duration_ms < 600000
GROUP BY bucket_sec, bucket_min, duration_category
ORDER BY bucket_sec
```

#### `models/gold/gold_position_patterns.sql`
```sql
-- Patterns par position dans la playlist
SELECT
    position,
    COUNT(DISTINCT track_uri)       AS unique_tracks,
    COUNT(DISTINCT artist_name)     AS unique_artists,
    ROUND(AVG(duration_min), 3)     AS avg_duration_min,
    COUNT(*)                        AS total_appearances
FROM {{ ref('silver_tracks') }}
WHERE position <= 50
GROUP BY position
ORDER BY position
```

#### `tests/singular/test_duration_positive.sql`
```sql
-- Vérifie qu'aucune durée n'est négative ou nulle
SELECT *
FROM {{ ref('silver_tracks') }}
WHERE duration_ms <= 0
```

#### `tests/singular/test_followers_non_negative.sql`
```sql
-- Vérifie qu'aucun nombre de followers n'est négatif
SELECT *
FROM {{ ref('silver_playlists') }}
WHERE num_followers < 0
```

### 5. Lancer dbt

```bash
cd dbt_spotify

# Vérifier la connexion
dbt debug

# Compiler sans exécuter (vérif SQL)
dbt compile

# Lancer tous les modèles
dbt run

# Lancer les tests
dbt test

# Tout en une commande
dbt build
```

### 6. Mettre à jour dashboard.py pour utiliser les tables Gold

Une fois dbt run exécuté, remplacer dans dashboard.py les requêtes
qui lisent `v_tracks` / `v_playlists` par les tables Gold :

```python
# Avant (Silver)
FROM v_tracks

# Après (Gold)
FROM gold_top_artists      -- pour les charts artistes
FROM gold_top_tracks       -- pour la table tracks
FROM gold_playlist_stats   -- pour les charts playlists
FROM gold_duration_analysis -- pour l'histogramme durées
FROM gold_position_patterns -- pour les patterns position
```

---

## Robustesse attendue

- `dbt test` valide la qualité des données à chaque run
- Les tables Gold sont **matérialisées** (pas des vues) → requêtes dashboard ultra rapides
- Si un fichier Parquet est corrompu, `dbt test` le détecte avant le dashboard
- `dbt run --select gold` pour ne regénérer que la Gold si besoin

---

## Structure finale du projet

```
varianteThree/
├── spotify_stream.duckdb      ← contient Silver views + Gold tables après dbt run
├── dashboard.py               ← Streamlit (à mettre à jour pour Gold)
├── requirements.txt
├── .streamlit/config.toml
├── conversionV3.py            ← JSON → Parquet (Bronze → Silver)
└── dbt_spotify/               ← nouveau
    ├── profiles.yml
    ├── dbt_project.yml
    ├── models/
    │   ├── sources.yml
    │   ├── silver/
    │   └── gold/
    └── tests/
        └── singular/
```