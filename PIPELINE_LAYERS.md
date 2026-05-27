# Pipeline ETL/ELT — Bronze · Silver · Gold

Projet EPSI M1 — Spotify Million Playlist Dataset  
Stack : Python · Polars · orjson · DuckDB · dbt-duckdb · Streamlit

---

## Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE — données brutes                                        │
│  1 000 fichiers JSON  (~1 000 playlists chacun)                 │
│  data/mpd.slice.*.json                                          │
└────────────────────────────┬────────────────────────────────────┘
                             │ conversionV3.py (JSON → Parquet)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER — données structurées                                   │
│  2 000 fichiers Parquet LZ4                                     │
│  outputParquetV3/*_playlists.parquet  (1 000 fichiers)          │
│  outputParquetV3/*_tracks.parquet     (1 000 fichiers)          │
│  Taille totale : 3,4 GB                                         │
│                                                                 │
│  Vues DuckDB (lecture des Parquet)                              │
│  v_playlists   1 000 000 lignes                                 │
│  v_tracks     66 346 428 lignes                                 │
│                                                                 │
│  dbt Silver — vues enrichies  (pas de copie physique)           │
│  silver_playlists   +3 colonnes calculées                       │
│  silver_tracks      +2 colonnes calculées, filtres qualité      │
└────────────────────────────┬────────────────────────────────────┘
                             │ dbt run (tables matérialisées)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD — tables DuckDB  (stockage physique)                      │
│  gold_top_artists        295 855 lignes                         │
│  gold_top_tracks       2 262 240 lignes                         │
│  gold_playlist_stats   1 000 000 lignes                         │
│  gold_duration_analysis         20 lignes                       │
│  gold_position_patterns         51 lignes                       │
└────────────────────────────┬────────────────────────────────────┘
                             │ SELECT simple
                             ▼
                      dashboard.py (Streamlit)
```

---

## Bronze — Données brutes (JSON)

### Rôle

Stocker les fichiers JSON bruts du dataset Spotify MPD tels quels, sans aucune transformation. C'est la couche de données source.

### Localisation

**`data/`** — 1 000 fichiers JSON.

### Transformation Bronze → Silver

**`conversionV3.py`** — exécuté une seule fois manuellement. Lit les JSON bruts (Bronze) et produit les fichiers Parquet (Silver).

### Ce qui est chargé en entrée (Bronze)

| Élément | Détail |
|---|---|
| Format source | JSON (Spotify Million Playlist Dataset) |
| Localisation | `/home/snowgou/EPSI/M1/ETL&ELT/code/data/mpd.slice.*.json` |
| Nombre de fichiers | 1 000 fichiers JSON |
| Contenu par fichier | ~1 000 playlists + leurs tracks imbriquées |
| Volume total | ~1 milliard de caractères JSON |

### Structure JSON d'entrée

```
{
  "playlists": [
    {
      "name": "Throwbacks",
      "collaborative": "false",
      "pid": 0,
      "modified_at": 1493424000,
      "num_tracks": 52,
      "num_albums": 47,
      "num_followers": 1,
      "num_edits": 6,
      "duration_ms": 11532414,
      "num_artists": 37,
      "tracks": [
        {
          "pos": 0,
          "artist_name": "Missy Elliott",
          "track_uri": "spotify:track:0UaMYEvWZi0ZqiDOoHU3YI",
          "artist_uri": "spotify:artist:...",
          "track_name": "Lose Control",
          "album_uri": "spotify:album:...",
          "duration_ms": 226863,
          "album_name": "The Cookbook"
        },
        ...
      ]
    },
    ...
  ]
}
```

### Ce qui est produit en sortie (Silver)

**2 fichiers Parquet par JSON** (1 000 playlists par JSON → 2 000 fichiers au total) dans `outputParquetV3/` :

#### `mpd.slice.X-Y_playlists.parquet`

| Colonne | Type | Origine |
|---|---|---|
| `pid` | Int64 | `playlist.pid` |
| `name` | Utf8 | `playlist.name` |
| `collaborative` | Utf8 | `playlist.collaborative` |
| `modified_at` | Int64 | `playlist.modified_at` (unix timestamp) |
| `num_tracks` | Int64 | `playlist.num_tracks` |
| `num_albums` | Int64 | `playlist.num_albums` |
| `num_followers` | Int64 | `playlist.num_followers` |
| `num_edits` | Int64 | `playlist.num_edits` |
| `duration_ms` | Int64 | `playlist.duration_ms` |
| `num_artists` | Int64 | `playlist.num_artists` |

#### `mpd.slice.X-Y_tracks.parquet`

| Colonne | Type | Origine |
|---|---|---|
| `pid` | Int64 | `playlist.pid` (clé de jointure) |
| `pos` | Int64 | `track.pos` (position dans la playlist) |
| `artist_name` | Utf8 | `track.artist_name` |
| `track_uri` | Utf8 | `track.track_uri` (identifiant Spotify) |
| `artist_uri` | Utf8 | `track.artist_uri` |
| `track_name` | Utf8 | `track.track_name` |
| `album_uri` | Utf8 | `track.album_uri` |
| `duration_ms` | Int64 | `track.duration_ms` |
| `album_name` | Utf8 | `track.album_name` |

### Optimisations techniques

| Axe | Choix | Raison |
|---|---|---|
| Parsing JSON | `orjson` | 2–3× plus rapide que `json` stdlib |
| Dataframes | `polars` | Écriture Parquet native, zero-copy |
| Compression | LZ4 | Plus rapide en écriture que Snappy ou Zstd |
| Parallélisme | `ProcessPoolExecutor(max_workers=8)` | 1 process par fichier JSON, évite le GIL |
| Structure sortie | Répertoire plat (pas de sous-dossiers) | Compatible `read_parquet('dir/*.parquet')` |

---

## Silver — Nettoyage et typage

### Rôle

Exposer les fichiers Parquet Silver sous un schéma cohérent avec des noms de colonnes métier, filtrer les lignes invalides, et ajouter des colonnes calculées utiles pour la couche Gold. **Aucune donnée n'est agrégée ici.**

Le nettoyage Silver se fait en deux étapes enchaînées, toutes deux matérialisées en **vues** (aucun stockage physique supplémentaire) :

### Étape 1 — Vues DuckDB (`v_tracks`, `v_playlists`)

Ces vues sont stockées dans `spotify_stream.duckdb`. Elles lisent les fichiers Parquet Silver via `read_parquet()` avec des **chemins absolus** et renomment les colonnes courtes en noms métier.

> **Pourquoi des chemins absolus ?**  
> dbt exécute les requêtes depuis le dossier `dbt_spotify/`. Si les vues utilisent des chemins relatifs (`./outputParquetV3/`), DuckDB les résout depuis le mauvais répertoire. Les chemins absolus garantissent que les vues fonctionnent quel que soit le répertoire d'exécution.

```sql
-- v_tracks
CREATE VIEW v_tracks AS
SELECT
    pid  AS playlist_id,   -- renommage : pid → playlist_id
    track_uri,
    track_name,
    artist_name,
    artist_uri,
    album_name,
    album_uri,
    duration_ms,
    pos  AS position        -- renommage : pos → position
FROM read_parquet('/chemin/absolu/outputParquetV3/*_tracks.parquet');

-- v_playlists
CREATE VIEW v_playlists AS
SELECT
    pid   AS playlist_id,   -- renommage : pid → playlist_id
    name  AS playlist_name, -- renommage : name → playlist_name
    num_tracks,
    num_albums,
    num_followers,
    modified_at
FROM read_parquet('/chemin/absolu/outputParquetV3/*_playlists.parquet');
```

### Étape 2 — Modèles dbt Silver (`silver_tracks`, `silver_playlists`)

Ces modèles lisent les vues DuckDB et ajoutent nettoyage + colonnes dérivées. Ils sont matérialisés en **vues** dans DuckDB (pas de table physique).

#### `silver_tracks` — ce qui est chargé

**Source :** `v_tracks` — 66 346 428 lignes

**Transformations appliquées :**

| Transformation | Colonne concernée | Détail |
|---|---|---|
| Nettoyage | `track_name`, `artist_name`, `album_name` | `TRIM()` — supprime les espaces parasites |
| Colonne calculée | `duration_min` | `duration_ms / 60000.0` arrondi à 3 décimales |
| Colonne calculée | `duration_category` | Catégorie textuelle selon la durée |
| Filtre qualité | — | `track_uri IS NOT NULL AND duration_ms > 0 AND playlist_id IS NOT NULL` |

**Logique de `duration_category` :**

```
< 1 min       → 'short'
1 à 3.5 min   → 'standard'
3.5 à 6 min   → 'long'
> 6 min       → 'very_long'
```

**Schéma de sortie `silver_tracks` :**

| Colonne | Type | Description |
|---|---|---|
| `playlist_id` | Int64 | ID de la playlist |
| `track_uri` | Varchar | URI Spotify du track |
| `track_name` | Varchar | Nom nettoyé |
| `artist_name` | Varchar | Artiste nettoyé |
| `artist_uri` | Varchar | URI Spotify de l'artiste |
| `album_name` | Varchar | Album nettoyé |
| `album_uri` | Varchar | URI Spotify de l'album |
| `duration_ms` | Int64 | Durée en millisecondes |
| `duration_min` | Double | Durée en minutes (3 décimales) |
| `position` | Int64 | Position dans la playlist (0-based) |
| `duration_category` | Varchar | short / standard / long / very_long |

---

#### `silver_playlists` — ce qui est chargé

**Source :** `v_playlists` — 1 000 000 lignes

**Transformations appliquées :**

| Transformation | Colonne concernée | Détail |
|---|---|---|
| Nettoyage | `playlist_name` | `TRIM()` |
| Colonne calculée | `album_diversity_ratio` | `num_albums / num_tracks` — mesure la variété des albums |
| Colonne calculée | `popularity_tier` | Segment de popularité selon `num_followers` |
| Colonne calculée | `size_category` | Catégorie de taille selon `num_tracks` |
| Filtre qualité | — | `playlist_id IS NOT NULL` |

**Logique de `popularity_tier` :**

```
0 followers           → 'no_followers'
1 à 99 followers      → 'small'
100 à 999 followers   → 'medium'
1 000 à 9 999         → 'large'
≥ 10 000              → 'viral'
```

**Logique de `size_category` :**

```
< 20 tracks    → 'mini'
20 à 49        → 'standard'
50 à 99        → 'large'
≥ 100          → 'mega'
```

**Schéma de sortie `silver_playlists` :**

| Colonne | Type | Description |
|---|---|---|
| `playlist_id` | Int64 | ID de la playlist |
| `playlist_name` | Varchar | Nom nettoyé |
| `num_tracks` | Int64 | Nombre de tracks |
| `num_albums` | Int64 | Nombre d'albums distincts |
| `num_followers` | Int64 | Nombre de followers |
| `album_diversity_ratio` | Double | num_albums / num_tracks |
| `popularity_tier` | Varchar | no_followers / small / medium / large / viral |
| `size_category` | Varchar | mini / standard / large / mega |
| `modified_at` | Int64 | Dernière modification (unix timestamp) |

---

## Gold — Agrégations et tables analytiques

### Rôle

Pré-calculer toutes les agrégations nécessaires au dashboard Streamlit et les stocker physiquement sous forme de **tables** dans DuckDB. Le dashboard ne fait plus que des `SELECT` simples sur ces tables — aucun calcul lourd à la volée.

### Outil

**dbt-duckdb** — les 5 modèles sont matérialisés en `table` (stockage physique) dans `spotify_stream.duckdb`.

```bash
# Commande pour (re)créer toute la couche Gold
cd dbt_spotify
dbt run --select gold --profiles-dir .
```

---

### `gold_top_artists` — 295 855 lignes

**Source :** `silver_tracks`  
**Granularité :** 1 ligne = 1 artiste unique  
**Utilisation dashboard :** graphique top artistes par playlists, scatter durée moyenne

| Colonne | Type | Description |
|---|---|---|
| `artist_name` | Varchar | Nom de l'artiste |
| `artist_uri` | Varchar | URI Spotify |
| `nb_playlists` | Int64 | Nombre de playlists distinctes où il apparaît |
| `nb_track_appearances` | Int64 | Total d'apparitions de ses tracks |
| `nb_unique_tracks` | Int64 | Nombre de tracks uniques |
| `nb_albums` | Int64 | Nombre d'albums distincts |
| `avg_duration_min` | Double | Durée moyenne de ses tracks (min) |
| `min_duration_min` | Double | Track la plus courte (min) |
| `max_duration_min` | Double | Track la plus longue (min) |
| `avg_position` | Double | Position moyenne dans les playlists |

**Exemple de ligne :**

```
Drake | nb_playlists=203 345 | nb_track_appearances=846 937 | avg_duration_min=3.969
```

---

### `gold_top_tracks` — 2 262 240 lignes

**Source :** `silver_tracks`  
**Granularité :** 1 ligne = 1 track unique (groupé par `track_uri`)  
**Utilisation dashboard :** tableau top tracks récurrents

| Colonne | Type | Description |
|---|---|---|
| `track_uri` | Varchar | URI Spotify (clé) |
| `track_name` | Varchar | Titre |
| `artist_name` | Varchar | Artiste |
| `album_name` | Varchar | Album |
| `duration_min` | Double | Durée en minutes |
| `duration_category` | Varchar | Catégorie de durée |
| `occurrences` | Int64 | Nombre total d'apparitions dans toutes les playlists |
| `nb_playlists` | Int64 | Nombre de playlists distinctes |
| `avg_position` | Double | Position moyenne |
| `best_position` | Int64 | Meilleure position atteinte |

**Exemple de ligne :**

```
HUMBLE. | Kendrick Lamar | occurrences=46 574 | nb_playlists=45 394 | best_position=0
```

---

### `gold_playlist_stats` — 1 000 000 lignes

**Source :** `silver_playlists` LEFT JOIN `silver_tracks`  
**Granularité :** 1 ligne = 1 playlist  
**Utilisation dashboard :** scatter followers vs tracks, tableau top playlists, histogramme tailles

| Colonne | Type | Description |
|---|---|---|
| `playlist_id` | Int64 | ID |
| `playlist_name` | Varchar | Nom |
| `num_tracks` | Int64 | Nombre de tracks |
| `num_albums` | Int64 | Nombre d'albums |
| `num_followers` | Int64 | Followers |
| `album_diversity_ratio` | Double | Ratio albums/tracks |
| `popularity_tier` | Varchar | Segment de popularité |
| `size_category` | Varchar | Catégorie de taille |
| `avg_track_duration_min` | Double | Durée moyenne des tracks (calculée par JOIN) |
| `nb_distinct_artists` | Int64 | Nombre d'artistes distincts (calculé par JOIN) |

> Le LEFT JOIN garantit que les playlists sans tracks apparaissent quand même, avec `NULL` pour les colonnes calculées par JOIN.

---

### `gold_duration_analysis` — 20 lignes

**Source :** `silver_tracks`  
**Granularité :** 1 ligne = 1 bucket de 30 secondes  
**Utilisation dashboard :** histogramme de distribution des durées  
**Filtre :** `duration_ms < 600000` (tracks < 10 min, exclut les outliers)

| Colonne | Type | Description |
|---|---|---|
| `bucket_sec` | Double | Début du bucket en secondes (0, 30, 60, ...) |
| `bucket_min` | Double | Début du bucket en minutes (0.0, 0.5, 1.0, ...) |
| `duration_category` | Varchar | Catégorie dominante du bucket |
| `nb_tracks` | Int64 | Nombre de tracks dans ce bucket |
| `nb_artists` | Int64 | Nombre d'artistes distincts dans ce bucket |

**Exemple de ligne :**

```
bucket_sec=180 | bucket_min=3.0 | duration_category=standard | nb_tracks=4 231 847
```

---

### `gold_position_patterns` — 51 lignes

**Source :** `silver_tracks`  
**Granularité :** 1 ligne = 1 position (0 à 50)  
**Utilisation dashboard :** courbes diversité et durée selon la position dans la playlist

| Colonne | Type | Description |
|---|---|---|
| `position` | Int64 | Position dans la playlist (0-based) |
| `unique_tracks` | Int64 | Nombre de tracks uniques à cette position |
| `unique_artists` | Int64 | Nombre d'artistes uniques à cette position |
| `avg_duration_min` | Double | Durée moyenne des tracks à cette position |
| `total_appearances` | Int64 | Total d'occurrences à cette position |

---

## Récapitulatif des volumes

| Couche | Élément | Lignes / Fichiers | Stockage | Matérialisation |
|---|---|---|---|---|
| **Source** | 1 000 JSON | 1 000 fichiers | ~32 GB | Fichiers disque |
| **Bronze** | Parquet playlists | 1 000 fichiers | — | Fichiers Parquet LZ4 |
| **Bronze** | Parquet tracks | 1 000 fichiers | 3,4 GB total | Fichiers Parquet LZ4 |
| **Silver** | `v_tracks` | 66 346 428 lignes | 0 | Vue DuckDB |
| **Silver** | `v_playlists` | 1 000 000 lignes | 0 | Vue DuckDB |
| **Silver** | `silver_tracks` | 66 346 428 lignes | 0 | Vue dbt |
| **Silver** | `silver_playlists` | 1 000 000 lignes | 0 | Vue dbt |
| **Gold** | `gold_top_artists` | 295 855 lignes | faible | Table DuckDB |
| **Gold** | `gold_top_tracks` | 2 262 240 lignes | faible | Table DuckDB |
| **Gold** | `gold_playlist_stats` | 1 000 000 lignes | faible | Table DuckDB |
| **Gold** | `gold_duration_analysis` | 20 lignes | négligeable | Table DuckDB |
| **Gold** | `gold_position_patterns` | 51 lignes | négligeable | Table DuckDB |

---

## Choix de matérialisation — Pourquoi vue vs table ?

| Couche | Type | Raison |
|---|---|---|
| Silver | **Vue** | Les données sont déjà dans les Parquet. Dupliquer 66M lignes en table serait inutile. |
| Gold | **Table** | Les agrégations sont coûteuses à recalculer (45s pour `gold_top_artists`). Stocker le résultat permet des requêtes dashboard < 100ms. |

---

## Pour régénérer le pipeline

```bash
# 1. Bronze — conversion JSON → Parquet (si nouveaux fichiers JSON)
python conversionV3.py

# 2. Silver — rien à faire (les vues DuckDB lisent automatiquement les nouveaux Parquet)

# 3. Gold — recréer les tables agrégées
cd dbt_spotify
dbt run --select gold --profiles-dir .

# 4. Tests qualité
dbt test --profiles-dir .

# 5. Dashboard
cd ..
streamlit run dashboard.py
```
