# Spotify Analytics Pipeline — Variante 4

Pipeline ETL/ELT sur le Spotify Million Playlist Dataset avec une couche Gold construite via **dbt-duckdb** et un dashboard **Streamlit**.

## Architecture

```
data/                        ← Bronze : JSON bruts (Spotify MPD)
outputParquetV3/             ← Silver : fichiers Parquet
spotify_stream.duckdb        ← base DuckDB (Silver views + Gold tables)
dbt_spotify/                 ← couche Gold via dbt
dashboard.py                 ← dashboard Streamlit
```

### Couches

| Couche | Technologie | Contenu |
|--------|-------------|---------|
| Bronze | JSON bruts | 1000 fichiers Spotify MPD |
| Silver | Parquet + DuckDB views | `v_tracks`, `v_playlists` |
| Gold | DuckDB tables (via dbt) | 5 tables agrégées prêtes dashboard |

---

## Prérequis

- Python 3.10+
- Les fichiers JSON Spotify MPD dans `../data/` (dossier parent)

---

## Installation

```bash
pip install polars orjson dbt-duckdb streamlit duckdb pandas plotly
```

---

## Lancer le pipeline complet

### Etape 1 — Bronze → Silver (JSON → Parquet)

Convertit les 1000 fichiers JSON en fichiers Parquet et crée les vues DuckDB Silver.

```bash
python conversionV3.py
```

Produit :
- `outputParquetV3/playlists_*.parquet`
- `outputParquetV3/tracks_*.parquet`
- `spotify_stream.duckdb` avec les vues `v_tracks` et `v_playlists`

### Etape 2 — Silver → Gold (dbt)

Construit les tables Gold agrégées dans `spotify_stream.duckdb`.

```bash
cd dbt_spotify

# Vérifier la connexion
dbt debug

# Lancer tous les modèles + tests
dbt build
```

Tables créées :

| Table | Description |
|-------|-------------|
| `gold_top_artists` | Top artistes (occurrences, playlists, durée moyenne) |
| `gold_top_tracks` | Top tracks (occurrences, playlists, position moyenne) |
| `gold_playlist_stats` | Stats playlists enrichies |
| `gold_duration_analysis` | Distribution des durées par bucket 30s |
| `gold_position_patterns` | Patterns audio par position dans la playlist |

### Etape 3 — Dashboard

```bash
# Depuis la racine du projet
streamlit run dashboard.py
```

Ouvre automatiquement `http://localhost:8501`.

---

## Commandes dbt utiles

```bash
cd dbt_spotify

dbt run                        # Regénérer tous les modèles
dbt run --select gold          # Regénérer uniquement la Gold
dbt test                       # Lancer les tests qualité
dbt compile                    # Vérifier le SQL sans exécuter
```

---

## Structure du projet

```
varianteFour/
├── data/                      ← Bronze : JSON bruts
├── conversionV3.py            ← Transformation Bronze → Silver
├── dashboard.py               ← Dashboard Streamlit
├── spotify_stream.duckdb      ← Base de données principale
├── outputParquetV3/           ← Silver : fichiers Parquet
└── dbt_spotify/
    ├── profiles.yml
    ├── dbt_project.yml
    ├── models/
    │   ├── sources.yml
    │   ├── silver/
    │   │   ├── silver_tracks.sql
    │   │   └── silver_playlists.sql
    │   └── gold/
    │       ├── gold_top_artists.sql
    │       ├── gold_top_tracks.sql
    │       ├── gold_playlist_stats.sql
    │       ├── gold_duration_analysis.sql
    │       └── gold_position_patterns.sql
    └── tests/
        └── singular/
            ├── test_duration_positive.sql
            └── test_followers_non_negative.sql
```
