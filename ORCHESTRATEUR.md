# Orchestrateur Windmill — Documentation

## 1. Pourquoi un orchestrateur ?

Avant Windmill, le pipeline etait lance manuellement, etape par etape :

```
python conversionV3.py   →   dbt build   →   streamlit run dashboard.py
```

Windmill permet de :
- **Enchainer** les etapes automatiquement (Bronze → Silver → Gold)
- **Planifier** les runs via cron
- **Surveiller** les executions (logs, statut, duree)
- **Rejouer** une etape en cas d'erreur sans tout relancer

---

## 2. Acces

| | |
|---|---|
| URL | http://localhost:80 |
| Email | `test@test.fr` |
| Mot de passe | `azertyuiop` |
| Workspace | `spotify` |

---

## 3. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     WINDMILL (Docker)                         │
│                                                              │
│  Flow: f/pipeline/pipeline_spotify                           │
│  ┌──────────────────────┐    ┌──────────────────────┐        │
│  │  Step A               │──▶│  Step B               │       │
│  │  bronze_to_silver     │    │  silver_to_gold       │       │
│  │                       │    │                       │       │
│  │  1000 JSON → 2000     │    │  pip install dbt      │       │
│  │  Parquet (polars)     │    │  dbt build            │       │
│  └──────────────────────┘    └──────────────────────┘        │
│           │                           │                       │
│           ▼                           ▼                       │
│     /data (ro)                  /dbt_spotify                  │
│     → outputParquetV3/          → /spotify_stream.duckdb      │
└──────────────────────────────────────────────────────────────┘
```

---

## 4. Ce qui a ete mis en place

### 4.1 Docker Compose (`docker-compose.yml`)

Windmill tourne avec les services suivants :

| Service | Role |
|---------|------|
| `db` | PostgreSQL 16 (metadonnees Windmill) |
| `windmill_server` | Interface web + API |
| `windmill_worker` (x3) | Executent les scripts Python |
| `windmill_worker_native` | Jobs legers (internes Windmill) |
| `windmill_extra` | LSP + debugger |
| `dind` | Docker-in-Docker (isolation) |
| `caddy` | Reverse proxy (port 80) |

#### Volumes montes sur les workers

Les workers ont acces aux fichiers du projet via des bind mounts :

```yaml
volumes:
  # Bronze — JSON bruts (lecture seule)
  - /home/snowgou/EPSI/M1/ETL&ELT/code/varianteFour/data:/data:ro

  # Silver — Parquet (meme chemin que l'hote pour compatibilite vues DuckDB)
  - /home/snowgou/EPSI/M1/ETL&ELT/code/varianteFour/outputParquetV3:/home/snowgou/EPSI/M1/ETL&ELT/code/varianteFour/outputParquetV3

  # Base DuckDB
  - /home/snowgou/EPSI/M1/ETL&ELT/code/varianteFour/spotify_stream.duckdb:/spotify_stream.duckdb

  # Projet dbt
  - /home/snowgou/EPSI/M1/ETL&ELT/code/varianteFour/dbt_spotify:/dbt_spotify
```

> **Pourquoi `outputParquetV3` est monte au meme chemin que l'hote ?**
> Les vues DuckDB (`v_tracks`, `v_playlists`) contiennent des chemins absolus
> (`/home/snowgou/.../outputParquetV3/*.parquet`). Si on montait a `/outputParquetV3`,
> DuckDB ne trouverait pas les fichiers dans le container.

### 4.2 Scripts Windmill

Deux scripts Python dans le workspace `spotify`, dossier `f/pipeline/` :

#### Script `f/pipeline/bronze_to_silver`

**Role** : Convertit les 1000 JSON bruts (Bronze) en 2000 fichiers Parquet (Silver).

**Comment ca marche** :
1. Lit chaque fichier JSON depuis `/data/` (monte en read-only)
2. Extrait les playlists et tracks avec `orjson`
3. Ecrit les Parquet avec `polars` (compression LZ4)
4. Sortie dans `outputParquetV3/`

**Dependances** (auto-installees par Windmill) : `orjson`, `polars`

**Code source** :

```python
import os
from pathlib import Path

import orjson
import polars as pl

DATA_DIR = Path("/data")
OUTPUT_DIR = Path("/home/snowgou/EPSI/M1/ETL&ELT/code/varianteFour/outputParquetV3")


def process_file(path: Path) -> str:
    with open(path, "rb") as f:
        data = orjson.loads(f.read())

    playlists = {
        "pid": [], "name": [], "collaborative": [], "modified_at": [],
        "num_tracks": [], "num_albums": [], "num_followers": [],
        "num_edits": [], "duration_ms": [], "num_artists": [],
    }
    tracks = {
        "pid": [], "pos": [], "artist_name": [], "track_uri": [],
        "artist_uri": [], "track_name": [], "album_uri": [],
        "duration_ms": [], "album_name": [],
    }

    for pl_data in data["playlists"]:
        pid = pl_data["pid"]
        playlists["pid"].append(pid)
        playlists["name"].append(pl_data["name"])
        playlists["collaborative"].append(pl_data["collaborative"])
        playlists["modified_at"].append(pl_data["modified_at"])
        playlists["num_tracks"].append(pl_data["num_tracks"])
        playlists["num_albums"].append(pl_data["num_albums"])
        playlists["num_followers"].append(pl_data["num_followers"])
        playlists["num_edits"].append(pl_data["num_edits"])
        playlists["duration_ms"].append(pl_data["duration_ms"])
        playlists["num_artists"].append(pl_data["num_artists"])

        for track in pl_data.get("tracks", []):
            tracks["pid"].append(pid)
            tracks["pos"].append(track["pos"])
            tracks["artist_name"].append(track["artist_name"])
            tracks["track_uri"].append(track["track_uri"])
            tracks["artist_uri"].append(track["artist_uri"])
            tracks["track_name"].append(track["track_name"])
            tracks["album_uri"].append(track["album_uri"])
            tracks["duration_ms"].append(track["duration_ms"])
            tracks["album_name"].append(track["album_name"])

    stem = path.stem
    pl.DataFrame(playlists).write_parquet(
        OUTPUT_DIR / f"{stem}_playlists.parquet", compression="lz4"
    )
    pl.DataFrame(tracks).write_parquet(
        OUTPUT_DIR / f"{stem}_tracks.parquet", compression="lz4"
    )
    return stem


def main():
    json_files = sorted(DATA_DIR.glob("*.json"))
    if not json_files:
        raise Exception("Aucun fichier JSON dans /data/")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Bronze: {len(json_files)} fichiers JSON")

    for i, path in enumerate(json_files, 1):
        process_file(path)
        if i % 100 == 0:
            print(f"  {i}/{len(json_files)} fichiers convertis")

    parquet_count = len(list(OUTPUT_DIR.glob("*.parquet")))
    print(f"Silver: {parquet_count} fichiers Parquet produits")

    return {
        "status": "ok",
        "json_files": len(json_files),
        "parquet_files": parquet_count,
    }
```

---

#### Script `f/pipeline/silver_to_gold`

**Role** : Lance `dbt build` pour creer les 5 tables Gold dans DuckDB.

**Comment ca marche** :
1. Installe `dbt-duckdb` via pip (dans l'environnement du worker)
2. Lance `dbt debug` pour verifier la connexion
3. Lance `dbt build` qui execute les modeles Silver (vues) + Gold (tables) + tests

**Tables Gold produites** :

| Table | Contenu |
|-------|---------|
| `gold_top_artists` | Artistes agreges (playlists, tracks, duree) |
| `gold_top_tracks` | Tracks agreges (occurrences, playlists) |
| `gold_playlist_stats` | Stats playlists enrichies |
| `gold_duration_analysis` | Histogramme des durees par bucket 30s |
| `gold_position_patterns` | Patterns par position dans la playlist |

**Code source** :

```python
import subprocess
import os


def main():
    dbt_dir = "/dbt_spotify"

    if not os.path.exists(os.path.join(dbt_dir, "dbt_project.yml")):
        raise Exception(f"dbt_project.yml introuvable dans {dbt_dir}")

    if not os.path.exists("/spotify_stream.duckdb"):
        raise Exception("spotify_stream.duckdb introuvable")

    # Installer dbt-duckdb
    print("Installation de dbt-duckdb...")
    install = subprocess.run(
        ["pip", "install", "dbt-duckdb"],
        capture_output=True, text=True, timeout=120
    )
    if install.returncode != 0:
        raise Exception(f"pip install echoue: {install.stderr}")
    print("dbt-duckdb installe")

    # dbt debug
    print("Verification connexion (dbt debug)...")
    debug = subprocess.run(
        ["dbt", "debug", "--profiles-dir", "."],
        cwd=dbt_dir, capture_output=True, text=True, timeout=60
    )
    if debug.returncode != 0:
        raise Exception(f"dbt debug echoue:\n{debug.stdout}\n{debug.stderr}")
    print("Connexion OK")

    # dbt build (run + test)
    print("Lancement de dbt build...")
    result = subprocess.run(
        ["dbt", "build", "--profiles-dir", "."],
        cwd=dbt_dir, capture_output=True, text=True, timeout=600
    )
    print(result.stdout)

    if result.returncode != 0:
        raise Exception(f"dbt build echoue:\n{result.stderr}\n{result.stdout}")

    return {"status": "ok", "output": result.stdout[-1000:]}
```

---

### 4.3 Flow

**Chemin** : `f/pipeline/pipeline_spotify`

Le flow enchaine les deux scripts dans l'ordre :

```
Step A : bronze_to_silver
    │
    │  si erreur → flow arrete
    │
    ▼
Step B : silver_to_gold
    │
    │  si erreur → flow arrete
    │
    ▼
Termine — tables Gold a jour dans spotify_stream.duckdb
```

---

## 5. Comment utiliser

### 5.1 Demarrer Windmill

```bash
cd "/home/snowgou/EPSI/M1/ETL&ELT/code/varianteFour"
docker compose up -d
```

Attendre ~10 secondes puis ouvrir http://localhost:80.

### 5.2 Lancer le pipeline complet (Flow)

1. Ouvrir http://localhost:80
2. Se connecter (`test@test.fr` / `azertyuiop`)
3. Dans le menu de gauche, cliquer sur le workspace **spotify**
4. Aller dans **Flows**
5. Cliquer sur **pipeline_spotify**
6. Cliquer sur le bouton **Run** (en haut a droite)
7. Suivre l'execution dans les logs en temps reel

### 5.3 Lancer un seul script

Pour relancer uniquement une etape :

1. Aller dans **Scripts**
2. Cliquer sur `bronze_to_silver` ou `silver_to_gold`
3. Cliquer sur **Run**

Utile si par exemple seul le Gold doit etre regenere (pas besoin de reconvertir les JSON).

### 5.4 Planifier une execution automatique (cron)

1. Ouvrir le flow `pipeline_spotify`
2. Aller dans l'onglet **Schedules**
3. Cliquer sur **+ New schedule**
4. Entrer l'expression cron :

| Expression | Frequence |
|------------|-----------|
| `0 2 * * *` | Tous les jours a 2h du matin |
| `0 */6 * * *` | Toutes les 6 heures |
| `0 0 * * 1` | Tous les lundis a minuit |

5. Cliquer sur **Save**

### 5.5 Consulter l'historique des runs

1. Dans le menu de gauche, cliquer sur **Runs**
2. On voit la liste de tous les jobs passes avec :
   - Statut (succes / echec)
   - Duree
   - Logs complets
3. Cliquer sur un run pour voir les details et les logs de chaque step

---

## 6. Depannage

### Le script echoue avec "Aucun fichier JSON dans /data/"

Les volumes Docker ne sont pas montes. Verifier :

```bash
docker exec variantefour-windmill_worker-1 ls /data/*.json | head -3
```

Si rien n'apparait, redemarrer :

```bash
docker compose down && docker compose up -d
```

### dbt build echoue avec "IO Error: No files found"

Les vues DuckDB ne trouvent pas les Parquet. Verifier que `outputParquetV3` est monte au bon chemin :

```bash
docker exec variantefour-windmill_worker-1 \
  ls "/home/snowgou/EPSI/M1/ETL&ELT/code/varianteFour/outputParquetV3/"*.parquet | head -3
```

### dbt build echoue avec OOM (exit code 137)

Le worker manque de memoire. Dans `docker-compose.yml`, augmenter la limite :

```yaml
windmill_worker:
  deploy:
    resources:
      limits:
        memory: 4096M   # augmenter si besoin
```

Puis `docker compose down && docker compose up -d`.

### Windmill ne repond pas sur http://localhost:80

```bash
# Verifier que les containers tournent
docker compose ps

# Voir les logs du serveur
docker compose logs -f windmill_server
```

---

## 7. Commandes utiles

```bash
# Demarrer Windmill
docker compose up -d

# Arreter Windmill
docker compose down

# Voir les logs en temps reel
docker compose logs -f windmill_server

# Redemarrer apres modification du docker-compose.yml
docker compose down && docker compose up -d

# Verifier les volumes dans un worker
docker exec variantefour-windmill_worker-1 ls /data/
docker exec variantefour-windmill_worker-1 ls /dbt_spotify/
docker exec variantefour-windmill_worker-1 ls /spotify_stream.duckdb
```
