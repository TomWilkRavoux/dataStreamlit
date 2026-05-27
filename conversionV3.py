from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import orjson
import polars as pl

DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = Path(__file__).parent / "outputParquetV3Test"

# Axe 1 : 8 workers
MAX_WORKERS = 8


def process_file(path: Path) -> str:
    # Axe 2 : orjson + open rb (buffered, évite d'allouer 32MB en bytes upfront)
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
    # Axe 3 : lz4 légèrement plus rapide que snappy en écriture
    # Axe 4 : tout dans un seul répertoire plat
    pl.DataFrame(playlists).write_parquet(OUTPUT_DIR / f"{stem}_playlists.parquet", compression="lz4")
    pl.DataFrame(tracks).write_parquet(OUTPUT_DIR / f"{stem}_tracks.parquet", compression="lz4")

    return stem


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    json_files = sorted(DATA_DIR.glob("*.json"))
    print(f"Found {len(json_files)} JSON files — {MAX_WORKERS} workers")

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, p) for p in json_files]
        for i, future in enumerate(as_completed(futures), 1):
            future.result()
            if i % 100 == 0:
                print(f"  {i}/{len(json_files)} files written...")

    print(f"Done: {len(list(OUTPUT_DIR.glob('*.parquet')))} parquet files")


if __name__ == "__main__":
    import time
    t0 = time.perf_counter()
    main()
    print(f"Done in {time.perf_counter() - t0:.1f}s")
