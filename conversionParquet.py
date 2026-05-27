import json
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = Path(__file__).parent / "outputParquet"


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    json_files = sorted(DATA_DIR.glob("*.json"))
    print(f"Found {len(json_files)} JSON files")

    writer_playlists = None
    writer_tracks = None

    for i, path in enumerate(json_files, 1):
        with open(path) as f:
            data = json.load(f)

        playlists_rows = []
        tracks_rows = []

        for pl_data in data["playlists"]:
            pid = pl_data["pid"]
            playlists_rows.append({
                "pid": pid,
                "name": pl_data["name"],
                "collaborative": pl_data["collaborative"],
                "modified_at": pl_data["modified_at"],
                "num_tracks": pl_data["num_tracks"],
                "num_albums": pl_data["num_albums"],
                "num_followers": pl_data["num_followers"],
                "num_edits": pl_data["num_edits"],
                "duration_ms": pl_data["duration_ms"],
                "num_artists": pl_data["num_artists"],
            })
            for track in pl_data.get("tracks", []):
                tracks_rows.append({
                    "pid": pid,
                    "pos": track["pos"],
                    "artist_name": track["artist_name"],
                    "track_uri": track["track_uri"],
                    "artist_uri": track["artist_uri"],
                    "track_name": track["track_name"],
                    "album_uri": track["album_uri"],
                    "duration_ms": track["duration_ms"],
                    "album_name": track["album_name"],
                })

        arrow_playlists = pl.DataFrame(playlists_rows).to_arrow()
        arrow_tracks = pl.DataFrame(tracks_rows).to_arrow()

        if writer_playlists is None:
            writer_playlists = pq.ParquetWriter(
                OUTPUT_DIR / "playlists.parquet", arrow_playlists.schema, compression="snappy"
            )
            writer_tracks = pq.ParquetWriter(
                OUTPUT_DIR / "tracks.parquet", arrow_tracks.schema, compression="snappy"
            )

        writer_playlists.write_table(arrow_playlists)
        writer_tracks.write_table(arrow_tracks)

        if i % 100 == 0:
            print(f"  {i}/{len(json_files)} files processed...")

    writer_playlists.close()
    writer_tracks.close()

    out_pl = OUTPUT_DIR / "playlists.parquet"
    out_tr = OUTPUT_DIR / "tracks.parquet"
    print(f"Written: {out_pl} ({out_pl.stat().st_size / 1_000_000:.1f} MB)")
    print(f"Written: {out_tr} ({out_tr.stat().st_size / 1_000_000:.1f} MB)")


if __name__ == "__main__":
    import time
    t0 = time.perf_counter()
    main()
    print(f"Done in {time.perf_counter() - t0:.1f}s")
