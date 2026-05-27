import glob
import os
import ujson as json
import polars as pl
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

INPUT_FOLDER = r"/home/snowgou/EPSI/M1/ETL&ELT/profSolus/epsi_m1_dataia_etlelt/data"

OUTPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_demo")

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

json_files = glob.glob(f"{INPUT_FOLDER}/*.json")

def process_json(json_file):
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        playlists = data.get("playlists", [])

        track_rows = []
        playlist_rows = []

        for playlist in playlists:
            playlist_id = playlist["pid"]

            playlist_rows.append((
                playlist_id,
                playlist["name"],
                playlist["num_tracks"],
                playlist["num_albums"],
                playlist["num_followers"],
                playlist["modified_at"],
            ))

            for track in playlist["tracks"]:
                track_rows.append((
                    playlist_id,
                    track["track_uri"],
                    track["track_name"],
                    track["artist_name"],
                    track["artist_uri"],
                    track["album_name"],
                    track["album_uri"],
                    track["duration_ms"],
                    track["pos"],
                ))

        df_tracks = pl.DataFrame(track_rows, schema=[
            "playlist_id", "track_uri", "track_name", "artist_name",
            "artist_uri", "album_name", "album_uri", "duration_ms", "position"
        ], orient="row")

        df_playlists = pl.DataFrame(playlist_rows, schema=[
            "playlist_id", "playlist_name", "num_tracks",
            "num_albums", "num_followers", "modified_at"
        ], orient="row")

        base_filename = os.path.basename(json_file).replace(".json", "")
        df_tracks.write_parquet(
            f"{OUTPUT_FOLDER}/{base_filename}_tracks.parquet",
            compression="zstd"
        )
        df_playlists.write_parquet(
            f"{OUTPUT_FOLDER}/{base_filename}_playlists.parquet",
            compression="zstd"
        )
    except Exception as e:
        print(f"Erreur avec {json_file}: {e}")

if __name__ == "__main__":
    num_workers = max(cpu_count() - 1, 1)

    print(f"Utilisation de {num_workers} processus...")
    with Pool(num_workers) as p:
        list(tqdm(p.imap(process_json, json_files), total=len(json_files), desc="Playlists en traitement"))