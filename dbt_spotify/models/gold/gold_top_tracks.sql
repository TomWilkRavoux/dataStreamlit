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
