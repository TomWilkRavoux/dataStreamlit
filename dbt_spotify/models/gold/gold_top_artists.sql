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
