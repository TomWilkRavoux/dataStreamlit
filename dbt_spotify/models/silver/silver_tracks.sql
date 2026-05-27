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
