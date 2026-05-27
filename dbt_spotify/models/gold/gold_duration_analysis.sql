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
