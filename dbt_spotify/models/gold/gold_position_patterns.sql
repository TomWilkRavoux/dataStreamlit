SELECT
    position,
    COUNT(DISTINCT track_uri)       AS unique_tracks,
    COUNT(DISTINCT artist_name)     AS unique_artists,
    ROUND(AVG(duration_min), 3)     AS avg_duration_min,
    COUNT(*)                        AS total_appearances
FROM {{ ref('silver_tracks') }}
WHERE position <= 50
GROUP BY position
ORDER BY position
