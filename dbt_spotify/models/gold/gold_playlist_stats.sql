WITH track_stats AS (
    SELECT
        playlist_id,
        ROUND(AVG(duration_min), 3)     AS avg_track_duration_min,
        COUNT(DISTINCT artist_name)     AS nb_distinct_artists
    FROM {{ ref('silver_tracks') }}
    GROUP BY playlist_id
)
SELECT
    p.playlist_id,
    p.playlist_name,
    p.num_tracks,
    p.num_albums,
    p.num_followers,
    p.album_diversity_ratio,
    p.popularity_tier,
    p.size_category,
    t.avg_track_duration_min,
    t.nb_distinct_artists
FROM {{ ref('silver_playlists') }} p
LEFT JOIN track_stats t
    ON p.playlist_id = t.playlist_id
ORDER BY p.num_followers DESC
