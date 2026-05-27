SELECT
    p.playlist_id,
    p.playlist_name,
    p.num_tracks,
    p.num_albums,
    p.num_followers,
    p.album_diversity_ratio,
    p.popularity_tier,
    p.size_category,
    ROUND(AVG(t.duration_min), 3)   AS avg_track_duration_min,
    COUNT(DISTINCT t.artist_name)   AS nb_distinct_artists
FROM {{ ref('silver_playlists') }} p
LEFT JOIN {{ ref('silver_tracks') }} t
    ON p.playlist_id = t.playlist_id
GROUP BY
    p.playlist_id, p.playlist_name, p.num_tracks, p.num_albums,
    p.num_followers, p.album_diversity_ratio, p.popularity_tier, p.size_category
ORDER BY p.num_followers DESC
