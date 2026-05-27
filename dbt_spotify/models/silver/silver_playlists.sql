SELECT
    playlist_id,
    TRIM(playlist_name)                                      AS playlist_name,
    num_tracks,
    num_albums,
    num_followers,
    ROUND(num_albums * 1.0 / NULLIF(num_tracks, 0), 3)     AS album_diversity_ratio,
    CASE
        WHEN num_followers = 0    THEN 'no_followers'
        WHEN num_followers < 100  THEN 'small'
        WHEN num_followers < 1000 THEN 'medium'
        WHEN num_followers < 10000 THEN 'large'
        ELSE 'viral'
    END                                                      AS popularity_tier,
    CASE
        WHEN num_tracks < 20  THEN 'mini'
        WHEN num_tracks < 50  THEN 'standard'
        WHEN num_tracks < 100 THEN 'large'
        ELSE 'mega'
    END                                                      AS size_category,
    modified_at
FROM {{ source('spotify_stream', 'v_playlists') }}
WHERE playlist_id IS NOT NULL
