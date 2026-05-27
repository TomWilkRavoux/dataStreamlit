SELECT *
FROM {{ ref('silver_playlists') }}
WHERE num_followers < 0
