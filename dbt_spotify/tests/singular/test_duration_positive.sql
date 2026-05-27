SELECT *
FROM {{ ref('silver_tracks') }}
WHERE duration_ms <= 0
