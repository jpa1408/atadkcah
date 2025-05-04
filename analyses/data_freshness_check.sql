-- Data Freshness Check
WITH latest_events AS (
    SELECT
        MAX(event_created_at) AS latest_event_time,
        COUNT(*) AS total_events
    FROM {{ ref('stg_github_push_events') }}
),

freshness_metrics AS (
    SELECT
        latest_event_time,
        total_events,
        DATEDIFF('hour', latest_event_time, CURRENT_TIMESTAMP()) AS hours_since_last_event,
        CASE
            WHEN hours_since_last_event <= 24 THEN 'Fresh'
            WHEN hours_since_last_event <= 48 THEN 'Stale'
            ELSE 'Very Stale'
        END AS freshness_status
    FROM latest_events
)

SELECT
    latest_event_time,
    total_events,
    hours_since_last_event,
    freshness_status,
    CASE
        WHEN freshness_status = 'Fresh' THEN '✅'
        WHEN freshness_status = 'Stale' THEN '⚠️'
        ELSE '❌'
    END AS status_icon
FROM freshness_metrics 