{{
    config(
        materialized='table',
        tags=['gold', 'metrics', 'data_quality']
    )
}}

WITH push_events_stats AS (
    SELECT
        DATE_TRUNC('day', push_event_created_at) AS metric_date,
        COUNT(*) AS total_events,
        COUNT(DISTINCT push_user_id) AS unique_users,
        COUNT(DISTINCT push_repo_id) AS unique_repositories,
        AVG(number_of_commits) AS avg_commits_per_push,
        COUNT(CASE WHEN parsed_commits IS NULL THEN 1 END) AS events_without_commits,
        COUNT(CASE WHEN push_repo_owner IS NULL OR push_repo_name IS NULL THEN 1 END) AS events_without_repo_info
    FROM {{ ref('stg_github_push_events') }}
    GROUP BY 1
),

quality_metrics AS (
    SELECT
        metric_date,
        total_events,
        unique_users,
        unique_repositories,
        avg_commits_per_push,
        events_without_commits,
        events_without_repo_info,
        (events_without_commits + events_without_repo_info) / NULLIF(total_events, 0) * 100 AS error_rate,
        CASE
            WHEN error_rate < 1 THEN 'Excellent'
            WHEN error_rate < 5 THEN 'Good'
            WHEN error_rate < 10 THEN 'Fair'
            ELSE 'Poor'
        END AS quality_rating
    FROM push_events_stats
)

SELECT
    metric_date,
    total_events,
    unique_users,
    unique_repositories,
    ROUND(avg_commits_per_push, 2) AS avg_commits_per_push,
    events_without_commits,
    events_without_repo_info,
    ROUND(error_rate, 2) AS error_rate,
    quality_rating,
    CURRENT_TIMESTAMP() AS last_updated
FROM quality_metrics
ORDER BY metric_date DESC 