{% set columns_to_select = [
    "repo_name",
    "actor_login",
    "event_type",
    "event_created_at"
] %}

WITH daily_events AS (
    SELECT
        DATE_TRUNC('day', event_created_at) AS activity_date,
        repo_name,
        actor_login,
        event_type,
        COUNT(*) AS event_count
    FROM {{ ref('stg_github_events') }}
    GROUP BY 1, 2, 3, 4
),

activity_metrics AS (
    SELECT
        activity_date,
        repo_name,
        COUNT(DISTINCT actor_login) AS unique_contributors,
        COUNT(CASE WHEN event_type = 'PushEvent' THEN 1 END) AS push_events,
        COUNT(CASE WHEN event_type = 'PullRequestEvent' THEN 1 END) AS pull_requests,
        COUNT(CASE WHEN event_type = 'IssuesEvent' THEN 1 END) AS issues,
        COUNT(CASE WHEN event_type = 'WatchEvent' THEN 1 END) AS stars
    FROM daily_events
    GROUP BY 1, 2
)

SELECT
    activity_date,
    repo_name,
    unique_contributors,
    push_events,
    pull_requests,
    issues,
    stars
FROM activity_metrics
ORDER BY activity_date DESC, repo_name
