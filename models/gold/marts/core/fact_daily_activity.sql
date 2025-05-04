-- models/gold/marts/core/fact_daily_activity.sql
{{
  config(
    materialized='table',
    tags=['gold', 'core', 'activity']
  )
}}

SELECT
  DATE(event_created_at) AS activity_date,
  repo_name,
  COUNT(DISTINCT event_id) AS total_events,
  COUNT(DISTINCT CASE WHEN activity_type = 'Push' THEN event_id END) AS push_events,
  COUNT(DISTINCT CASE WHEN activity_type = 'Fork' THEN event_id END) AS fork_events,
  COUNT(DISTINCT user_login) AS active_users,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('int_merged_events') }}
WHERE event_created_at IS NOT NULL
  AND repo_name IS NOT NULL
GROUP BY 1, 2
ORDER BY activity_date DESC, total_events DESC