-- models/gold/marts/core/dim_repositories.sql
{{
  config(
    materialized='table',
    tags=['gold', 'core', 'dimension'],
    unique_key='repo_id'
  )
}}

WITH repo_activity AS (
  SELECT
    repo_id,
    repo_name,
    MIN(event_created_at) AS first_activity_date,
    MAX(event_created_at) AS last_activity_date,
    COUNT(DISTINCT event_id) AS total_events
  FROM {{ ref('stg_github_push_events') }}
  GROUP BY 1, 2
)

SELECT
  r.repo_id,
  r.repo_name,
  r.repo_owner,
  a.first_activity_date,
  a.last_activity_date,
  a.total_events,
  COALESCE(f.fork_count, 0) AS fork_count
FROM {{ ref('stg_github_push_events') }} r
LEFT JOIN (
  SELECT original_repo, COUNT(*) AS fork_count
  FROM {{ ref('stg_github_fork_events') }}
  GROUP BY 1
) f ON r.repo_name = f.original_repo
LEFT JOIN repo_activity a ON r.repo_id = a.repo_id