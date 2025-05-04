-- models/gold/marts/core/dim_repositories.sql
{{
  config(
    materialized='table',
    tags=['gold', 'core', 'dimension'],
    unique_key='repo_id'
  )
}}

WITH base_repos AS (
  SELECT DISTINCT
    push_repo_id as repo_id,
    push_repo_name as repo_name,
    push_repo_owner as repo_owner
  FROM {{ ref('stg_github_push_events') }}
  WHERE push_repo_id IS NOT NULL
    AND push_repo_name IS NOT NULL
    AND push_repo_owner IS NOT NULL
),

repo_activity AS (
  SELECT
    push_repo_id as repo_id,
    MIN(push_event_created_at) AS first_activity_date,
    MAX(push_event_created_at) AS last_activity_date,
    COUNT(DISTINCT event_id) AS total_events
  FROM {{ ref('stg_github_push_events') }}
  WHERE push_repo_id IS NOT NULL
  GROUP BY 1
),

fork_counts AS (
  SELECT 
    original_repo, 
    COUNT(*) AS fork_count
  FROM {{ ref('stg_github_fork_events') }}
  WHERE original_repo IS NOT NULL
  GROUP BY 1
)

SELECT
  r.repo_id,
  r.repo_name,
  r.repo_owner,
  a.first_activity_date,
  a.last_activity_date,
  a.total_events,
  COALESCE(f.fork_count, 0) AS fork_count,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM base_repos r
LEFT JOIN repo_activity a 
  ON r.repo_id = a.repo_id
LEFT JOIN fork_counts f 
  ON r.repo_name = f.original_repo