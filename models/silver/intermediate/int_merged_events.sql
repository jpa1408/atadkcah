-- models/silver/intermediate/int_merged_events.sql
{{
  config(
    materialized='table',
    tags=['silver', 'intermediate', 'activity_metrics']
  )
}}

WITH push_events AS (
  SELECT
    event_id,
    event_created_at,
    user_login,
    repo_name,
    branch_name,
    'Push' AS activity_type,
    COUNT(DISTINCT commit_sha) AS metric_value
  FROM {{ ref('stg_github_push_commits') }}
  GROUP BY 1, 2, 3, 4, 5, 6
),

fork_events AS (
  SELECT
    event_id,
    event_created_at,
    user_login,
    original_repo AS repo_name,
    NULL AS branch_name,
    'Fork' AS activity_type,
    1 AS metric_value  -- Cada Fork cuenta como 1 actividad
  FROM {{ ref('stg_github_fork_events') }}
)

-- Unión de métricas de diferentes tipos de eventos
SELECT * FROM push_events
UNION ALL
SELECT * FROM fork_events