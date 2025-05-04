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
    push_event_created_at AS event_created_at,
    push_user_login AS user_login,
    push_repo_name AS repo_name,
    push_branch_name AS branch_name,
    'Push' AS activity_type,
    COUNT(DISTINCT commit_sha) AS metric_value
  FROM {{ ref('stg_github_push_commits') }}
  WHERE event_id IS NOT NULL
    AND push_event_created_at IS NOT NULL
    AND push_user_login IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6
),

fork_events AS (
  SELECT
    event_id,
    fork_event_created_at AS event_created_at,
    fork_user_login AS user_login,
    fork_repo_name AS repo_name,
    NULL AS branch_name,
    'Fork' AS activity_type,
    1 AS metric_value  -- Cada Fork cuenta como 1 actividad
  FROM {{ ref('stg_github_fork_events') }}
  WHERE event_id IS NOT NULL
    AND fork_event_created_at IS NOT NULL
    AND fork_user_login IS NOT NULL
)

-- Unión de métricas de diferentes tipos de eventos
SELECT * FROM push_events
UNION ALL
SELECT * FROM fork_events