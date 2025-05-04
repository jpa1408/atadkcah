-- models/gold/marts/dev_analytics/repo_health_metrics.sql
{{
  config(
    materialized='table',
    tags=['gold', 'dev_analytics']
  )
}}

SELECT
  push_repo_name AS repo_name,
  -- Frecuencia de commits
  COUNT(DISTINCT commit_sha) AS total_commits,
  GREATEST(1, DATEDIFF(DAY, MIN(push_event_created_at), MAX(push_event_created_at))) AS days_active,
  total_commits::FLOAT / days_active AS commits_per_day,
  
  -- Calidad de commits
  AVG(LENGTH(commit_message)) AS avg_commit_message_length,
  COUNT(DISTINCT author_email) AS unique_contributors
FROM {{ ref('stg_github_push_commits') }}
WHERE push_repo_name IS NOT NULL
GROUP BY 1