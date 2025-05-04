-- models/gold/marts/dev_analytics/repo_health_metrics.sql
{{
  config(
    materialized='table',
    tags=['gold', 'dev_analytics']
  )
}}

SELECT
  repo_name,
  -- Frecuencia de commits
  COUNT(DISTINCT commit_sha) AS total_commits,
  DATEDIFF(DAY, MIN(event_created_at), MAX(event_created_at)) AS days_active,
  total_commits / NULLIF(days_active, 0) AS commits_per_day,
  
  -- Calidad de commits
  AVG(LENGTH(commit_message)) AS avg_commit_message_length,
  COUNT(DISTINCT author_email) AS unique_contributors
FROM {{ ref('stg_github_push_commits') }}
GROUP BY 1