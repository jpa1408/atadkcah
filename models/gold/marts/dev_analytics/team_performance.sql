-- models/gold/marts/dev_analytics/team_performance.sql
{{
  config(
    materialized='table',
    tags=['gold', 'dev_analytics', 'team']
  )
}}

WITH team_activity AS (
  SELECT
    repo_owner AS team_name,
    DATE_TRUNC('WEEK', event_created_at) AS activity_week,
    COUNT(DISTINCT event_id) AS total_events,
    COUNT(DISTINCT repo_name) AS active_repos,
    COUNT(DISTINCT user_login) AS active_members,
    SUM(total_commits) AS total_commits
  FROM {{ ref('fact_daily_activity') }} f
  JOIN {{ ref('dim_repositories') }} r ON f.repo_name = r.repo_name
  GROUP BY 1, 2
),

fork_metrics AS (
  SELECT
    SPLIT_PART(original_repo, '/', 1) AS team_name,
    COUNT(*) AS forks_received
  FROM {{ ref('stg_github_fork_events') }}
  GROUP BY 1
),

response_metrics AS (
  SELECT
    repo_owner AS team_name,
    AVG(DATEDIFF('HOUR', commit_time, next_commit_time)) AS avg_hours_between_commits
  FROM (
    SELECT
      repo_owner,
      event_created_at AS commit_time,
      LEAD(event_created_at) OVER (PARTITION BY repo_owner ORDER BY event_created_at) AS next_commit_time
    FROM {{ ref('stg_github_push_events') }}
  )
  GROUP BY 1
)

SELECT
  t.team_name,
  t.activity_week,
  t.total_events,
  t.active_repos,
  t.active_members,
  t.total_commits,
  f.forks_received,
  r.avg_hours_between_commits,
  -- Cálculo de productividad relativa
  ROUND(t.total_commits / NULLIF(t.active_members, 0), 2) AS commits_per_member
FROM team_activity t
LEFT JOIN fork_metrics f ON t.team_name = f.team_name
LEFT JOIN response_metrics r ON t.team_name = r.team_name