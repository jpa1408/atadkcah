-- models/gold/marts/dev_analytics/team_performance.sql
{{
  config(
    materialized='table',
    tags=['gold', 'dev_analytics', 'team']
  )
}}

WITH team_activity AS (
  SELECT
    r.repo_owner AS team_name,
    DATE_TRUNC('WEEK', f.activity_date) AS activity_week,
    COUNT(DISTINCT f.total_events) AS total_events,
    COUNT(DISTINCT f.repo_name) AS active_repos,
    COUNT(DISTINCT f.active_users) AS active_members,
    SUM(f.push_events) AS total_commits
  FROM {{ ref('fact_daily_activity') }} f
  JOIN {{ ref('dim_repositories') }} r ON f.repo_name = r.repo_name
  GROUP BY 1, 2
),

fork_metrics AS (
  SELECT
    SPLIT_PART(fork_original_repo, '/', 1) AS team_name,
    COUNT(*) AS forks_received
  FROM {{ ref('stg_github_fork_events') }}
  WHERE fork_original_repo IS NOT NULL
  GROUP BY 1
),

response_metrics AS (
  SELECT
    push_repo_owner AS team_name,
    AVG(DATEDIFF('HOUR', commit_time, next_commit_time)) AS avg_hours_between_commits
  FROM (
    SELECT
      push_repo_owner,
      push_event_created_at AS commit_time,
      LEAD(push_event_created_at) OVER (PARTITION BY push_repo_owner ORDER BY push_event_created_at) AS next_commit_time
    FROM {{ ref('stg_github_push_events') }}
    WHERE push_repo_owner IS NOT NULL AND push_event_created_at IS NOT NULL
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