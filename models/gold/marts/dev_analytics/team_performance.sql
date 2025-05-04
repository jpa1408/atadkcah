-- models/gold/marts/dev_analytics/team_performance.sql
{{
  config(
    materialized='table',
    tags=['gold', 'dev_analytics', 'team']
  )
}}

-- Paso 1: Crear una dimensión de equipos unificada (para tener un listado completo de equipos/owners)
team_dimension AS (
  -- Equipos de actividad principal
  SELECT DISTINCT
    LOWER(TRIM(repo_owner)) AS team_name_normalized,
    repo_owner AS team_name_original
  FROM {{ ref('dim_repositories') }}
  WHERE repo_owner IS NOT NULL

  UNION

  -- Equipos con forks
  SELECT DISTINCT
    LOWER(TRIM(SPLIT_PART(fork_original_repo, '/', 1))) AS team_name_normalized,
    SPLIT_PART(fork_original_repo, '/', 1) AS team_name_original
  FROM {{ ref('stg_github_fork_events') }}
  WHERE fork_original_repo IS NOT NULL
),

-- Paso 2: Medir la actividad principal
team_activity AS (
  SELECT
    LOWER(TRIM(r.repo_owner)) AS team_name_normalized,
    DATE_TRUNC('WEEK', f.activity_date) AS activity_week,
    COUNT(DISTINCT f.total_events) AS total_events,
    COUNT(DISTINCT f.repo_name) AS active_repos,
    COUNT(DISTINCT f.active_users) AS active_members,
    SUM(f.push_events) AS total_commits
  FROM {{ ref('fact_daily_activity') }} f
  JOIN {{ ref('dim_repositories') }} r ON f.repo_name = r.repo_name
  GROUP BY 1, 2
),

-- Paso 3: Medir los forks
fork_metrics AS (
  SELECT
    LOWER(TRIM(SPLIT_PART(fork_original_repo, '/', 1))) AS team_name_normalized,
    COUNT(*) AS forks_received
  FROM {{ ref('stg_github_fork_events') }}
  WHERE fork_original_repo IS NOT NULL
  GROUP BY 1
),

-- Paso 4: Medir tiempos de respuesta entre commits
response_metrics AS (
  SELECT
    LOWER(TRIM(push_repo_owner)) AS team_name_normalized,
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

-- Paso 5: Unir todas las métricas a través del nombre normalizado del equipo
SELECT
  td.team_name_original AS team_name,
  t.activity_week,
  COALESCE(t.total_events, 0) AS total_events,
  COALESCE(t.active_repos, 0) AS active_repos,
  COALESCE(t.active_members, 0) AS active_members,
  COALESCE(t.total_commits, 0) AS total_commits,
  COALESCE(f.forks_received, 0) AS forks_received,
  COALESCE(r.avg_hours_between_commits, 0) AS avg_hours_between_commits,
  -- Cálculo de productividad relativa
  ROUND(COALESCE(t.total_commits, 0) / NULLIF(COALESCE(t.active_members, 0), 0), 2) AS commits_per_member
FROM team_dimension td
LEFT JOIN team_activity t ON td.team_name_normalized = t.team_name_normalized
LEFT JOIN fork_metrics f ON td.team_name_normalized = f.team_name_normalized
LEFT JOIN response_metrics r ON td.team_name_normalized = r.team_name_normalized
WHERE t.activity_week IS NOT NULL  -- Solo incluir equipos con actividad