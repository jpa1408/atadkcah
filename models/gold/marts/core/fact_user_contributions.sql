-- models/gold/marts/core/fact_user_contributions.sql
{{
  config(
    materialized='incremental',
    tags=['gold', 'core', 'users'],
    unique_key='user_id'
  )
}}

SELECT
  user_id,
  user_login,
  COUNT(DISTINCT event_id) AS total_contributions,
  COUNT(DISTINCT repo_id) AS repos_contributed_to,
  MIN(event_created_at) AS first_contribution_date,
  MAX(event_created_at) AS last_contribution_date
FROM {{ ref('stg_github_push_events') }}
GROUP BY 1, 2