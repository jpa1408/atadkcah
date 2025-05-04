-- models/gold/marts/core/fact_user_contributions.sql
{{
  config(
    materialized='incremental',
    tags=['gold', 'core', 'users'],
    unique_key='user_id',
    incremental_strategy='merge'
  )
}}

WITH source_data AS (
  SELECT
    push_user_id as user_id,
    push_user_login as user_login,
    event_id,
    push_repo_id as repo_id,
    push_event_created_at as event_created_at
  FROM {{ ref('stg_github_push_events') }}
  WHERE push_user_id IS NOT NULL
    AND push_user_login IS NOT NULL
    AND event_id IS NOT NULL
    AND push_repo_id IS NOT NULL
    AND push_event_created_at IS NOT NULL
  {% if is_incremental() %}
    AND push_event_created_at >= (
      SELECT DATEADD(day, -1, MAX(last_contribution_date))
      FROM {{ this }}
    )
  {% endif %}
)

SELECT
  user_id,
  user_login,
  COUNT(DISTINCT event_id) AS total_contributions,
  COUNT(DISTINCT repo_id) AS repos_contributed_to,
  MIN(event_created_at) AS first_contribution_date,
  MAX(event_created_at) AS last_contribution_date,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM source_data
GROUP BY 1, 2