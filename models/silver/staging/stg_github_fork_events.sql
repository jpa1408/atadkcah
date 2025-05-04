-- models/silver/staging/stg_github_fork_events.sql
{{
  config(
    materialized='view',
    tags=['silver', 'staging', 'fork_events']
  )
}}

SELECT
  event_data["id"]::STRING AS event_id,
  event_data["type"]::STRING AS event_type,
  event_data["actor"]["login"]::STRING AS user_login,
  event_data["repo"]["name"]::STRING AS original_repo,
  event_data["payload"]["forkee"]["full_name"]::STRING AS forked_repo
FROM DATAHACK.public.raw_github_events
WHERE event_data["type"] = 'ForkEvent'