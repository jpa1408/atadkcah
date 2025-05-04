-- models/silver/staging/stg_github_fork_events.sql
{{
  config(
    materialized='view',
    tags=['silver', 'staging', 'fork_events']
  )
}}

SELECT
  -- Metadatos del evento
  TRY_CAST(event_data:"id"::STRING AS VARCHAR) AS event_id,
  TRY_CAST(event_data:"type"::STRING AS VARCHAR) AS event_type,
  TRY_CAST(event_data:"public"::BOOLEAN AS BOOLEAN) AS fork_is_public_event,
  TRY_TO_TIMESTAMP(event_data:"created_at"::STRING) AS fork_event_created_at,
  
  -- Información del usuario
  TRY_CAST(event_data:"actor":"id"::INT AS INT) AS fork_user_id,
  TRY_CAST(event_data:"actor":"login"::STRING AS VARCHAR) AS fork_user_login,
  TRY_CAST(event_data:"actor":"display_login"::STRING AS VARCHAR) AS fork_user_display_login,
  
  -- Información del repositorio
  TRY_CAST(event_data:"repo":"id"::INT AS INT) AS fork_repo_id,
  TRY_CAST(event_data:"repo":"name"::STRING AS VARCHAR) AS fork_original_repo,
  TRY_CAST(SPLIT_PART(event_data:"repo":"name"::STRING, '/', 1) AS VARCHAR) AS fork_repo_owner,
  TRY_CAST(SPLIT_PART(event_data:"repo":"name"::STRING, '/', 2) AS VARCHAR) AS fork_repo_name,
  
  -- Información específica del fork
  TRY_CAST(event_data:"payload":"forkee":"id"::INT AS INT) AS fork_forkee_id,
  TRY_CAST(event_data:"payload":"forkee":"full_name"::STRING AS VARCHAR) AS forked_repo,
  TRY_CAST(SPLIT_PART(event_data:"payload":"forkee":"full_name"::STRING, '/', 1) AS VARCHAR) AS forked_repo_owner,
  TRY_CAST(SPLIT_PART(event_data:"payload":"forkee":"full_name"::STRING, '/', 2) AS VARCHAR) AS forked_repo_name
  
FROM DATAHACK.public.raw_github_events
WHERE event_data:"type"::STRING = 'ForkEvent'
  AND event_data:"id" IS NOT NULL