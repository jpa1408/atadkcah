-- models/silver/staging/stg_github_push_events.sql
{{
  config(
    materialized='view',
    tags=['silver', 'staging', 'push_events']
  )
}}

WITH source_data AS (
  SELECT
    -- Metadatos del evento
    TRY_CAST(event_data:"id"::STRING AS VARCHAR) AS event_id,
    -- Solo campos relevantes para PushEvent
    TRY_CAST(event_data:"type"::STRING AS VARCHAR) AS event_type,
    TRY_CAST(event_data:"public"::BOOLEAN AS BOOLEAN) AS push_is_public_event,
    TRY_TO_TIMESTAMP(event_data:"created_at"::STRING) AS push_event_created_at,

    -- Información del usuario
    TRY_CAST(event_data:"actor":"id"::INT AS INT) AS push_user_id,
    TRY_CAST(event_data:"actor":"login"::STRING AS VARCHAR) AS push_user_login,
    TRY_CAST(event_data:"actor":"display_login"::STRING AS VARCHAR) AS push_user_display_login,
    TRY_CAST(event_data:"actor":"avatar_url"::STRING AS VARCHAR) AS push_user_avatar_url,

    -- Detalles del repositorio
    TRY_CAST(event_data:"repo":"id"::INT AS INT) AS push_repo_id,
    TRY_CAST(SPLIT_PART(event_data:"repo":"name"::STRING, '/', 1) AS VARCHAR) AS push_repo_owner,
    TRY_CAST(SPLIT_PART(event_data:"repo":"name"::STRING, '/', 2) AS VARCHAR) AS push_repo_name,

    -- Payload específico
    TRY_CAST(event_data:"payload":"push_id"::INT AS INT) AS push_id,
    TRY_CAST(event_data:"payload":"repository_id"::INT AS INT) AS push_repository_id,
    TRY_CAST(event_data:"payload":"ref"::STRING AS VARCHAR) AS push_git_reference,
    TRY_CAST(REGEXP_REPLACE(event_data:"payload":"ref"::STRING, 'refs/heads/', '') AS VARCHAR) AS push_branch_name,
    TRY_CAST(event_data:"payload":"head"::STRING AS VARCHAR) AS push_head_commit_sha,
    TRY_CAST(event_data:"payload":"before"::STRING AS VARCHAR) AS push_before_commit_sha,
    TRY_CAST(event_data:"payload":"size"::INT AS INT) AS push_total_commits,
    TRY_CAST(event_data:"payload":"distinct_size"::INT AS INT) AS push_distinct_commits,
    event_data:"payload":"commits" AS push_commits_array

  FROM DATAHACK.public.raw_github_events
  WHERE
    event_data:"type"::STRING = 'PushEvent'
    AND event_data:"id" IS NOT NULL
)

SELECT
  sd.event_id,
  sd.event_type,
  sd.push_is_public_event,
  sd.push_event_created_at,
  sd.push_user_id,
  sd.push_user_login,
  sd.push_user_display_login,
  sd.push_user_avatar_url,
  sd.push_repo_id,
  sd.push_repo_owner,
  sd.push_repo_name,
  sd.push_id,
  sd.push_repository_id,
  sd.push_git_reference,
  sd.push_branch_name,
  sd.push_head_commit_sha,
  sd.push_before_commit_sha,
  sd.push_total_commits,
  sd.push_distinct_commits,
  sd.push_commits_array,
  ARRAY_SIZE(sd.push_commits_array) AS number_of_commits,
  ARRAY_AGG(
    OBJECT_CONSTRUCT(
      'sha', TRY_CAST(f.value:sha::STRING AS VARCHAR),
      'message', TRY_CAST(f.value:message::STRING AS VARCHAR),
      'author_name', TRY_CAST(f.value:author:name::STRING AS VARCHAR),
      'author_email', TRY_CAST(f.value:author:email::STRING AS VARCHAR),
      'distinct', TRY_CAST(f.value:distinct::BOOLEAN AS BOOLEAN),
      'url', TRY_CAST(f.value:url::STRING AS VARCHAR)
    )
  ) WITHIN GROUP (ORDER BY f.index) AS parsed_commits
FROM source_data sd,
LATERAL FLATTEN(input => sd.push_commits_array) AS f
GROUP BY
  sd.event_id, sd.event_type, sd.push_is_public_event, sd.push_event_created_at,
  sd.push_user_id, sd.push_user_login, sd.push_user_display_login, sd.push_user_avatar_url,
  sd.push_repo_id, sd.push_repo_owner, sd.push_repo_name,
  sd.push_id, sd.push_repository_id, sd.push_git_reference, sd.push_branch_name, 
  sd.push_head_commit_sha, sd.push_before_commit_sha, sd.push_total_commits, sd.push_distinct_commits,
  sd.push_commits_array