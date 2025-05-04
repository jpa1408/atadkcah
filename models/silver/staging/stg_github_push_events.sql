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
  *,
  ARRAY_SIZE(commits_array) AS number_of_commits,
  ARRAY_AGG(
    OBJECT_CONSTRUCT(
      'sha', TRY_CAST(commit:sha::STRING AS VARCHAR),
      'message', TRY_CAST(commit:message::STRING AS VARCHAR),
      'author_name', TRY_CAST(commit:author:name::STRING AS VARCHAR),
      'author_email', TRY_CAST(commit:author:email::STRING AS VARCHAR),
      'distinct', TRY_CAST(commit:distinct::BOOLEAN AS BOOLEAN),
      'url', TRY_CAST(commit:url::STRING AS VARCHAR)
    )
  ) WITHIN GROUP (ORDER BY INDEX) AS parsed_commits
FROM source_data,
LATERAL FLATTEN(input => push_commits_array) AS f(commit, index)
GROUP BY
  event_id, event_type, push_is_public_event, push_event_created_at,
  push_user_id, push_user_login, push_user_display_login, push_user_avatar_url,
  push_repo_id, push_repo_owner, push_repo_name,
  push_id, push_repository_id, push_git_reference, push_branch_name, 
  push_head_commit_sha, push_before_commit_sha, push_total_commits, push_distinct_commits,
  push_commits_array