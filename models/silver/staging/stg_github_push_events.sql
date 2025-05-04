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
    TRY_CAST(event_data["id"]::STRING AS VARCHAR) AS event_id,
    TRY_CAST(event_data["type"]::STRING AS VARCHAR) AS event_type,
    TRY_CAST(event_data["public"]::BOOLEAN AS BOOLEAN) AS is_public_event,
    TRY_TO_TIMESTAMP(event_data["created_at"]::STRING) AS event_created_at,

    -- Información del usuario
    TRY_CAST(event_data["actor"]["id"]::INT AS INT) AS user_id,
    TRY_CAST(event_data["actor"]["login"]::STRING AS VARCHAR) AS user_login,
    TRY_CAST(event_data["actor"]["display_login"]::STRING AS VARCHAR) AS user_display_login,
    TRY_CAST(event_data["actor"]["avatar_url"]::STRING AS VARCHAR) AS user_avatar_url,

    -- Detalles del repositorio
    TRY_CAST(event_data["repo"]["id"]::INT AS INT) AS repo_id,
    TRY_CAST(SPLIT_PART(event_data["repo"]["name"]::STRING, '/', 1) AS VARCHAR) AS repo_owner,
    TRY_CAST(SPLIT_PART(event_data["repo"]["name"]::STRING, '/', 2) AS VARCHAR) AS repo_name,

    -- Payload específico
    TRY_CAST(event_data["payload"]["push_id"]::INT AS INT) AS push_id,
    TRY_CAST(event_data["payload"]["repository_id"]::INT AS INT) AS repository_id,
    TRY_CAST(event_data["payload"]["ref"]::STRING AS VARCHAR) AS git_reference,
    TRY_CAST(REGEXP_REPLACE(event_data["payload"]["ref"]::STRING, 'refs/heads/', '') AS VARCHAR) AS branch_name,
    TRY_CAST(event_data["payload"]["head"]::STRING AS VARCHAR) AS head_commit_sha,
    TRY_CAST(event_data["payload"]["before"]::STRING AS VARCHAR) AS before_commit_sha,
    TRY_CAST(event_data["payload"]["size"]::INT AS INT) AS total_commits,
    TRY_CAST(event_data["payload"]["distinct_size"]::INT AS INT) AS distinct_commits,
    TRY_CAST(event_data["payload"]["commits"] AS ARRAY) AS commits_array

  FROM DATAHACK.bronze.raw_github_events
  WHERE
    event_data["type"] = 'PushEvent'
    AND event_data["id"] IS NOT NULL
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
LATERAL FLATTEN(input => commits_array) AS f(commit, index)
GROUP BY
  event_id, event_type, is_public_event, event_created_at,
  user_id, user_login, user_display_login, user_avatar_url,
  repo_id, repo_owner, repo_name,
  push_id, repository_id, git_reference, branch_name, 
  head_commit_sha, before_commit_sha, total_commits, distinct_commits,
  commits_array