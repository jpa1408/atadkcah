-- models/silver/staging/stg_github_push_commits.sql
{{
  config(
    materialized='table',
    tags=['silver', 'staging', 'commits']
  )
}}

SELECT
  e.event_id,
  e.push_event_created_at,
  e.push_user_login,
  e.push_repo_owner,
  e.push_repo_name,
  TRY_CAST(c.value:sha::STRING AS VARCHAR) AS commit_sha,
  TRY_CAST(c.value:author:name::STRING AS VARCHAR) AS author_name,
  TRY_CAST(c.value:author:email::STRING AS VARCHAR) AS author_email,
  TRY_CAST(c.value:message::STRING AS VARCHAR) AS commit_message,
  TRY_CAST(c.value:url::STRING AS VARCHAR) AS commit_url,
  TRY_CAST(c.value:distinct::BOOLEAN AS BOOLEAN) AS is_distinct_commit,
  e.push_branch_name,
  e.push_id
FROM {{ ref('stg_github_push_events') }} e,
LATERAL FLATTEN(input => e.push_commits_array) c