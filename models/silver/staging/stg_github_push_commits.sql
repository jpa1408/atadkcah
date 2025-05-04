-- models/silver/staging/stg_github_push_commits.sql
{{
  config(
    materialized='table',
    tags=['silver', 'staging', 'commits']
  )
}}

SELECT
  e.event_id,
  e.event_created_at,
  e.user_login,
  e.repo_name,
  c.value:sha::STRING AS commit_sha,
  c.value:author:name::STRING AS author_name,
  c.value:author:email::STRING AS author_email,
  c.value:message::STRING AS commit_message,
  c.value:url::STRING AS commit_url
FROM {{ ref('stg_github_push_events') }} e,
LATERAL FLATTEN(input => e.commits_array) c