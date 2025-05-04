{{
    config(
        materialized='view',
        tags=['silver', 'staging']
    )
}}

WITH bronze_events AS (
    SELECT
        event_id,
        type as event_type,
        actor:"id"::string as actor_id,
        actor:"login"::string as actor_login,
        repo:"id"::string as repo_id,
        repo:"name"::string as repo_name,
        created_at as event_created_at,
        payload
    FROM DATAHACK.bronze.raw_github_events
)

SELECT
    event_id,
    event_type,
    repo_id,
    repo_name,
    actor_id,
    actor_login,
    event_created_at,
    
    -- Campos específicos para PushEvent
    CASE WHEN event_type = 'PushEvent' THEN payload:"ref"::string END as payload_ref,
    CASE WHEN event_type = 'PushEvent' THEN payload:"size"::integer END as payload_size,
    CASE WHEN event_type = 'PushEvent' THEN payload:"distinct_size"::integer END as payload_distinct_size,
    CASE WHEN event_type = 'PushEvent' THEN payload:"head"::string END as payload_head,
    CASE WHEN event_type = 'PushEvent' THEN payload:"before"::string END as payload_before,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits"::integer END as payload_commits,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits_sha"::string END as payload_commits_sha,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits_author_email"::string END as payload_commits_author_email,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits_author_name"::string END as payload_commits_author_name,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits_message"::string END as payload_commits_message,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits_distinct"::string END as payload_commits_distinct,
    
    -- Campos específicos para PullRequestEvent
    CASE WHEN event_type = 'PullRequestEvent' THEN payload:"number"::integer END as payload_number,
    CASE WHEN event_type = 'PullRequestEvent' THEN payload:"action"::string END as payload_action,
    CASE WHEN event_type = 'PullRequestEvent' THEN payload:"pull_request_title"::string END as payload_pull_request_title,
    CASE WHEN event_type = 'PullRequestEvent' THEN payload:"pull_request_user_login"::string END as payload_pull_request_user_login,
    CASE WHEN event_type = 'PullRequestEvent' THEN payload:"pull_request_merged"::string END as payload_pull_request_merged,
    
    -- Campos específicos para IssuesEvent
    CASE WHEN event_type = 'IssuesEvent' THEN payload:"number"::integer END as payload_number,
    CASE WHEN event_type = 'IssuesEvent' THEN payload:"action"::string END as payload_action,
    CASE WHEN event_type = 'IssuesEvent' THEN payload:"issue_title"::string END as payload_issue_title,
    CASE WHEN event_type = 'IssuesEvent' THEN payload:"issue_user_login"::string END as payload_issue_user_login,
    CASE WHEN event_type = 'IssuesEvent' THEN payload:"issue_labels"::string END as payload_issue_labels
FROM bronze_events
