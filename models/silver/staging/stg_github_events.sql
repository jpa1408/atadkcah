{{
    config(
        materialized='view',
        tags=['silver', 'staging']
    )
}}

WITH bronze_events AS (
    SELECT
        event_data:"id"::string as event_id,
        event_data:"type"::string as event_type,
        event_data:"repo":"id"::string as repo_id,
        event_data:"repo":"name"::string as repo_name,
        event_data:"actor":"id"::string as actor_id,
        event_data:"actor":"login"::string as actor_login,
        event_data:"created_at"::string as event_created_at,
        event_data:"payload" as payload
    FROM DATAHACK.public.raw_github_events
)

SELECT
    event_id,
    event_type,
    repo_id,
    repo_name,
    actor_id,
    actor_login,
    event_created_at,
    
    -- PushEvent
    CASE WHEN event_type = 'PushEvent' THEN payload:"ref"::string END as push_ref,
    CASE WHEN event_type = 'PushEvent' THEN payload:"size"::integer END as push_size,
    CASE WHEN event_type = 'PushEvent' THEN payload:"distinct_size"::integer END as push_distinct_size,
    CASE WHEN event_type = 'PushEvent' THEN payload:"head"::string END as push_head,
    CASE WHEN event_type = 'PushEvent' THEN payload:"before"::string END as push_before,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits"::integer END as push_commits,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits_sha"::string END as push_commits_sha,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits_author_email"::string END as push_commits_author_email,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits_author_name"::string END as push_commits_author_name,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits_message"::string END as push_commits_message,
    CASE WHEN event_type = 'PushEvent' THEN payload:"commits_distinct"::string END as push_commits_distinct,

    -- PullRequestEvent
    CASE WHEN event_type = 'PullRequestEvent' THEN payload:"number"::integer END as pr_number,
    CASE WHEN event_type = 'PullRequestEvent' THEN payload:"action"::string END as pr_action,
    CASE WHEN event_type = 'PullRequestEvent' THEN payload:"pull_request_title"::string END as pr_title,
    CASE WHEN event_type = 'PullRequestEvent' THEN payload:"pull_request_user_login"::string END as pr_user_login,
    CASE WHEN event_type = 'PullRequestEvent' THEN payload:"pull_request_merged"::string END as pr_merged,

    -- IssuesEvent
    CASE WHEN event_type = 'IssuesEvent' THEN payload:"number"::integer END as issue_number,
    CASE WHEN event_type = 'IssuesEvent' THEN payload:"action"::string END as issue_action,
    CASE WHEN event_type = 'IssuesEvent' THEN payload:"issue_title"::string END as issue_title,
    CASE WHEN event_type = 'IssuesEvent' THEN payload:"issue_user_login"::string END as issue_user_login,
    CASE WHEN event_type = 'IssuesEvent' THEN payload:"issue_labels"::string END as issue_labels
FROM bronze_events
