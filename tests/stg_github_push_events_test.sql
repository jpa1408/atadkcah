-- Test for required fields
SELECT
    event_id,
    event_type,
    event_created_at,
    user_id,
    repo_id
FROM {{ ref('stg_github_push_events') }}
WHERE
    event_id IS NULL
    OR event_type IS NULL
    OR event_created_at IS NULL
    OR user_id IS NULL
    OR repo_id IS NULL
HAVING COUNT(*) > 0

UNION ALL

-- Test for valid timestamps
SELECT
    event_id,
    event_type,
    event_created_at,
    user_id,
    repo_id
FROM {{ ref('stg_github_push_events') }}
WHERE
    event_created_at < '2010-01-01'  -- GitHub was founded in 2008
    OR event_created_at > CURRENT_TIMESTAMP()
HAVING COUNT(*) > 0

UNION ALL

-- Test for valid repository names
SELECT
    event_id,
    event_type,
    event_created_at,
    user_id,
    repo_id
FROM {{ ref('stg_github_push_events') }}
WHERE
    repo_owner IS NULL
    OR repo_name IS NULL
    OR LENGTH(repo_owner) = 0
    OR LENGTH(repo_name) = 0
HAVING COUNT(*) > 0

UNION ALL

-- Test for valid commit information
SELECT
    event_id,
    event_type,
    event_created_at,
    user_id,
    repo_id
FROM {{ ref('stg_github_push_events') }}
WHERE
    number_of_commits < 0
    OR (number_of_commits > 0 AND parsed_commits IS NULL)
HAVING COUNT(*) > 0 