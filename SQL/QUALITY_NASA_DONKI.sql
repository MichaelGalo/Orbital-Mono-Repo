SELECT
    message_id,
    message_type,
    message_issue_time_human_readable,
    message_body
FROM STAGED.NASA_DONKI
WHERE message_id IS NULL
    AND message_type IS NULL
    AND message_issue_time_human_readable IS NULL
    AND message_body IS NULL;