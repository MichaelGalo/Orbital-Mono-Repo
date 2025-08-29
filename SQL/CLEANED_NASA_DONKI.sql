CREATE TABLE IF NOT EXISTS CLEANED.NASA_DONKI AS
SELECT
    messageID AS message_id,
    messageType AS message_type,
    message_issue_time_human_readable AS message_issue_time,
    messageBody AS message_body
FROM STAGED.NASA_DONKI;