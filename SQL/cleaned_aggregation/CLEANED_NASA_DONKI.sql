CREATE OR REPLACE TABLE CLEANED.NASA_DONKI AS
SELECT
    message_id,
    message_type,
    message_issue_time_human_readable AS message_issue_time,
    message_body
FROM STAGED.NASA_DONKI;