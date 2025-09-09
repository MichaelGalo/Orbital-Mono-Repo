CREATE OR REPLACE TABLE STAGED.NASA_DONKI AS
SELECT
    messageID AS message_id,
    messageType AS message_type,
    STRFTIME(STRPTIME(messageIssueTime, '%Y-%m-%dT%H:%MZ'), '%B %d, %Y %H:%M UTC') AS message_issue_time_human_readable,
    messageBody AS message_body
FROM RAW_DATA.NASA_DONKI
WHERE messageID IS NOT NULL
    AND messageType IS NOT NULL
    AND messageIssueTime IS NOT NULL
    AND messageBody IS NOT NULL;