CREATE TABLE IF NOT EXISTS STAGED.NASA_DONKI AS
SELECT
    messageID,
    messageType,
    STRFTIME(STRPTIME(messageIssueTime, '%Y-%m-%dT%H:%MZ'), '%B %d, %Y %H:%M UTC') AS message_issue_time_human_readable,
    messageBody
FROM RAW.NASA_DONKI;