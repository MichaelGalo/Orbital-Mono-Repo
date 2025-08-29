SELECT
    messageID,
    messageType,
    STRFTIME(STRPTIME(messageIssueTime, '%Y-%m-%dT%H:%MZ'), '%B %d, %Y %H:%M UTC') AS message_issue_time_human_readable,
    messageBody
FROM RAW.NASA_DONKI
WHERE messageID IS NULL
    AND messageType IS NULL
    AND messageIssueTime IS NULL
    AND messageBody IS NULL;