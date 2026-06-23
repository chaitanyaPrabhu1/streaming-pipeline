-- Sample near-real-time queries once data is landing.

-- Busiest wikis in the last 15 minutes.
SELECT wiki, SUM(event_count) AS events
FROM edits_per_minute
WHERE window_start > now() - interval '15' minute
GROUP BY wiki
ORDER BY events DESC
LIMIT 10;

-- Per-minute edit volume for English Wikipedia (a time series to chart).
SELECT window_start, event_count, edit_count, bot_count
FROM edits_per_minute
WHERE wiki = 'enwiki'
ORDER BY window_start DESC
LIMIT 60;

-- Bot vs human share over the last hour.
SELECT
    SUM(bot_count)                         AS bot_events,
    SUM(event_count) - SUM(bot_count)      AS human_events,
    round(1.0 * SUM(bot_count) / SUM(event_count), 3) AS bot_share
FROM edits_per_minute
WHERE window_start > now() - interval '1' hour;

-- Largest single content changes today (queries the raw enriched events).
SELECT event_time, wiki, title, `user`, bytes_changed
FROM events
WHERE dt = date_format(current_date, '%Y-%m-%d')
ORDER BY abs(bytes_changed) DESC
LIMIT 20;
