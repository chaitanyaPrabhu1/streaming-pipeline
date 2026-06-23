-- Final per-minute metrics: SUM the batch partials back together.
-- This is what makes the design correct under at-least-once delivery and
-- out-of-order arrival — every batch's partial for a window simply adds in,
-- regardless of when (or how many times across batches) the events arrived.
CREATE OR REPLACE VIEW edits_per_minute AS
SELECT
    window_start,
    wiki,
    SUM(event_count)         AS event_count,
    SUM(edit_count)          AS edit_count,
    SUM(new_page_count)      AS new_page_count,
    SUM(bot_count)           AS bot_count,
    SUM(total_bytes_changed) AS total_bytes_changed,
    SUM(abs_bytes_changed)   AS abs_bytes_changed
FROM agg_partials
GROUP BY window_start, wiki;
