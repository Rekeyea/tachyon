INSERT INTO win_lake
SELECT order_id, window_start, window_end, COUNT(*) AS n, SUM(amount) AS amount
FROM events
GROUP BY order_id, TUMBLE(event_time, INTERVAL '1' SECOND);
