INSERT INTO multiwin_lake
SELECT service, COUNT(*) AS events, SUM(amount) AS total
FROM combined_streams
WHERE event_time IS NOT NULL
GROUP BY service;
