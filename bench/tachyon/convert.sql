INSERT INTO convert_lake
SELECT order_id, status, source_version, amount, event_time
FROM combined_orders;
