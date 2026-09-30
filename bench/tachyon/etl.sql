INSERT INTO etl_lake
SELECT order_id, status, source_version, amount, event_time
FROM orders
WHERE status <> 'cancelled';
