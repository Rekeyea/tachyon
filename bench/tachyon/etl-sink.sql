INSERT INTO etl_out
SELECT order_id, status, source_version, amount, event_time
FROM orders
WHERE status <> 'cancelled';
