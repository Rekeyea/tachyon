-- ETL stateless desde Kinesis hacia un stream de Kinesis: filtra y proyecta
-- por pedido. El schema y el binding físico vienen de pipeline.yaml.
INSERT INTO orders_out
SELECT
    order_id,
    status,
    source_version,
    amount
FROM orders
WHERE status <> 'cancelled';
