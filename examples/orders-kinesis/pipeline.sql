-- ETL stateless desde Kinesis: filtra y proyecta por pedido.
-- El schema y el binding físico vienen de pipeline.yaml.
INSERT INTO orders_lake
SELECT
    order_id,
    status,
    source_version,
    amount
FROM orders
WHERE status <> 'cancelled';
