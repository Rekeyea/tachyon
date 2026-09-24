-- ETL stateless: filtra, proyecta y agrega por pedido.
-- El schema y el binding físico vienen de pipeline.yaml.
INSERT INTO orders_lake
SELECT
    order_id,
    status,
    source_version,
    event_time,
    SUM(amount) AS order_total
FROM orders
WHERE status <> 'cancelled'
GROUP BY order_id, status, source_version, event_time;
