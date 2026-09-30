-- ETL stateless: filtra y proyecta JSON de Redpanda a una tabla PK de Paimon.
-- Placeholders (${...}) los reemplaza bench/run.sh.
SET 'pipeline.name' = '${NAME}';
SET 'parallelism.default' = '${PARALLELISM}';
SET 'execution.checkpointing.interval' = '${COMMIT}';
SET 'table.dml-sync' = 'false';

CREATE CATALOG lake WITH ('type' = 'paimon', 'warehouse' = 'file:///wh/flink');
USE CATALOG lake;

CREATE TEMPORARY TABLE orders (
    order_id BIGINT,
    status STRING,
    source_version BIGINT,
    amount DOUBLE,
    event_time BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = '${TOPIC}',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = '${GROUP}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

INSERT INTO `default`.etl_lake
SELECT order_id, status, source_version, amount, event_time
FROM orders
WHERE status <> 'cancelled';
