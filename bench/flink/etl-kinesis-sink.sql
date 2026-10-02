-- ETL stateless: filtra y proyecta JSON de Kinesis (floCi) a un stream de
-- Kinesis de salida (floCi). Placeholders (${...}) los reemplaza bench/run.sh.
SET 'pipeline.name' = '${NAME}';
SET 'parallelism.default' = '${PARALLELISM}';
SET 'execution.checkpointing.interval' = '${COMMIT}';
SET 'table.dml-sync' = 'false';

CREATE TEMPORARY TABLE orders (
    order_id BIGINT,
    status STRING,
    source_version BIGINT,
    amount DOUBLE,
    event_time BIGINT
) WITH (
    'connector' = 'kinesis',
    'stream.arn' = 'arn:aws:kinesis:us-east-1:000000000000:stream/${STREAM}',
    'aws.region' = 'us-east-1',
    'aws.endpoint' = 'http://localhost:4566',
    'source.init.position' = 'TRIM_HORIZON',
    'aws.credentials.provider' = 'BASIC',
    'aws.credentials.basic.accesskeyid' = 'test',
    'aws.credentials.basic.secretkey' = 'test',
    'format' = 'json'
);

CREATE TEMPORARY TABLE etl_out (
    order_id BIGINT,
    status STRING,
    source_version BIGINT,
    amount DOUBLE,
    event_time BIGINT
) WITH (
    'connector' = 'kinesis',
    'stream.arn' = 'arn:aws:kinesis:us-east-1:000000000000:stream/${OUT_STREAM}',
    'aws.region' = 'us-east-1',
    'aws.endpoint' = 'http://localhost:4566',
    'sink.partition-by' = 'order_id',
    'aws.credentials.provider' = 'BASIC',
    'aws.credentials.basic.accesskeyid' = 'test',
    'aws.credentials.basic.secretkey' = 'test',
    'format' = 'json'
);

INSERT INTO etl_out
SELECT order_id, status, source_version, amount, event_time
FROM orders
WHERE status <> 'cancelled';
