-- Ventana tumbling de 1s por clave (COUNT + SUM), event time con watermark.
SET 'pipeline.name' = '${NAME}';
SET 'parallelism.default' = '${PARALLELISM}';
SET 'execution.checkpointing.interval' = '${COMMIT}';
SET 'table.dml-sync' = 'false';
SET 'table.local-time-zone' = 'UTC';

CREATE CATALOG lake WITH ('type' = 'paimon', 'warehouse' = 'file:///wh/flink');
USE CATALOG lake;

CREATE TEMPORARY TABLE events (
    order_id BIGINT,
    event_time BIGINT,
    amount BIGINT,
    ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    WATERMARK FOR ts AS ts - INTERVAL '${LAG_S}' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${TOPIC}',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = '${GROUP}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

INSERT INTO `default`.win_lake
SELECT
    order_id,
    CAST(TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', window_start) AS BIGINT) * 1000,
    CAST(TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', window_end) AS BIGINT) * 1000,
    COUNT(*),
    SUM(amount)
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(ts), INTERVAL '1' SECOND))
GROUP BY order_id, window_start, window_end;
