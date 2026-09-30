-- Tabla de salida del benchmark (win). La misma DDL para los dos motores:
-- solo cambia el warehouse (${WAREHOUSE}). La crea Paimon (Java) con el SQL
-- client de Flink; Tachyon escribe sobre esa misma definición.
SET 'sql-client.execution.result-mode' = 'tableau';
CREATE CATALOG lake WITH ('type' = 'paimon', 'warehouse' = 'file://${WAREHOUSE}');
USE CATALOG lake;
CREATE TABLE `default`.win_lake (
    order_id BIGINT,
    window_start BIGINT,
    window_end BIGINT,
    n BIGINT,
    amount BIGINT,
    PRIMARY KEY (order_id, window_start) NOT ENFORCED
) WITH (
    'bucket' = '8',
    'bucket-key' = 'order_id',
    'sequence.field' = 'window_end',
    'file.format' = 'parquet',
    'file.compression' = 'zstd',
    'write-only' = 'true'
);
