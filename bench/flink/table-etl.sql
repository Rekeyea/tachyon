-- Tabla de salida del benchmark (etl). La misma DDL para los dos motores:
-- solo cambia el warehouse (${WAREHOUSE}). La crea Paimon (Java) con el SQL
-- client de Flink; Tachyon escribe sobre esa misma definición.
SET 'sql-client.execution.result-mode' = 'tableau';
CREATE CATALOG lake WITH ('type' = 'paimon', 'warehouse' = 'file://${WAREHOUSE}');
USE CATALOG lake;
CREATE TABLE `default`.etl_lake (
    order_id BIGINT,
    status STRING,
    source_version BIGINT,
    amount DOUBLE,
    event_time BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '8',
    'sequence.field' = 'source_version',
    'file.format' = 'parquet',
    'file.compression' = 'zstd',
    -- Sin compactación en el writer: Tachyon tampoco compacta en el camino
    -- caliente (la compactación es otro proceso). Favorece a Flink.
    'write-only' = 'true'
);
