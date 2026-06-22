CREATE TABLE rate_limit_gen (
  record_id BIGINT
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '600',
  'rows-per-second' = '10000',
  'fields.record_id.kind' = 'sequence',
  'fields.record_id.start' = '0',
  'fields.record_id.end' = '599'
);

CREATE TABLE rate_limit_input (
  id BIGINT,
  payload STRING
) WITH (
  'connector' = 'kafka-safe',
  'topic' = 'rate-limit-it',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'format' = 'json'
);

INSERT INTO rate_limit_input
SELECT
  CAST(record_id AS BIGINT),
  CONCAT('record-', CAST(record_id AS STRING))
FROM rate_limit_gen;
