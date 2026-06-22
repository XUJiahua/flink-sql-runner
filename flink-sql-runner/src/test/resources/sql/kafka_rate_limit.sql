CREATE TABLE rate_limit_source (
  id BIGINT,
  payload STRING
) WITH (
  'connector' = 'kafka-safe',
  'topic' = 'rate-limit-it',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'rate-limit-it',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'scan.parallelism' = '1',
  'scan.rate-limit.records-per-second' = '60',
  'format' = 'json'
);

CREATE TABLE rate_limit_results (
  id BIGINT
) WITH (
  'connector' = 'jdbc',
  'driver' = 'org.postgresql.Driver',
  'url' = '${JDBC_URL}',
  'username' = '${JDBC_USERNAME}',
  'password' = '${JDBC_PASSWORD}',
  'table-name' = 'rate_limit_results'
);

INSERT INTO rate_limit_results
SELECT id FROM rate_limit_source;
