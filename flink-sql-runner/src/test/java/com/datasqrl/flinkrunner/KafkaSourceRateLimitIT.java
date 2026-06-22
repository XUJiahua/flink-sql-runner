/*
 * Copyright © 2026 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.flinkrunner;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.nextbreakpoint.flink.client.model.JobStatus;
import com.nextbreakpoint.flink.client.model.TerminationMode;
import java.time.Duration;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

class KafkaSourceRateLimitIT extends AbstractITSupport {

  private static final String TOPIC = "rate-limit-it";

  // Must stay in sync with kafka_rate_limit_produce.sql / kafka_rate_limit.sql.
  private static final long TOTAL_RECORDS = 600;
  private static final long RECORDS_PER_SECOND = 60;

  // The source physically cannot emit faster than the configured rate, so consuming all records
  // must take at least TOTAL_RECORDS / RECORDS_PER_SECOND seconds (10s here). We assert a generous
  // lower bound to prove throttling happened while staying robust against timing jitter. An
  // un-throttled bounded job over the same data finishes in a couple of seconds.
  private static final Duration MIN_EXPECTED_DURATION = Duration.ofSeconds(6);

  @Test
  void givenRateLimitedSource_whenConsumingBoundedTopic_thenConsumptionIsThrottled()
      throws Exception {
    var resultDao = connect();
    resultDao.createTable();
    resultDao.truncateTable();

    createTopic(TOPIC);
    produceRecords();

    long startNanos = System.nanoTime();
    String jobId = flinkRun("--sqlfile", "/it/sqlfile/kafka_rate_limit.sql");

    try {
      await()
          .atMost(120, SECONDS)
          .pollInterval(500, MILLISECONDS)
          .ignoreExceptions()
          .until(() -> client.getJobStatusInfo(jobId).getStatus() == JobStatus.FINISHED);

      Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

      assertThat(resultDao.getCount()).isEqualTo(TOTAL_RECORDS);
      assertThat(elapsed)
          .as(
              "Consuming %d records at %d records/second must take at least %s",
              TOTAL_RECORDS, RECORDS_PER_SECOND, MIN_EXPECTED_DURATION)
          .isGreaterThanOrEqualTo(MIN_EXPECTED_DURATION);
    } catch (Throwable t) {
      throw new AssertionError(readFlinkLogs(), t);
    } finally {
      cancelJob(jobId);
    }
  }

  private void produceRecords() throws Exception {
    String jobId = flinkRun("--sqlfile", "/it/sqlfile/kafka_rate_limit_produce.sql");

    await()
        .atMost(60, SECONDS)
        .pollInterval(500, MILLISECONDS)
        .ignoreExceptions()
        .until(() -> client.getJobStatusInfo(jobId).getStatus() == JobStatus.FINISHED);
  }

  private void createTopic(String topic) throws Exception {
    Container.ExecResult result =
        redpandaContainer.execInContainer(
            "rpk", "topic", "create", topic, "--brokers", "redpanda:9092", "--partitions", "1");

    assertThat(result.getExitCode())
        .withFailMessage(result.getStdout() + result.getStderr())
        .isZero();
  }

  private String readFlinkLogs() throws Exception {
    Container.ExecResult result =
        flinkContainer.execInContainer(
            "bash", "-c", "for f in /opt/flink/log/*; do echo ==== $f ====; tail -200 $f; done");
    return result.getStdout() + result.getStderr();
  }

  private void cancelJob(String jobId) {
    try {
      client.cancelJob(jobId, TerminationMode.CANCEL);
    } catch (Exception ignored) {
    }
  }

  private ResultDao connect() {
    var mappedPort = postgresContainer.getMappedPort(5432);
    var jdbi =
        Jdbi.create(
            "jdbc:postgresql://localhost:" + mappedPort + "/datasqrl", "postgres", "postgres");
    jdbi.installPlugin(new SqlObjectPlugin());
    return jdbi.onDemand(ResultDao.class);
  }

  interface ResultDao {

    @SqlUpdate("CREATE TABLE IF NOT EXISTS rate_limit_results (id BIGINT)")
    void createTable();

    @SqlUpdate("TRUNCATE rate_limit_results")
    void truncateTable();

    @SqlQuery("SELECT count(*) FROM rate_limit_results")
    long getCount();
  }
}
