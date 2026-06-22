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
package com.datasqrl.flinkrunner.connector.kafka;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

/** Configuration options for rate limiting the {@code kafka-safe} scan source. */
public class RateLimitOptions {

  public static final ConfigOption<Long> SCAN_RATE_LIMIT_RECORDS_PER_SECOND =
      ConfigOptions.key("scan.rate-limit.records-per-second")
          .longType()
          .noDefaultValue()
          .withDescription(
              "The maximum number of records per second emitted by the Kafka source, applied "
                  + "across all source subtasks. The limit is divided evenly among the parallel "
                  + "subtasks. When unset, rate limiting is disabled.");

  public static void validateRateLimitOptions(ReadableConfig tableOptions) {
    var recordsPerSecond = tableOptions.getOptional(SCAN_RATE_LIMIT_RECORDS_PER_SECOND);
    if (recordsPerSecond.isPresent() && recordsPerSecond.get() <= 0) {
      throw new ValidationException(
          String.format(
              "'%s' must be greater than 0, but was %d.",
              SCAN_RATE_LIMIT_RECORDS_PER_SECOND.key(), recordsPerSecond.get()));
    }
  }

  private RateLimitOptions() {}
}
