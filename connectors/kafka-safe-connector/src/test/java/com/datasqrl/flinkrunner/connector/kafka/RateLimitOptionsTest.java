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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Stream;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RateLimitOptionsTest {

  @ParameterizedTest(name = "Should pass validation: recordsPerSecond={0}")
  @MethodSource("validConfigs")
  void shouldNotThrowForValidConfigs(Long recordsPerSecond) {
    ReadableConfig config = mockConfig(recordsPerSecond);

    assertThatCode(() -> RateLimitOptions.validateRateLimitOptions(config))
        .doesNotThrowAnyException();
  }

  static Stream<Arguments> validConfigs() {
    return Stream.of(Arguments.of((Long) null), Arguments.of(1L), Arguments.of(1_000_000L));
  }

  @ParameterizedTest(name = "Should fail validation: recordsPerSecond={0}")
  @MethodSource("invalidConfigs")
  void shouldThrowForInvalidConfigs(Long recordsPerSecond) {
    ReadableConfig config = mockConfig(recordsPerSecond);

    assertThatThrownBy(() -> RateLimitOptions.validateRateLimitOptions(config))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("scan.rate-limit.records-per-second")
        .hasMessageContaining("must be greater than 0");
  }

  static Stream<Arguments> invalidConfigs() {
    return Stream.of(Arguments.of(0L), Arguments.of(-1L), Arguments.of(-1_000L));
  }

  private static ReadableConfig mockConfig(Long recordsPerSecond) {
    ReadableConfig config = mock(ReadableConfig.class);
    when(config.getOptional(RateLimitOptions.SCAN_RATE_LIMIT_RECORDS_PER_SECOND))
        .thenReturn(Optional.ofNullable(recordsPerSecond));
    return config;
  }
}
