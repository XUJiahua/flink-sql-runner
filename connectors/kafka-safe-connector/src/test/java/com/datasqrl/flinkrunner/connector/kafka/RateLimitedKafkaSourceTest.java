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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimitedSourceReader;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class RateLimitedKafkaSourceTest {

  @Test
  void shouldRejectNonPositiveRate() {
    KafkaSource<RowData> delegate = kafkaSource();

    assertThatThrownBy(() -> new RateLimitedKafkaSource<>(delegate, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new RateLimitedKafkaSource<>(delegate, -5))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldRejectNullDelegate() {
    assertThatThrownBy(() -> new RateLimitedKafkaSource<RowData>(null, 100))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void shouldExposeDelegateAndRate() {
    KafkaSource<RowData> delegate = kafkaSource();

    RateLimitedKafkaSource<RowData> source = new RateLimitedKafkaSource<>(delegate, 100);

    assertThat(source.getDelegate()).isSameAs(delegate);
    assertThat(source.getRecordsPerSecond()).isEqualTo(100.0);
  }

  @Test
  void shouldDelegateBoundedness() {
    KafkaSource<RowData> delegate = kafkaSource();
    when(delegate.getBoundedness()).thenReturn(Boundedness.CONTINUOUS_UNBOUNDED);

    RateLimitedKafkaSource<RowData> source = new RateLimitedKafkaSource<>(delegate, 100);

    assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
  }

  @Test
  void shouldDelegateLineageVertex() {
    KafkaSource<RowData> delegate = kafkaSource();
    SourceLineageVertex lineageVertex = mock(SourceLineageVertex.class);
    when(delegate.getLineageVertex()).thenReturn(lineageVertex);

    RateLimitedKafkaSource<RowData> source = new RateLimitedKafkaSource<>(delegate, 100);

    assertThat(source.getLineageVertex()).isSameAs(lineageVertex);
  }

  @Test
  void shouldWrapDelegateReaderWithRateLimitedReader() throws Exception {
    KafkaSource<RowData> delegate = kafkaSource();
    SourceReaderContext readerContext = mock(SourceReaderContext.class);
    when(readerContext.currentParallelism()).thenReturn(2);

    @SuppressWarnings("unchecked")
    SourceReader<RowData, KafkaPartitionSplit> innerReader = mock(SourceReader.class);
    when(delegate.createReader(readerContext)).thenReturn(innerReader);

    RateLimitedKafkaSource<RowData> source = new RateLimitedKafkaSource<>(delegate, 100);

    SourceReader<RowData, KafkaPartitionSplit> reader = source.createReader(readerContext);

    assertThat(reader).isInstanceOf(RateLimitedSourceReader.class);
  }

  @SuppressWarnings("unchecked")
  private static KafkaSource<RowData> kafkaSource() {
    return mock(KafkaSource.class);
  }
}
