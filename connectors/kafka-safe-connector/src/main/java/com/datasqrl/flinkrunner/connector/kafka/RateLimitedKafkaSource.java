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

import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimitedSourceReader;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.Preconditions;

/**
 * A {@link Source} that delegates to a {@link KafkaSource} and applies rate limiting to the records
 * it emits.
 *
 * <p>All split enumeration, serialization and type handling is delegated to the wrapped {@link
 * KafkaSource}. The only behavioral change is in {@link #createReader(SourceReaderContext)}, where
 * the reader produced by the delegate is wrapped in a {@link RateLimitedSourceReader} backed by a
 * {@link RateLimiterStrategy#perSecond(double)} strategy.
 *
 * <p>The configured rate is the cumulative rate across all source subtasks; the strategy divides it
 * evenly among the parallel subtasks.
 *
 * @param <T> the output type of the source.
 */
public class RateLimitedKafkaSource<T>
    implements Source<T, KafkaPartitionSplit, KafkaSourceEnumState>,
        ResultTypeQueryable<T>,
        LineageVertexProvider {

  private static final long serialVersionUID = 1L;

  private final KafkaSource<T> delegate;
  private final double recordsPerSecond;

  public RateLimitedKafkaSource(KafkaSource<T> delegate, double recordsPerSecond) {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate must not be null.");
    Preconditions.checkArgument(
        recordsPerSecond > 0,
        "recordsPerSecond must be greater than 0, but was %s.",
        recordsPerSecond);
    this.recordsPerSecond = recordsPerSecond;
  }

  @Override
  public Boundedness getBoundedness() {
    return delegate.getBoundedness();
  }

  @Override
  public SourceReader<T, KafkaPartitionSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    SourceReader<T, KafkaPartitionSplit> reader = delegate.createReader(readerContext);
    return new RateLimitedSourceReader<>(
        reader, createRateLimiter(readerContext.currentParallelism()));
  }

  @SuppressWarnings("unchecked")
  private RateLimiter<KafkaPartitionSplit> createRateLimiter(int parallelism) {
    return RateLimiterStrategy.perSecond(recordsPerSecond).createRateLimiter(parallelism);
  }

  @Override
  public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> createEnumerator(
      SplitEnumeratorContext<KafkaPartitionSplit> enumContext) throws Exception {
    return delegate.createEnumerator(enumContext);
  }

  @Override
  public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> restoreEnumerator(
      SplitEnumeratorContext<KafkaPartitionSplit> enumContext, KafkaSourceEnumState checkpoint)
      throws Exception {
    return delegate.restoreEnumerator(enumContext, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<KafkaPartitionSplit> getSplitSerializer() {
    return delegate.getSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<KafkaSourceEnumState> getEnumeratorCheckpointSerializer() {
    return delegate.getEnumeratorCheckpointSerializer();
  }

  @Override
  public Set<? extends WatermarkDeclaration> declareWatermarks() {
    return delegate.declareWatermarks();
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return delegate.getProducedType();
  }

  @Override
  public SourceLineageVertex getLineageVertex() {
    return delegate.getLineageVertex();
  }

  public KafkaSource<T> getDelegate() {
    return delegate;
  }

  public double getRecordsPerSecond() {
    return recordsPerSecond;
  }
}
