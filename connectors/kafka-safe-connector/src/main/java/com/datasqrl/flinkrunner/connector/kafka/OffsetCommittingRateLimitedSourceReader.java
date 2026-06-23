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

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimitedSourceReader;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.util.Preconditions;

/**
 * A {@link RateLimitedSourceReader} that also forwards checkpoint notifications to the wrapped
 * delegate reader.
 *
 * <p>Flink's {@link RateLimitedSourceReader} overrides {@link #notifyCheckpointComplete(long)} to
 * notify only the {@link RateLimiter} and intentionally does not forward the call to the delegate
 * reader. For a {@code KafkaSource}, the offset commit back to the broker happens inside the
 * delegate {@code KafkaSourceReader#notifyCheckpointComplete(long)}. Dropping that call means
 * offsets are never committed on checkpoint: the consumer group never shows up on the broker and
 * external lag tooling cannot observe progress, even though record processing and checkpointing are
 * perfectly healthy (the manifestation is {@code committedOffset = -1} with both {@code
 * commitsSucceeded} and {@code commitsFailed} stuck at {@code 0}).
 *
 * <p>This subclass restores the delegate notification while preserving the rate-limiting behavior
 * provided by the superclass. Offset commit correctness for recovery is unaffected either way,
 * since Flink restores from checkpointed offsets rather than the broker-side group offsets; this
 * only re-enables the broker-side commit used for monitoring.
 *
 * @param <E> the output type of the reader.
 * @param <SplitT> the type of the source splits.
 */
class OffsetCommittingRateLimitedSourceReader<E, SplitT extends SourceSplit>
    extends RateLimitedSourceReader<E, SplitT> {

  private final SourceReader<E, SplitT> delegate;

  OffsetCommittingRateLimitedSourceReader(
      SourceReader<E, SplitT> delegate, RateLimiter<SplitT> rateLimiter) {
    super(delegate, rateLimiter);
    this.delegate = Preconditions.checkNotNull(delegate, "delegate must not be null.");
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // super notifies the rate limiter only; the delegate (e.g. KafkaSourceReader) performs the
    // actual offset commit to the broker and must be notified explicitly.
    super.notifyCheckpointComplete(checkpointId);
    delegate.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    super.notifyCheckpointAborted(checkpointId);
    delegate.notifyCheckpointAborted(checkpointId);
  }
}
