package org.myorg.quickstart;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Counting application for FlinkForward 2016
 */
public class FlinkForward2016Checkpointing {

	interface MyOptions extends FlinkPipelineOptions {
		@Description("Fail after a timeout in seconds")
		@Default.Long(0)
		void setFailAfter(long timeout);
		long getFailAfter();
	}

	public static void main(String[] args) throws Exception {

		MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
		options.setRunner(FlinkRunner.class);
		options.setNumberOfExecutionRetries(Integer.MAX_VALUE);
		options.setExecutionRetryDelay(3000L);
		options.setCheckpointingInterval(1000L);

		final long failAfter = options.getFailAfter();

		Pipeline p = Pipeline.create(options);

		PCollection<Long> source = p.apply(Read.from(new MySource()));

		source
				.apply(ParDo.of(new CountExposer()));

		source
				.apply(ParDo.of(new Preprocessor(failAfter)))

				.apply(Window.<Long>into(FixedWindows.of(Duration.standardSeconds(3)))
					.triggering(AfterWatermark.pastEndOfWindow())
					.withAllowedLateness(Duration.ZERO)
					.discardingFiredPanes())

				.apply(Combine.globally(new PrintWindowCounts()).withoutDefaults());

		p.run();
	}

	private static class CountExposer extends DoFn<Long, Long> {

		private final Aggregator<Long, Long> counterTracker =
				createAggregator("maxCount", new SerializableFunction<Iterable<Long>, Long>() {
					@Override
					public Long apply(Iterable<Long> longs) {
						long max = 0;
						for (long val : longs) {
							max = Math.max(max, val);
						}
						return max;
					}
				});

		@ProcessElement
		public void processElement(ProcessContext c) {
			counterTracker.addValue(c.element());
		}
	}

	private static class Preprocessor extends DoFn<Long, Long> {

		private long failAfter;
		private long lastStarted;

		public Preprocessor() {
			this(0);
		}

		public Preprocessor(long failAfter) {
			this.failAfter = failAfter * 1000;
		}

		@Setup
		public void setup() {
			lastStarted = System.currentTimeMillis();
		}

		@ProcessElement
		public void processElement(ProcessContext c) {
			if (failAfter > 0 && System.currentTimeMillis() - lastStarted >= 10000) {
				throw new RuntimeException("FAILING");
			}
			c.output(c.element());
		}
	}

	private static class PrintWindowCounts implements SerializableFunction<Iterable<Long>, Long> {

		@Override
		public Long apply(Iterable<Long> input) {
			//System.out.println("Firing window");
			long count = 0;
			for (long val : input) {
				System.out.println(val);
				count+=val;
			}
			return count;
		}
	}

	private static class MySource extends UnboundedSource<Long, LongCheckpointMark> {


		@Override
		public List<? extends UnboundedSource<Long, LongCheckpointMark>> generateInitialSplits(int desiredSplits, PipelineOptions pipelineOptions) throws Exception {
			List<MySource> sources = new ArrayList<>(desiredSplits);
			for (int i = 0; i < desiredSplits; i++) {
				sources.add(new MySource());
			}
			return sources;
		}

		@Override
		public UnboundedReader<Long> createReader(PipelineOptions pipelineOptions, @Nullable LongCheckpointMark longCheckpointMark) throws IOException {
			return new Reader(this, longCheckpointMark);
		}

		@Nullable
		@Override
		public Coder<LongCheckpointMark> getCheckpointMarkCoder() {
			return SerializableCoder.of(LongCheckpointMark.class);
		}

		@Override
		public void validate() {}

		@Override
		public Coder<Long> getDefaultOutputCoder() {
			return BigEndianLongCoder.of();
		}

		static class Reader extends UnboundedReader<Long> {

			private final UnboundedSource<Long, LongCheckpointMark> origin;
			private long currentOffset = 0;

			public Reader(UnboundedSource<Long, LongCheckpointMark> origin) {
				this(origin, null);
			}

			public Reader(UnboundedSource<Long, LongCheckpointMark> origin, LongCheckpointMark checkpointMark) {
				this.origin = origin;

				if (checkpointMark != null) {
					//System.out.println("Restoring from " + checkpointMark);
					this.currentOffset = checkpointMark.offset;
				}
			}

			@Override
			public boolean start() throws IOException {
				return true;
			}

			@Override
			public boolean advance() throws IOException {
				currentOffset++;
				try {
					Thread.sleep(500);
				} catch (InterruptedException ignored) {}
				return true;
			}

			@Override
			public Long getCurrent() throws NoSuchElementException {
				return currentOffset;
			}

			@Override
			public Instant getCurrentTimestamp() throws NoSuchElementException {
				return new Instant(currentOffset*1000);
			}

			@Override
			public void close() throws IOException {}

			@Override
			public Instant getWatermark() {
				return new Instant(currentOffset*1000);
			}

			@Override
			public CheckpointMark getCheckpointMark() {
				return new LongCheckpointMark(currentOffset);
			}

			@Override
			public UnboundedSource<Long, ?> getCurrentSource() {
				return origin;
			}
		}
	}

	private static class LongCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

		private final long offset;

		public LongCheckpointMark(long offset) {
			this.offset = offset;
		}

		@Override
		public void finalizeCheckpoint() throws IOException {
		}

		@Override
		public String toString() {
			return "LongCheckpointMark{" +
					"offset=" + offset +
					'}';
		}
	}

}
