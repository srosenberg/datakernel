/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.logfs;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.SimpleCompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import io.datakernel.stream.processor.StreamTransformer;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogStreamConsumer_ByteBuffer extends StreamConsumerDecorator<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(LogStreamConsumer_ByteBuffer.class);
	private final StreamWriteLog streamWriteLog;

	public LogStreamConsumer_ByteBuffer(Eventloop eventloop, DateTimeFormatter datetimeFormat, long fileSwitchPeriod,
	                                    LogFileSystem fileSystem, String logPartition) {
		this.streamWriteLog = new StreamWriteLog(eventloop, datetimeFormat, fileSwitchPeriod, fileSystem, logPartition);
		setActualConsumer(streamWriteLog.getInput());
	}

	public void setCompletionCallback(CompletionCallback completionCallback) {
		streamWriteLog.inputConsumer.callback = completionCallback;
	}

	public void setTag(Object tag) {
		streamWriteLog.setTag(tag);
	}

	private class StreamWriteLog extends AbstractStreamTransformer_1_1<ByteBuf, ByteBuf> {
		private InputConsumer inputConsumer;
		private OutputProducer outputProducer;

		protected StreamWriteLog(Eventloop eventloop, DateTimeFormatter datetimeFormat, long fileSwitchPeriod,
		                         LogFileSystem fileSystem, String logPartition) {
			super(eventloop);
			outputProducer = new OutputProducer();
			inputConsumer = new InputConsumer(datetimeFormat, fileSwitchPeriod, fileSystem, logPartition);
		}

		private class InputConsumer extends AbstractInputConsumer implements StreamDataReceiver<ByteBuf> {
			private final String logPartition;

			private long currentPeriod = -1;
			private LogFile currentLogFile;

			private final DateTimeFormatter datetimeFormat;
			private final long fileSwitchPeriod;
			private final LogFileSystem fileSystem;
			private boolean creatingFile;
			private boolean switchedPeriodWhileCreating;

			private int activeWriters = 0;

			private CompletionCallback callback;

			public InputConsumer(DateTimeFormatter datetimeFormat, long fileSwitchPeriod,
			                     LogFileSystem fileSystem, String logPartition) {
				this.logPartition = logPartition;
				this.datetimeFormat = datetimeFormat;
				this.fileSwitchPeriod = fileSwitchPeriod;
				this.fileSystem = fileSystem;
			}

			@Override
			protected void onUpstreamEndOfStream() {
				logger.trace("{}: upstream producer {} endOfStream.", this, upstreamProducer);

				if (outputProducer.getDownstream() != null) {
					outputProducer.sendEndOfStream();
				}

				if (activeWriters == 0 && !creatingFile) {
					zeroActiveWriters();
				}
			}

			@Override
			public StreamDataReceiver<ByteBuf> getDataReceiver() {
				return this;
			}

			@Override
			public void onData(ByteBuf buf) {
				long timestamp = eventloop.currentTimeMillis();
				long newPeriod = timestamp / fileSwitchPeriod;
				outputProducer.send(buf);

				if (newPeriod != currentPeriod && creatingFile) switchedPeriodWhileCreating = true;
				if (newPeriod != currentPeriod && !creatingFile) {
					currentPeriod = newPeriod;
					String chunkName = datetimeFormat.print(timestamp);
					if (currentLogFile == null || !chunkName.equals(currentLogFile.getName())) {
						createWriteStream(chunkName);
					}
				}
			}

			private void createWriteStream(final String newChunkName) {
				creatingFile = true;
				fileSystem.makeUniqueLogFile(logPartition, newChunkName, new ResultCallback<LogFile>() {
					@Override
					public void onResult(LogFile result) {
						creatingFile = false;
						++activeWriters;

						if (outputProducer.getDownstream() != null) {
							outputProducer.getDownstream().onProducerEndOfStream();
						}

						currentLogFile = result;
						StreamTransformer<ByteBuf, ByteBuf> forwarder = new StreamForwarder<>(eventloop);
						fileSystem.write(logPartition, currentLogFile, forwarder.getOutput(), createCloseCompletionCallback());
						outputProducer.streamTo(forwarder.getInput());

						if (getConsumerStatus() == StreamStatus.END_OF_STREAM) {
							forwarder.getInput().onProducerEndOfStream();
						}
						if (getConsumerStatus() == StreamStatus.CLOSED_WITH_ERROR) {
							forwarder.getInput().onProducerError(getConsumerException());
						}
						if (switchedPeriodWhileCreating) {
							switchedPeriodWhileCreating = false;
							checkPeriod();
						}
					}

					@Override
					public void onException(Exception exception) {
						creatingFile = false;
						logger.error("{}: creating new unique log file with name '{}' and log partition '{}' failed.",
								LogStreamConsumer_ByteBuffer.this, newChunkName, logPartition, exception);

						closeWithError(exception);
						if (activeWriters == 0) {
							zeroActiveWriters();
						}
					}
				});
			}

			private void checkPeriod() {
				long timestamp = eventloop.currentTimeMillis();

				currentPeriod = timestamp / fileSwitchPeriod;
				String chunkName = datetimeFormat.print(timestamp);
				if (currentLogFile == null || !chunkName.equals(currentLogFile.getName())) {
					createWriteStream(chunkName);
				}
			}

			private void zeroActiveWriters() {
				if (getConsumerStatus() == StreamStatus.END_OF_STREAM) {
					if (callback != null) {
						callback.onComplete();
						callback = null;
					}
				} else if (error != null) {
					if (callback != null) {
						callback.onException(getConsumerException());
						callback = null;
					}
				} else {
					resume();
				}
			}

			private CompletionCallback createCloseCompletionCallback() {
				return new SimpleCompletionCallback() {
					@Override
					protected void onCompleteOrException() {
						--activeWriters;
						if (activeWriters == 0) {
							zeroActiveWriters();
						} else {
							resume();
						}
					}
				};
			}
		}

		private class OutputProducer extends AbstractOutputProducer {
			@Override
			protected void doCleanup() {
				for (ByteBuf byteBuf : outputProducer.bufferedList) {
					byteBuf.recycle();
				}
				outputProducer.bufferedList.clear();
			}

			@Override
			protected void onDownstreamSuspended() {
				inputConsumer.suspend();
			}

			@Override
			protected void onDownstreamResumed() {
				inputConsumer.resume();
			}
		}
	}
}