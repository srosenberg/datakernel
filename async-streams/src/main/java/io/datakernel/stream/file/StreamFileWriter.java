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

package io.datakernel.stream.file;

import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.concurrent.ExecutorService;

import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.nio.file.StandardOpenOption.*;

/**
 * This class allows you to write data from file non-blocking. It represents consumer which receives
 * data and writes it to file.
 */
public final class StreamFileWriter extends AbstractStreamConsumer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(StreamFileWriter.class);
	public static final OpenOption[] CREATE_OPTIONS = new OpenOption[]{WRITE, CREATE, TRUNCATE_EXISTING};

	private final ArrayDeque<ByteBuf> queue = new ArrayDeque<>();
	private final AsyncFile asyncFile;

	private long position;

	private boolean pendingAsyncOperation;

	private CompletionCallback flushCallback;

	// creators
	private StreamFileWriter(Eventloop eventloop, AsyncFile asyncFile) {
		super(eventloop);
		this.asyncFile = asyncFile;
	}

	public static StreamFileWriter create(final Eventloop eventloop, ExecutorService executor, Path path) throws IOException {
		AsynchronousFileChannel channel = AsynchronousFileChannel.open(path, CREATE_OPTIONS);
		AsyncFile asyncFile = new AsyncFile(eventloop, executor, channel, path.getFileName().toString());
		return create(eventloop, asyncFile);
	}

	public static StreamFileWriter create(Eventloop eventloop, AsyncFile asyncFile) {
		return new StreamFileWriter(eventloop, asyncFile);
	}

	// api
	public void setFlushCallback(CompletionCallback flushCallback) {
		if (getConsumerStatus().isOpen()) {
			this.flushCallback = flushCallback;
		} else {
			if (getConsumerStatus() == END_OF_STREAM) {
				flushCallback.onComplete();
			} else {
				flushCallback.onException(getConsumerException());
			}
		}
	}

	@Override
	public String toString() {
		return "StreamFileWriter{" +
				"asyncFile=" + asyncFile +
				'}';
	}

	@Override
	public StreamDataReceiver<ByteBuf> getDataReceiver() {
		return this;
	}

	// functional
	private void doFlush() {
		final ByteBuf buf = queue.poll();
		final int length = buf.remaining();
		asyncFile.writeFully(buf, position, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.trace("Completed writing in file {}", asyncFile);
				position += length;
				buf.recycle();
				pendingAsyncOperation = false;
				if (queue.size() <= 1) {
					resume();
				}
				postFlush();
			}

			@Override
			public void onException(final Exception e) {
				logger.error("Failed to write data in file", e);
				buf.recycle();
				doCleanup(new CompletionCallback() {
					@Override
					public void onComplete() {
						pendingAsyncOperation = false;
						closeWithError(e);
					}

					@Override
					public void onException(Exception ignored) {
						pendingAsyncOperation = false;
						closeWithError(e);
					}
				});
			}
		});
	}

	private void postFlush() {
		if (!queue.isEmpty() && !pendingAsyncOperation) {
			logger.trace("Writing in file {}", asyncFile);
			pendingAsyncOperation = true;
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					doFlush();
				}
			});
		}
		if (getConsumerStatus() == END_OF_STREAM && queue.isEmpty() && !pendingAsyncOperation) {
			doCleanup(new CompletionCallback() {
				@Override
				public void onComplete() {
					if (flushCallback != null) {
						flushCallback.onComplete();
					}
				}

				@Override
				public void onException(Exception e) {
					closeWithError(new Exception("Can't do cleanup for file\t" + asyncFile));
				}
			});
		}
	}

	private void doCleanup(CompletionCallback callback) {
		for (ByteBuf buf : queue) {
			buf.recycle();
		}
		queue.clear();
		asyncFile.close(callback);
	}

	@Override
	protected void onStarted() {
		logger.trace("Started writing to file {}", asyncFile);
	}

	@Override
	public void onData(ByteBuf buf) {
		queue.offer(buf);
		if (queue.size() > 1) {
			suspend();
		}
		postFlush();
	}

	@Override
	protected void onEndOfStream() {
		logger.trace("endOfStream for {}, upstream: {}", this, getUpstream());
		postFlush();
	}

	@Override
	protected void onError(final Exception e) {
		logger.error("{}: onError", this, e);
		postFlush();
		if (flushCallback != null) {
			flushCallback.onException(e);
		}
	}

}
