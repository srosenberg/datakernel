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

package io.datakernel.stream.net;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ParseException;
import io.datakernel.async.SimpleCompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Represent the TCP connection which  processes received items with {@link StreamProducer} and {@link StreamConsumer},
 * which organized by binary protocol. It is created with socketChannel and sides exchange ByteBufs.
 */
public final class MessagingConnection<I, O> implements AsyncTcpSocket.EventHandler, Messaging<I, O> {
	private static final Logger logger = LoggerFactory.getLogger(MessagingConnection.class);

	private final Eventloop eventloop;
	private final AsyncTcpSocket asyncTcpSocket;
	private final MessagingSerializer<I, O> serializer;

	private ByteBuf readBuf;
	private boolean readEndOfStream;
	private ReadCallback<I> readCallback;
	private List<CompletionCallback> writeCallbacks = new ArrayList<>();
	private boolean writeEndOfStream;
	private SocketStreamProducer socketReader;
	private SocketStreamConsumer socketWriter;

	public MessagingConnection(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, MessagingSerializer<I, O> serializer) {
		this.eventloop = eventloop;
		this.asyncTcpSocket = asyncTcpSocket;
		this.serializer = serializer;
	}

	@Override
	public void read(ReadCallback<I> callback) {
		checkState(socketReader == null && readCallback == null);
		readCallback = callback;
		if (readBuf != null || readEndOfStream) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					if (socketReader == null && readCallback != null) {
						tryReadMessage();
					}
				}
			});
		} else {
			asyncTcpSocket.read();
		}
	}

	private void tryReadMessage() {
		if (readBuf != null && readCallback != null) {
			try {
				I message = serializer.tryDeserialize(readBuf);
				if (message == null) {
					asyncTcpSocket.read();
				} else {
					if (!readBuf.hasRemaining()) {
						readBuf.recycle();
						readBuf = null;
					}
					takeReadCallback().onRead(message);
				}
			} catch (ParseException e) {
				takeReadCallback().onException(e);
			}
		}
		if (readBuf == null && readEndOfStream) {
			if (readCallback != null) {
				takeReadCallback().onReadEndOfStream();
			}
		}
	}

	private ReadCallback<I> takeReadCallback() {
		ReadCallback<I> callback = this.readCallback;
		readCallback = null;
		return callback;
	}

	@Override
	public void write(O msg, CompletionCallback callback) {
		checkState(socketWriter == null && !writeEndOfStream);
		writeCallbacks.add(callback);
		ByteBuf buf = serializer.serialize(msg);
		asyncTcpSocket.write(buf);
	}

	@Override
	public void writeEndOfStream(CompletionCallback callback) {
		checkState(socketWriter == null && !writeEndOfStream);
		writeEndOfStream = true;
		writeCallbacks.add(callback);
		asyncTcpSocket.writeEndOfStream();
	}

	@Override
	public void writeStream(StreamProducer<ByteBuf> producer, final CompletionCallback callback) {
		checkState(socketWriter == null && !writeEndOfStream);
		socketWriter = new SocketStreamConsumer(eventloop, asyncTcpSocket, callback);
		producer.streamTo(socketWriter);
	}

	@Override
	public void readStream(StreamConsumer<ByteBuf> consumer, final CompletionCallback callback) {
		checkState(this.socketReader == null && this.readCallback == null);
		socketReader = new SocketStreamProducer(eventloop, asyncTcpSocket, callback);
		socketReader.streamTo(consumer);
		if (readBuf != null || readEndOfStream) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					if (readBuf != null) {
						readUnconsumedBuf();
					}
					if (readEndOfStream) {
						socketReader.onReadEndOfStream();
					}
				}
			});
		}
	}

	@Override
	public void close() {
		asyncTcpSocket.close();
		if (readBuf != null) {
			readBuf.recycle();
			readBuf = null;
		}
	}

	/**
	 * Is called after connection registration. Wires socketReader with StreamConsumer specified by,
	 * and socketWriter with StreamProducer, that are specified by overridden method {@code wire} of subclass.
	 * If StreamConsumer is null, items from socketReader are ignored. If StreamProducer is null, socketWriter
	 * gets EndOfStream signal.
	 */
	@Override
	public void onRegistered() {
		asyncTcpSocket.read();
	}

	private void readUnconsumedBuf() {
		assert readBuf != null;
		socketReader.onRead(readBuf);
		readBuf = null;
	}

	@Override
	public void onRead(ByteBuf buf) {
		logger.trace("onRead", this);
		assert eventloop.inEventloopThread();
		if (socketReader == null) {
			if (readBuf == null) {
				readBuf = ByteBufPool.allocate(Math.max(8192, buf.remaining()));
				readBuf.limit(0);
			}
			int oldPos = readBuf.position();
			readBuf.position(readBuf.limit());
			readBuf = ByteBufPool.append(readBuf, buf);
			buf.recycle();
			readBuf.position(oldPos);
			tryReadMessage();
			if (readBuf == null) {
				asyncTcpSocket.read();
			}
		} else {
			if (readBuf != null) {
				readUnconsumedBuf();
			}
			socketReader.onRead(buf);
		}
	}

	@Override
	public void onReadEndOfStream() {
		logger.trace("onReadEndOfStream", this);
		readEndOfStream = true;
		if (socketReader == null) {
			tryReadMessage();
		} else {
			if (readBuf != null) {
				readUnconsumedBuf();
			}
			socketReader.onReadEndOfStream();
		}
	}

	@Override
	public void onWrite() {
		logger.trace("onWrite", this);
		if (socketWriter == null) {
			List<CompletionCallback> callbacks = this.writeCallbacks;
			writeCallbacks = new ArrayList<>();
			for (CompletionCallback callback : callbacks) {
				callback.onComplete();
			}
		} else {
			socketWriter.onWrite();
		}
	}

	@Override
	public void onClosedWithError(Exception e) {
		logger.trace("onClosedWithError", this);
		// TODO

		if (readBuf != null) {
			readBuf.recycle();
			readBuf = null;
		}
	}

	@Override
	public String toString() {
		return "{asyncTcpSocket=" + asyncTcpSocket + "}";
	}

	public void writeAndClose(O msg) {
		write(msg, new SimpleCompletionCallback() {
			@Override
			protected void onCompleteOrException() {
				close();
			}
		});
	}
}
