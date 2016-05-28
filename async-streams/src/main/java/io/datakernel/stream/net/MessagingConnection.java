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
import io.datakernel.async.ResultCallback;
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
	private ResultCallback<MessageOrEndOfStream<I>> readCallback;
	private List<CompletionCallback> writeCallbacks = new ArrayList<>();
	private boolean writeEndOfStream;
	private SocketStreamProducer socketReader;
	private SocketStreamConsumer socketWriter;

	public MessagingConnection(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, MessagingSerializer<I, O> serializer) {
		this.eventloop = eventloop;
		this.asyncTcpSocket = asyncTcpSocket;
		this.serializer = serializer;
		this.asyncTcpSocket.setEventHandler(this);
//		this.messagingProtocol = messagingProtocol;
	}

	@Override
	public void read(ResultCallback<MessageOrEndOfStream<I>> callback) {
		checkState(socketReader == null && readCallback == null);
		readCallback = callback;
		if (readBuf.hasRemaining() || readEndOfStream) {
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
		assert readCallback != null;
		if (readBuf.hasRemaining()) {
			try {
				I message = serializer.tryDeserialize(readBuf);
				if (message == null) {
					asyncTcpSocket.read();
				} else {
					readCallback.onResult(new MessageOrEndOfStream<>(message));
					readCallback = null;
				}
			} catch (ParseException e) {
				readCallback.onException(e);
				readCallback = null;
			}
		} else if (readEndOfStream) {
			readCallback.onResult(new MessageOrEndOfStream<I>(null));
			readCallback = null;
		}
	}

	@Override
	public void write(O message, CompletionCallback callback) {
		checkState(socketWriter == null && !writeEndOfStream);
		writeCallbacks.add(callback);
		ByteBuf buf = serializer.serialize(message);
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
		checkState(socketWriter == null && writeCallbacks.isEmpty() && !writeEndOfStream);
		socketWriter = new SocketStreamConsumer(eventloop, asyncTcpSocket, callback);
		producer.streamTo(socketWriter);
	}

	@Override
	public void readStream(StreamConsumer<ByteBuf> consumer, final CompletionCallback callback) {
		checkState(this.socketReader == null && this.readCallback == null);
		socketReader = new SocketStreamProducer(eventloop, asyncTcpSocket, callback);
		socketReader.streamTo(consumer);
		if (readBuf.hasRemaining() || readEndOfStream) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					if (readBuf.hasRemaining()) {
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
	}

	/**
	 * Is called after connection registration. Wires socketReader with StreamConsumer specified by,
	 * and socketWriter with StreamProducer, that are specified by overridden method {@code wire} of subclass.
	 * If StreamConsumer is null, items from socketReader are ignored. If StreamProducer is null, socketWriter
	 * gets EndOfStream signal.
	 */
	@Override
	public void onRegistered() {
	}

	private void readUnconsumedBuf() {
		assert readBuf.hasRemaining();
		socketReader.onRead(readBuf);
		readBuf.recycle();
		readBuf = ByteBuf.empty();
	}

	/**
	 * Sends received bytes to StreamConsumer
	 */
	@Override
	public void onRead(ByteBuf buf) {
		assert eventloop.inEventloopThread();
		if (socketReader == null) {
			readBuf = ByteBufPool.append(readBuf, buf);
			tryReadMessage();
		} else {
			if (readBuf.hasRemaining()) {
				readUnconsumedBuf();
			}
			socketReader.onRead(buf);
		}
	}

	@Override
	public void onReadEndOfStream() {
		readEndOfStream = true;
		if (socketReader == null) {
			tryReadMessage();
		} else {
			if (readBuf.hasRemaining()) {
				readUnconsumedBuf();
			}
			socketReader.onReadEndOfStream();
		}
	}

	@Override
	public void onWrite() {
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
		// TODO
	}

	@Override
	public String toString() {
		return "{asyncTcpSocket=" + asyncTcpSocket + "}";
	}
}
