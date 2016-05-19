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

package io.datakernel.eventloop;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * Represent the TCP connection, which is {@link SocketConnection}. It is created with socketChannel
 * and in which sides can exchange {@link ByteBuf}.
 */
public final class AsyncTcpSocketImpl implements AsyncTcpSocket, NioChannelEventHandler {
	public static final int DEFAULT_RECEIVE_BUFFER_SIZE = 8 * 1024;

	private final Eventloop eventloop;
	private SelectionKey key;

	public static final int OP_RECORDING = 1 << 7;

	private int ops = 0;

	protected int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;

	private final SocketChannel channel;
	private final ByteBufQueue writeQueue;

	private AsyncTcpSocket.EventHandler socketEventHandler;

	/**
	 * Creates a new instance of TcpSocketConnection
	 *
	 * @param socketChannel socketChannel for creating this connection
	 */
	public AsyncTcpSocketImpl(Eventloop eventloop, SocketChannel socketChannel) {
		this.eventloop = eventloop;
		this.channel = socketChannel;
		this.writeQueue = new ByteBufQueue();
	}

	/**
	 * Registers channel of this connection in eventloop with which it was related.
	 */
	public final void register() {
		try {
			key = channel.register(eventloop.ensureSelector(), ops, this);
		} catch (final IOException e) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					closeChannel();
					socketEventHandler.onClosedWithError(e);
				}
			});
		}
		socketEventHandler.onRegistered();
	}

	/**
	 * Gets interested operations of channel of this connection.
	 *
	 * @param newOps new interest
	 */
	@SuppressWarnings("MagicConstant")
	private void interests(int newOps) {
		ops = newOps;
		if ((ops & OP_RECORDING) == 0 && key != null) {
			key.interestOps(ops);
		}
	}

	private void readInterest(boolean readInterest) {
		interests(readInterest ? (ops | SelectionKey.OP_READ) : (ops & ~SelectionKey.OP_READ));
	}

	private void writeInterest(boolean writeInterest) {
		interests(writeInterest ? (ops | SelectionKey.OP_WRITE) : (ops & ~SelectionKey.OP_WRITE));
	}

	private void recordInterests() {
		ops = ops | OP_RECORDING;
	}

	private void applyInterests() {
		interests(ops & ~OP_RECORDING);
	}

	@Override
	public void setEventHandler(AsyncTcpSocket.EventHandler eventHandler) {
		this.socketEventHandler = eventHandler;
	}

	@Override
	public void read() {
		readInterest(true);
	}

	/**
	 * Reads received bytes, creates ByteBufs with it and call its method onRead() with
	 * this buffer.
	 */
	@Override
	public void onReadReady() {
		recordInterests();
		readInterest(false);
		doRead();
		applyInterests();
	}

	private void doRead() {
		ByteBuf buf = ByteBufPool.allocate(receiveBufferSize);
		ByteBuffer byteBuffer = buf.toByteBuffer();

		int numRead;
		try {
			numRead = channel.read(byteBuffer);
			buf.setByteBuffer(byteBuffer);
		} catch (IOException e) {
			buf.recycle();
			closeWithError(e, false);
			return;
		}

		if (numRead == 0) {
			buf.recycle();
			return;
		}

		if (numRead == -1) {
			buf.recycle();
			socketEventHandler.onReadEndOfStream();
			return;
		}

		buf.flip();

		socketEventHandler.onRead(buf);
	}

	private boolean writing = false;

	private final Runnable writeRunnable = new Runnable() {
		@Override
		public void run() {
			if (!writing || !isOpen())
				return;
			writing = false;
			try {
				doWrite();
				if (writeQueue.isEmpty()) {
					socketEventHandler.onWrite();
				}
			} catch (IOException e) {
				closeWithError(e, true);
			}
		}
	};

	@Override
	public void write(ByteBuf buf) {
		writeQueue.add(buf);
		if (!writing) {
			writing = true;
			eventloop.post(writeRunnable);
		}
	}

	/**
	 * This method is called if writeInterest is on and it is possible to write to the channel.
	 */
	@Override
	public void onWriteReady() {
		writing = false;
		try {
			int writtenBytes = doWrite();
			if (writtenBytes != 0) {
				socketEventHandler.onWrite();
			}
		} catch (IOException e) {
			closeWithError(e, false);
		}
	}

	/**
	 * Peeks ByteBuf from writeQueue, and sends its bytes to address.
	 */
	private int doWrite() throws IOException {
		int writtenBytes = 0;
		while (!writeQueue.isEmpty()) {
			ByteBuf buf = writeQueue.peekBuf();
			ByteBuffer byteBuffer = buf.toByteBuffer();
			writtenBytes += channel.write(byteBuffer);
			buf.setByteBuffer(byteBuffer);

			int remainingNew = buf.remaining();

			if (remainingNew > 0) {
				break;
			}
			writeQueue.take();
			buf.recycle();
		}
		if (writeQueue.isEmpty()) {
			writeInterest(false);
		} else {
			writeInterest(true);
		}
		return writtenBytes;
	}

	@Override
	public void close() {
		assert eventloop.inEventloopThread();
		if (key == null) return;
		closeChannel();
		key = null;
		writeQueue.clear();
	}

	private void closeChannel() {
		if (channel == null)
			return;
		try {
			channel.close();
		} catch (IOException e) {
			eventloop.recordIoError(e, toString());
		}
	}

	public boolean isOpen() {
		return key != null;
	}

	private void closeWithError(final Exception e, boolean fireAsync) {
		if (isOpen()) {
			eventloop.recordIoError(e, this);
			close();
			if (fireAsync)
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						socketEventHandler.onClosedWithError(e);
					}
				});
			else
				socketEventHandler.onClosedWithError(e);
		}
	}

	public InetSocketAddress getRemoteSocketAddress() {
		try {
			return (InetSocketAddress) channel.getRemoteAddress();
		} catch (IOException ignored) {
			throw new AssertionError("I/O error occurs or channel closed");
		}
	}

	@Override
	public String toString() {
		return getRemoteSocketAddress() + " " + socketEventHandler.toString();
	}
}
