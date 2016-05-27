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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;

import static io.datakernel.util.Preconditions.checkNotNull;

@SuppressWarnings("WeakerAccess")
public final class AsyncTcpSocketImpl implements AsyncTcpSocket, NioChannelEventHandler {
	public static final int DEFAULT_RECEIVE_BUFFER_SIZE = 16 * 1024;
	public static final int OP_POSTPONED = 1 << 7;  // SelectionKey constant
	private static final int MAX_MERGE_SIZE = 16 * 1024;

	private final Eventloop eventloop;
	private final SocketChannel channel;
	private final ArrayDeque<ByteBuf> writeQueue = new ArrayDeque<>();
	private boolean writeEndOfStream;
	private AsyncTcpSocket.EventHandler socketEventHandler;
	private SelectionKey key;

	private int ops = 0;
	private boolean writing = false;

	protected int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;

	private final Runnable writeRunnable = new Runnable() {
		@Override
		public void run() {
			if (!writing || !isOpen())
				return;
			writing = false;
			try {
				doWrite();
			} catch (IOException e) {
				closeWithError(e, true);
			}
		}
	};

	// creators and builder methods
	public AsyncTcpSocketImpl(Eventloop eventloop, SocketChannel socketChannel) {
		this.eventloop = checkNotNull(eventloop);
		this.channel = checkNotNull(socketChannel);
	}

	@Override
	public void setEventHandler(AsyncTcpSocket.EventHandler eventHandler) {
		this.socketEventHandler = eventHandler;
	}

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

	// interests management
	@SuppressWarnings("MagicConstant")
	private void interests(int newOps) {
		if (ops != newOps) {
			ops = newOps;
			if ((ops & OP_POSTPONED) == 0 && key != null) {
				key.interestOps(ops);
			}
		}

	}

	private void readInterest(boolean readInterest) {
		interests(readInterest ? (ops | SelectionKey.OP_READ) : (ops & ~SelectionKey.OP_READ));
	}

	private void writeInterest(boolean writeInterest) {
		interests(writeInterest ? (ops | SelectionKey.OP_WRITE) : (ops & ~SelectionKey.OP_WRITE));
	}

	// read cycle
	@Override
	public void read() {
		readInterest(true);
	}

	@Override
	public void onReadReady() {
		int oldOps = ops;
		ops = ops | OP_POSTPONED;
		readInterest(false);
		doRead();
		int newOps = ops & ~OP_POSTPONED;
		ops = oldOps;
		interests(newOps);
	}

	private void doRead() {
		ByteBuf buf = ByteBufPool.allocate(receiveBufferSize);
		ByteBuffer buffer = buf.toByteBuffer();

		int numRead;
		try {
			numRead = channel.read(buffer);
			buf.setByteBuffer(buffer);
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

	// write cycle
	@Override
	public void write(ByteBuf buf) {
		assert !writeEndOfStream;
		writeQueue.add(buf);
		if (!writing) {
			writing = true;
			eventloop.post(writeRunnable);
		}
	}

	@Override
	public void writeEndOfStream() {
		assert !writeEndOfStream;
		writeEndOfStream = true;
		if (!writing) {
			writing = true;
			eventloop.post(writeRunnable);
		}
	}

	@Override
	public void onWriteReady() {
		writing = false;
		try {
			doWrite();
		} catch (IOException e) {
			closeWithError(e, false);
		}
	}

	private void doWrite() throws IOException {
		while (true) {
			ByteBuf bufToSend = writeQueue.poll();
			if (bufToSend == null)
				break;

			while (true) {
				ByteBuf nextBuf = writeQueue.peek();
				if (nextBuf == null)
					break;

				int bytesToCopy = nextBuf.remaining(); // bytes to append to bufToSend
				if (bufToSend.position() + bufToSend.remaining() + bytesToCopy > bufToSend.array().length)
					bytesToCopy += bufToSend.remaining(); // append will resize bufToSend
				if (bytesToCopy < MAX_MERGE_SIZE) {
					int oldPos = bufToSend.position();
					bufToSend.position(bufToSend.limit());
					bufToSend = ByteBufPool.append(bufToSend, nextBuf);
					bufToSend.position(oldPos);
					nextBuf.recycle();
					writeQueue.poll();
				} else {
					break;
				}
			}

			@SuppressWarnings("ConstantConditions")
			ByteBuffer buffer = bufToSend.toByteBuffer();
			channel.write(buffer);
			bufToSend.setByteBuffer(buffer);

			int remaining = bufToSend.remaining();

			if (remaining > 0) {
				writeQueue.addFirst(bufToSend); // put the buf back to the queue, to send it the next time
				break;
			}
			bufToSend.recycle();
		}

		if (writeQueue.isEmpty()) {
			if (writeEndOfStream) {
				channel.shutdownOutput();
			}
			writeInterest(false);
			socketEventHandler.onWrite();
		} else {
			writeInterest(true);
		}
	}

	// close methods
	@Override
	public void close() {
		assert eventloop.inEventloopThread();
		if (key == null) return;
		closeChannel();
		key = null;
		for (ByteBuf buf : writeQueue) {
			buf.recycle();
		}
		writeQueue.clear();
	}

	private void closeChannel() {
		if (channel == null) return;
		try {
			channel.close();
		} catch (IOException e) {
			eventloop.recordIoError(e, toString());
		}
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

	// miscellaneous
	public boolean isOpen() {
		return key != null;
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
