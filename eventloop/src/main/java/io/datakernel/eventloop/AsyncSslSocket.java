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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.ExecutorService;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_OVERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;

public final class AsyncSslSocket implements AsyncTcpSocket {
	private static final Logger logger = LoggerFactory.getLogger(AsyncSslSocket.class);

	private final Eventloop eventloop;
	private final SSLEngine engine;
	private final ExecutorService executor;
	private final AsyncTcpSocket upstream;
	private AsyncTcpSocket.EventHandler downstreamEventHandler;

	private final ArrayDeque<ByteBuf> net2engine = new ArrayDeque<>(); // raw net data
	private final ByteBufQueue app2engine = new ByteBufQueue(); //  used to store raw app data before pushing it to handshake

	private boolean open = true;
	private boolean readInterest = false;
	private boolean writeInterest = false;
	private boolean syncPosted = false;
	private final EventHandler upstreamEventHandler = new EventHandler() {
		@Override
		public void onRegistered() {
			open = true;
			downstreamEventHandler.onRegistered();
			try {
				AsyncSslSocket.this.engine.beginHandshake();
				doProcessMsg();
			} catch (SSLException e) {
				handleSSLException(e, true);
			}
		}

		@Override
		public void onRead(ByteBuf buf) {
			if (isOpen()) {
				net2engine.add(buf);
				processMsg();
			}
		}

		@Override
		public void onReadEndOfStream() {
			try {
				AsyncSslSocket.this.engine.closeInbound();
			} catch (SSLException ignored) {
				logger.warn("inbound closed without receiving proper close notification message");
			}
			downstreamEventHandler.onReadEndOfStream();
		}

		@Override
		public void onWrite() {
			if (!isOpen()) return;
			if (AsyncSslSocket.this.engine.getHandshakeStatus() != NOT_HANDSHAKING) {
				writeInterest = false;
				downstreamEventHandler.onWrite();
			}
		}

		@Override
		public void onClosedWithError(Exception e) {
			if (!isOpen()) return;
			open = false;
			downstreamEventHandler.onClosedWithError(e);
		}
	};

	// creators & builder methods
	public AsyncSslSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLEngine engine, ExecutorService executor) {
		this.eventloop = eventloop;
		this.engine = engine;
		this.executor = executor;

		this.upstream = asyncTcpSocket;
		this.upstream.setEventHandler(upstreamEventHandler);
	}

	@Override
	public void setEventHandler(EventHandler eventHandler) {
		this.downstreamEventHandler = eventHandler;
	}

	// read cycle
	@Override
	public void read() {
		if (!isOpen()) return;
		upstream.read();
		readInterest = true;
		postProcessMsg();
	}

	private SSLEngineResult doRead() throws SSLException {
		return writeToNet(net2engine.getFirst());
	}

	// write cycle
	@Override
	public void write(ByteBuf buf) {
		if (isOpen()) {
			app2engine.add(buf);
			writeInterest = true;
			postProcessMsg();
		}
	}

	private SSLEngineResult doWrite() throws SSLException {
		// TODO: (arashev)
		ByteBuf buf = getFromQueue(app2engine);
		return writeToNet(buf);
	}

	// miscellaneous
	public boolean isOpen() {
		return open;
	}

	@Override
	public void close() {
		if (!isOpen()) return;
		open = false;
		upstream.close();
	}

	private void handleSSLException(final SSLException e, boolean post) {
		if (!isOpen()) return;
		upstream.close();
		if (post) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					downstreamEventHandler.onClosedWithError(e);
				}
			});
		} else {
			downstreamEventHandler.onClosedWithError(e);
		}
	}

	private SSLEngineResult writeToNet(ByteBuf buf) throws SSLException {
		ByteBuf targetBuf = ByteBufPool.allocate(getRecommendedSize());
		targetBuf.limit(targetBuf.array().length);
		ByteBuffer sourceBuffer = buf.toByteBuffer();
		ByteBuffer targetBuffer = targetBuf.toByteBuffer();

		SSLEngineResult result = engine.wrap(sourceBuffer, targetBuffer);

		while (result.getStatus() == BUFFER_OVERFLOW) {
			targetBuf = ByteBufPool.resize(targetBuf, targetBuffer.limit() * 2);    // apply some resize strategy
			targetBuffer = targetBuf.toByteBuffer();
			targetBuf.limit(targetBuf.array().length);
			result = engine.wrap(sourceBuffer, targetBuffer);
		}

		buf.setByteBuffer(sourceBuffer);
		if (buf.hasRemaining()) {
			app2engine.add(buf);
		} else {
			buf.recycle();
		}

		targetBuffer.flip();
		targetBuf.setByteBuffer(targetBuffer);

		upstream.write(targetBuf);

		return result;
	}

	private int getRecommendedSize() {
		return Math.max(engine.getSession().getApplicationBufferSize(), engine.getSession().getPacketBufferSize());
	}

	private ByteBuf getFromQueue(ByteBufQueue queue) {
		ByteBuf sourceBuf;
		if (queue.remainingBufs() == 1) {
			sourceBuf = queue.peekBuf();
		} else {
			sourceBuf = ByteBufPool.allocate(queue.remainingBytes());
			queue.drainTo(sourceBuf);
			sourceBuf.flip();
		}
		return sourceBuf;
	}

	private void executeTasks() {
		while (true) {
			final Runnable task = engine.getDelegatedTask();
			if (task == null) break;
			executor.execute(new Runnable() {
				@Override
				public void run() {
					task.run();
					eventloop.execute(new Runnable() {
						@Override
						public void run() {
							processMsg();
						}
					});
				}
			});
		}
	}

	private void processMsg() {
		try {
			doProcessMsg();
		} catch (SSLException e) {
			handleSSLException(e, false);
		}
	}

	private void doProcessMsg() throws SSLException {
		SSLEngineResult result;
		while (true) {
			SSLEngineResult.HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
			if (handshakeStatus == NEED_WRAP) {
				result = doWrite();
			} else if (handshakeStatus == NEED_UNWRAP) {
				result = doRead();
				if (result.getStatus() == BUFFER_UNDERFLOW) {
					break;
				}
			} else if (handshakeStatus == NEED_TASK) {
				executeTasks();
				return;
			} else if (handshakeStatus == NOT_HANDSHAKING) {
				if (readInterest) {
					while (!net2engine.isEmpty()) {
						result = doRead();
						if (result.getStatus() == BUFFER_UNDERFLOW) {
							break;
						}
					}
				}
				if (writeInterest) {
					while (app2engine.hasRemaining()) {
						result = doWrite();
					}
				}
				break;
			} else
				break;
		}

		if (engine.getHandshakeStatus() == NEED_UNWRAP || readInterest) {
			upstream.read();
		}
	}

	private void postProcessMsg() {
		if (!syncPosted) {
			syncPosted = true;
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					syncPosted = false;
					processMsg();
				}
			});
		}
	}
}
