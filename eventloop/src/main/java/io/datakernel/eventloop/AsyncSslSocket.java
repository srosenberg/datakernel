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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;

import static io.datakernel.bytebuf.ByteBufPool.recycleIfEmpty;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;

@SuppressWarnings("AssertWithSideEffects")
public final class AsyncSslSocket implements AsyncTcpSocket, AsyncTcpSocket.EventHandler, AsyncTcpSocketDebug {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final SSLEngine engine;
	private final Executor executor;
	private final AsyncTcpSocket upstream;

	private AsyncTcpSocket.EventHandler downstreamEventHandler;

	private ByteBuf net2engine = ByteBuf.empty();
	private ByteBuf engine2app = ByteBuf.empty();
	private ByteBuf app2engine = ByteBuf.empty();

	private boolean syncPosted = false;
	private boolean read = false;
	private boolean write = false;

	private AsyncTcpSocketContract contractChecker;
	private boolean closed = false;

	private static PacketDebugger packetDebugger;

	// region builders
	private static AsyncSslSocket wrapSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLContext sslContext, Executor executor, boolean clientMode) {
		SSLEngine sslEngine = sslContext.createSSLEngine();
		sslEngine.setUseClientMode(clientMode);
		AsyncSslSocket asyncSslSocket = AsyncSslSocket.create(eventloop, asyncTcpSocket, sslEngine, executor);
		asyncTcpSocket.setEventHandler(asyncSslSocket);
		return asyncSslSocket;
	}

	public static AsyncSslSocket wrapClientSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLContext sslContext, Executor sslExecutor) {
		return wrapSocket(eventloop, asyncTcpSocket, sslContext, sslExecutor, true);
	}

	public static AsyncSslSocket wrapServerSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLContext sslContext, Executor executor) {
		return wrapSocket(eventloop, asyncTcpSocket, sslContext, executor, false);
	}

	private AsyncSslSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLEngine engine, Executor executor) {
		this.eventloop = eventloop;
		this.engine = engine;
		this.executor = executor;
		this.upstream = asyncTcpSocket;

		//noinspection ConstantConditions
		assert (this.contractChecker = AsyncTcpSocketContract.create()) != null;
	}

	public static AsyncSslSocket create(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket,
	                                    SSLEngine engine, Executor executor) {
		return new AsyncSslSocket(eventloop, asyncTcpSocket, engine, executor);
	}
	// endregion

	public static void setPacketDebugger(PacketDebugger packetDebugger) {
		AsyncSslSocket.packetDebugger = packetDebugger;
	}

	@Override
	public void onRegistered() {
		downstreamEventHandler.onRegistered();
		try {
			engine.beginHandshake();
			doSync();
		} catch (SSLException e) {
			handleSSLException(e, true);
		}
	}

	@Override
	public void onRead(ByteBuf buf) {
		net2engine = ByteBufPool.append(net2engine, buf);
		sync();
	}

	@Override
	public void onReadEndOfStream() {
		if (engine.isInboundDone()) return;
		try {
			engine.closeInbound();
		} catch (SSLException e) {
			assert contractChecker.onClosedWithError();
			downstreamEventHandler.onClosedWithError(e);
			recycleByteBufs();
			upstream.close();
		}
	}

	private void recycleByteBufs() {
		net2engine.recycle();
		engine2app.recycle();
		app2engine.recycle();
	}

	@Override
	public void onWrite() {
		if (engine.isOutboundDone()) {
			upstream.close();
			recycleByteBufs();
			return;
		}
		if (!app2engine.canRead() && engine.getHandshakeStatus() == NOT_HANDSHAKING && write) {
			write = false;
			assert contractChecker.onWrite();
			downstreamEventHandler.onWrite();
		}
	}

	@Override
	public void onClosedWithError(Exception e) {
		assert contractChecker.onClosedWithError();
		recycleByteBufs();
		downstreamEventHandler.onClosedWithError(e);
	}

	@Override
	public void setEventHandler(EventHandler eventHandler) {
		this.downstreamEventHandler = eventHandler;
	}

	@Override
	public void read() {
		assert contractChecker.read();

		read = true;
		if (!net2engine.canRead() && !engine2app.canRead()) {
			upstream.read();
		}
		postSync();
	}

	private void postSync() {
		if (!syncPosted) {
			syncPosted = true;
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					sync();
				}
			});
		}
	}

	@Override
	public void write(ByteBuf buf) {
		assert contractChecker.write();
		write = true;
		app2engine = ByteBufPool.append(app2engine, buf);
		postSync();
	}

	@Override
	public void writeEndOfStream() {
		throw new UnsupportedOperationException("SSL cannot work in half-duplex mode");
	}

	@Override
	public void close() {
		assert contractChecker.close();

		closed = true;
		engine.closeOutbound();
		postSync();
	}

	@Override
	public InetSocketAddress getRemoteSocketAddress() {
		return upstream.getRemoteSocketAddress();
	}

	private void handleSSLException(final SSLException e, boolean post) {
		upstream.close();
		recycleByteBufs();
		if (post) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					assert contractChecker.onClosedWithError();
					downstreamEventHandler.onClosedWithError(e);
				}
			});
		} else {
			assert contractChecker.onClosedWithError();
			downstreamEventHandler.onClosedWithError(e);
		}
	}

	private SSLEngineResult tryToUnwrap() throws SSLException {
		ByteBuf dstBuf = ByteBufPool.allocate(engine.getSession().getPacketBufferSize());
		ByteBuffer srcBuffer = net2engine.toReadByteBuffer();
		ByteBuffer dstBuffer = dstBuf.toWriteByteBuffer();

		SSLEngineResult result;
		try {
			result = engine.unwrap(srcBuffer, dstBuffer);
		} catch (SSLException e) {
			dstBuf.recycle();
			throw e;
		} catch (RuntimeException e) {
			// https://bugs.openjdk.java.net/browse/JDK-8072452
			dstBuf.recycle();
			throw new SSLException(e);
		}

		net2engine.ofReadByteBuffer(srcBuffer);
		net2engine = recycleIfEmpty(net2engine);

		dstBuf.ofWriteByteBuffer(dstBuffer);
		if (dstBuf.canRead()) {
			engine2app = ByteBufPool.append(engine2app, dstBuf);
		} else {
			dstBuf.recycle();
		}

		return result;
	}

	private SSLEngineResult tryToWrap() throws SSLException {
		ByteBuf dstBuf = ByteBufPool.allocate(engine.getSession().getPacketBufferSize());
		ByteBuffer srcBuffer = app2engine.toReadByteBuffer();
		ByteBuffer dstBuffer = dstBuf.toWriteByteBuffer();

		SSLEngineResult result;
		try {
			result = engine.wrap(srcBuffer, dstBuffer);
		} catch (SSLException e) {
			dstBuf.recycle();
			throw e;
		}

		if (packetDebugger != null) {
			recordWriteEvent(srcBuffer, app2engine);
		}

		app2engine.ofReadByteBuffer(srcBuffer);
		app2engine = recycleIfEmpty(app2engine);

		dstBuf.ofWriteByteBuffer(dstBuffer);
		if (dstBuf.canRead()) {
			upstream.write(dstBuf);
		} else {
			dstBuf.recycle();
		}
		return result;
	}

	private void recordWriteEvent(ByteBuffer buffer, ByteBuf buf) {
		boolean partialFlush = buffer.remaining() > 0;
		if (partialFlush) {
			int bytesFlushed = buf.readRemaining() - buffer.remaining();
			int readPos = buf.readPosition();
			ByteBuf flushedBuf = ByteBuf.wrap(buf.array(), readPos, readPos + bytesFlushed);
			packetDebugger.onWrite(this, flushedBuf, true);
		} else {
			packetDebugger.onWrite(this, buf, false);
		}
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
							sync();
						}
					});
				}
			});
		}
	}

	private void sync() {
		syncPosted = false;
		try {
			doSync();
		} catch (SSLException e) {
			handleSSLException(e, false);
		}
	}

	@SuppressWarnings("UnusedAssignment")
	private void doSync() throws SSLException {
		HandshakeStatus handshakeStatus;
		SSLEngineResult result = null;
		while (true) {
			handshakeStatus = engine.getHandshakeStatus();
			if (handshakeStatus == NEED_WRAP) {
				result = tryToWrap();
			} else if (handshakeStatus == NEED_UNWRAP) {
				result = tryToUnwrap();
				if (result.getStatus() == BUFFER_UNDERFLOW) {
					upstream.read();
					break;
				}
			} else if (handshakeStatus == NEED_TASK) {
				executeTasks();
				return;
			} else if (handshakeStatus == NOT_HANDSHAKING) {
				if (result != null && result.getHandshakeStatus() == FINISHED) {
					upstream.read();
				}

				// read data from net
				if (net2engine.canRead()) {
					do {
						result = tryToUnwrap();
					} while (net2engine.canRead() && (result.bytesConsumed() != 0 || result.bytesProduced() != 0));
					if (result.getStatus() == BUFFER_UNDERFLOW) {
						upstream.read();
					}
				}

				// write data to net
				if (app2engine.canRead()) {
					do {
						result = tryToWrap();
					} while (app2engine.canRead() && (result.bytesConsumed() != 0 || result.bytesProduced() != 0));
				}
				break;
			} else
				break;
		}

		if (read && engine2app.canRead()) {
			read = false;
			ByteBuf readBuf = engine2app;
			engine2app = ByteBuf.empty();

			if (packetDebugger != null) {
				packetDebugger.onRead(this, readBuf);
			}
			assert contractChecker.onRead();
			downstreamEventHandler.onRead(readBuf);
		}

		if (closed) { // socket was closed by its handler
			return;
		}

		if (result != null && result.getStatus() == CLOSED) {
			assert contractChecker.onReadEndOfStream();
			downstreamEventHandler.onReadEndOfStream();
		}
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public EventHandler getEventHandler() {
		return downstreamEventHandler;
	}

	@Override
	public SocketChannel getSocketChannel() {
		return upstream instanceof AsyncTcpSocketImpl ? ((AsyncTcpSocketImpl) upstream).getSocketChannel() : null;
	}
}