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

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;

public final class AsyncSslSocket implements AsyncTcpSocket, AsyncTcpSocket.EventHandler, SecuredAsyncSocket {
	private final Eventloop eventloop;
	private final SSLEngine engine;
	private final ExecutorService executor;
	private final AsyncTcpSocket upstream;

	private AsyncTcpSocket.EventHandler downstreamEventHandler;

	private ByteBuf net2engine;
	private final ByteBufQueue app2engineQueue = new ByteBufQueue();

	private boolean open = true;
	private boolean readInterest = false;
	private boolean writeInterest = false;
	private boolean syncPosted = false;

	public AsyncSslSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLEngine engine, ExecutorService executor) {
		this.eventloop = eventloop;
		this.engine = engine;
		this.executor = executor;
		this.upstream = asyncTcpSocket;
	}

	@Override
	public void onRegistered() {
		open = true;
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
		if (!isOpen()) return;
		if (net2engine == null) {
			net2engine = buf;
		} else {
			if (net2engine.position() + buf.remaining() > net2engine.capacity()) {
				net2engine = ByteBufPool.resize(net2engine, net2engine.remaining() + buf.remaining());
			}
			int oldPos = net2engine.position();
			net2engine.position(net2engine.limit());
			net2engine = ByteBufPool.append(net2engine, buf);
			net2engine.position(oldPos);
			buf.recycle();
		}
		sync();
	}

	@Override
	public void onReadEndOfStream() {
		try {
			engine.closeInbound();
			downstreamEventHandler.onReadEndOfStream();
		} catch (SSLException e) {
			handleSSLException(e, false);
		}
	}

	@Override
	public void onWrite() {
		if (!isOpen()) return;
		if (app2engineQueue.isEmpty() && writeInterest) {
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

	@Override
	public void setEventHandler(EventHandler eventHandler) {
		this.downstreamEventHandler = eventHandler;
	}

	@Override
	public void read() {
		if (!isOpen()) return;
		upstream.read();
		readInterest = true;
		postSync();
	}

	private void postSync() {
		if (!syncPosted) {
			syncPosted = true;
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					syncPosted = false;
					sync();
				}
			});
		}
	}

	@Override
	public void write(ByteBuf buf) {
		if (!isOpen()) return;
		app2engineQueue.add(buf);
		writeInterest = true;
		postSync();
	}

	@Override
	public void writeEndOfStream() {
		// TODO
	}

	@Override
	public void close() {
		if (!isOpen()) return;
		open = false;
		upstream.close();
	}

	public boolean isOpen() {
		return open;
	}

	private void handleSSLException(final SSLException e, boolean post) {
		if (!isOpen())
			return;
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

	private SSLEngineResult tryToWriteToApp() throws SSLException {
		ByteBuf targetBuf = ByteBufPool.allocate(engine.getSession().getPacketBufferSize());
		targetBuf.limit(targetBuf.array().length);
		ByteBuffer sourceBuffer = net2engine.toByteBuffer();
		ByteBuffer targetBuffer = targetBuf.toByteBuffer();

		SSLEngineResult result = engine.unwrap(sourceBuffer, targetBuffer);

		net2engine.setByteBuffer(sourceBuffer);
		if (!net2engine.hasRemaining()) {
			net2engine.recycle();
			net2engine = null;
		}

		targetBuffer.flip();
		targetBuf.setByteBuffer(targetBuffer);
		if (targetBuf.hasRemaining()) {
			downstreamEventHandler.onRead(targetBuf);
		} else {
			targetBuf.recycle();
		}

		return result;
	}

	private SSLEngineResult tryToWriteToNet() throws SSLException {
		ByteBuf sourceBuf = app2engineQueue.takeRemaining();

		ByteBuf targetBuf = ByteBufPool.allocate(engine.getSession().getPacketBufferSize());
		targetBuf.limit(targetBuf.array().length);
		ByteBuffer sourceBuffer = sourceBuf.toByteBuffer();
		ByteBuffer targetBuffer = targetBuf.toByteBuffer();

		SSLEngineResult result = engine.wrap(sourceBuffer, targetBuffer);

		sourceBuf.setByteBuffer(sourceBuffer);
		if (sourceBuf.hasRemaining()) {
			app2engineQueue.add(sourceBuf);
		} else {
			sourceBuf.recycle();
		}

		targetBuffer.flip();
		targetBuf.setByteBuffer(targetBuffer);
		if (targetBuf.hasRemaining()) {
			upstream.write(targetBuf);
		} else {
			targetBuf.recycle();
		}
		return result;
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
		try {
			doSync();
		} catch (SSLException e) {
			handleSSLException(e, false);
		}
	}

	@SuppressWarnings("UnusedAssignment")
	private void doSync() throws SSLException {
		SSLEngineResult result;
		while (true) {
			HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
			if (handshakeStatus == NEED_WRAP) {
				result = tryToWriteToNet();
			} else if (handshakeStatus == NEED_UNWRAP) {
				if (net2engine != null) {
					result = tryToWriteToApp();
					if (result.getStatus() == BUFFER_UNDERFLOW) {
						readInterest = true;
						break;
					}
				} else {
					readInterest = true;
					break;
				}
			} else if (handshakeStatus == NEED_TASK) {
				executeTasks();
				return;
			} else if (handshakeStatus == NOT_HANDSHAKING) {
				if (readInterest && net2engine != null) {
					do {
						result = tryToWriteToApp();
					} while (net2engine != null && result.getStatus() != BUFFER_UNDERFLOW);
				}
				if (writeInterest && app2engineQueue.hasRemaining()) {
					do {
						result = tryToWriteToNet();
					} while (app2engineQueue.hasRemaining());
				}
				break;
			} else {
				break;
			}
		}

		if (engine.getHandshakeStatus() == NEED_UNWRAP || readInterest) {
			upstream.read();
		}
	}
}