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
import java.util.concurrent.ExecutorService;

public final class AsyncSslSocket implements AsyncTcpSocket {
	private static final Logger logger = LoggerFactory.getLogger(AsyncSslSocket.class);

	private final Eventloop eventloop;
	private final SSLEngine engine;
	private final ExecutorService executor;
	private final AsyncTcpSocket upstream;
	private final AsyncTcpSocket.EventHandler upstreamEventHandler = new AsyncTcpSocket.EventHandler() {
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
			sync();
		}

		@Override
		public void onReadEndOfStream() {

		}

		@Override
		public void onWrite() {
			if (!isOpen()) return;
			if (engine2netQueue.isEmpty() && app2engineQueue.isEmpty() && writeInterest) {
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
	private AsyncTcpSocket.EventHandler downstreamEventHandler;

	private final ByteBufQueue net2engineQueue; //  encoded data received from peer that is to be passed to SSLEngine
	private final ByteBufQueue engine2netQueue; //  filled by SSLEngine with encoded data that is to be send to a remote peer
	private final ByteBufQueue engine2appQueue = new ByteBufQueue(); //  decoded data that is to be passed to app
	private final ByteBufQueue app2engineQueue = new ByteBufQueue(); //  used to store raw app data

	private boolean open = true;
	private boolean readInterest = false;
	private boolean writeInterest = false;
	private boolean syncPosted = false;

	public AsyncSslSocket(Eventloop eventloop, AsyncTcpSocket asyncTcpSocket, SSLEngine engine, ExecutorService executor) {
		this.eventloop = eventloop;
		this.engine = engine;
		this.executor = executor;

		this.net2engineQueue = null; //asyncTcpSocket.getReadQueue();
		this.engine2netQueue = null; //asyncTcpSocket.getWriteQueue();

		this.upstream = asyncTcpSocket;
		this.upstream.setEventHandler(upstreamEventHandler);
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
		try {
			writeInterest = true;
			SSLEngineResult.HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
			if (handshakeStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
				postSync();
			} else {
				if (doWrite().bytesProduced() != 0) {
					upstream.write(buf);
				}
			}
		} catch (SSLException e) {
			handleSSLException(e, true);
		}
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

	private interface Converter {
		void endOfStream() throws SSLException;

		SSLEngineResult convert(ByteBuffer sourceBuffer, ByteBuffer targetBuffer) throws SSLException;
	}

	private SSLEngineResult convert(ByteBufQueue sourceQueue, ByteBufQueue targetQueue, Converter converter) throws SSLException {
//		if (logger.isTraceEnabled()) {
//			logger.trace("" +
//					(sourceQueue == app2engineQueue ? "app->net" : "net->app") + ": {" + sourceQueue + "}" + "->{" + targetQueue + "}" +
//					" " + engine.getHandshakeStatus()
//			);
//		}

		ByteBuf sourceBuf;
		if (sourceQueue.remainingBufs() == 1) {
			sourceBuf = sourceQueue.take();
		} else {
			sourceBuf = ByteBufPool.allocate(sourceQueue.remainingBytes());
			sourceQueue.drainTo(sourceBuf);
			sourceBuf.flip();
		}

		ByteBuf targetBuf = ByteBufPool.allocate(Math.max(engine.getSession().getApplicationBufferSize(), engine.getSession().getPacketBufferSize()));
		targetBuf.limit(targetBuf.array().length);
		ByteBuffer sourceBuffer = sourceBuf.toByteBuffer();
		ByteBuffer targetBuffer = targetBuf.toByteBuffer();
//		if (sourceQueue.isEndOfStream()) {
//			converter.endOfStream();
//		}

		SSLEngineResult result = converter.convert(sourceBuffer, targetBuffer);

		sourceBuf.setByteBuffer(sourceBuffer);
		if (sourceBuf.hasRemaining()) {
			sourceQueue.add(sourceBuf);
		} else {
			sourceBuf.recycle();
		}

		targetBuffer.flip();
		targetBuf.setByteBuffer(targetBuffer);
		if (targetBuf.hasRemaining()) {
			targetQueue.add(targetBuf);
		} else {
			targetBuf.recycle();
		}

//		if (logger.isTraceEnabled()) {
//			logger.trace("" +
//					(sourceQueue == app2engineQueue ? "app->net" : "net->app") + ": {" + sourceQueue + "}" + "->{" + targetQueue + "}" +
//					" " + result
//			);
//		}
		return result;
	}

	private final Converter app2netConverter = new Converter() {
		@Override
		public void endOfStream() throws SSLException {
			engine.closeOutbound();
		}

		@Override
		public SSLEngineResult convert(ByteBuffer sourceBuffer, ByteBuffer targetBuffer) throws SSLException {
			return engine.wrap(sourceBuffer, targetBuffer);
		}
	};

	private SSLEngineResult doWrite() throws SSLException {
		return convert(app2engineQueue, engine2netQueue, app2netConverter);
	}

	private final Converter net2appConverter = new Converter() {
		@Override
		public void endOfStream() throws SSLException {
			engine.closeInbound();
		}

		@Override
		public SSLEngineResult convert(ByteBuffer sourceBuffer, ByteBuffer targetBuffer) throws SSLException {
			return engine.unwrap(sourceBuffer, targetBuffer);
		}
	};

	private SSLEngineResult doRead() throws SSLException {
		return convert(net2engineQueue, engine2appQueue, net2appConverter);
	}

	private void executeTasks() {
		while (true) {
			final Runnable task = engine.getDelegatedTask();
			if (task == null)
				break;
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

	private void doSync() throws SSLException {
		int engine2appRemaining = engine2appQueue.remainingBytes();
		int engine2netRemaining = engine2netQueue.remainingBytes();

		SSLEngineResult result;
		while (true) {
			SSLEngineResult.HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
			if (handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
				result = doWrite();
			} else if (handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP && (net2engineQueue.hasRemaining())){// || net2engineQueue.isEndOfStream())) {
				result = doRead();
				if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW)
					break;
			} else if (handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_TASK) {
				executeTasks();
			} else if (handshakeStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
				if (readInterest && (net2engineQueue.hasRemaining())){// || net2engineQueue.isEndOfStream())) {
					do {
						result = doRead();
					} while (net2engineQueue.hasRemaining());
				}
				if (writeInterest && (app2engineQueue.hasRemaining())){// || app2engineQueue.isEndOfStream())) {
					do {
						result = doWrite();
					} while (app2engineQueue.hasRemaining());
				}
				break;
			} else
				break;
		}

		if (engine2netRemaining != engine2netQueue.remainingBytes()) {
//			upstream.write();
		}
		if (engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP || readInterest) {
			upstream.read();
		}
		if (engine2appRemaining != engine2appQueue.remainingBytes() && readInterest) {
			readInterest = false;
//			downstreamEventHandler.onRead();
		}

	}

}
