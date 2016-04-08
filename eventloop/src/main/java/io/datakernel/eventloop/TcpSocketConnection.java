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

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;
import static javax.net.ssl.SSLEngineResult.Status.*;

/**
 * Represent the TCP connection, which is {@link SocketConnection}. It is created with socketChannel
 * and in which sides can exchange {@link ByteBuf}.
 */
public abstract class TcpSocketConnection extends SocketConnection {
	private static final Logger logger = LoggerFactory.getLogger(TcpSocketConnection.class);

	protected final SocketChannel channel;
	protected final InetSocketAddress remoteSocketAddress;
	protected final ByteBufQueue writeQueue;
	protected final ByteBufQueue readQueue;

	/**
	 * Creates a new instance of TcpSocketConnection
	 *
	 * @param eventloop     eventloop in which this connection will be handled
	 * @param socketChannel socketChannel for creating this connection
	 */
	public TcpSocketConnection(Eventloop eventloop, SocketChannel socketChannel) {
		this(eventloop, socketChannel, null);
	}

	public TcpSocketConnection(Eventloop eventloop, SocketChannel socketChannel, SSLEngine engine) {
		super(eventloop);
		this.engine = engine;
		this.channel = socketChannel;
		try {
			this.remoteSocketAddress = (InetSocketAddress) channel.getRemoteAddress();
		} catch (IOException ignored) {
			throw new AssertionError("I/O error occurs or channel closed");
		}
		this.writeQueue = new ByteBufQueue();
		this.readQueue = new ByteBufQueue();
	}

	/**
	 * Reads received bytes, creates ByteBufs with it and call its method onRead() with
	 * this buffer.
	 */
	@Override
	public void onReadReady() {
		ByteBuf buf = ByteBufPool.allocate(receiveBufferSize);
		ByteBuffer byteBuffer = buf.toByteBuffer();

		int numRead;
		try {
			numRead = channel.read(byteBuffer);
			buf.setByteBuffer(byteBuffer);
		} catch (IOException e) {
			buf.recycle();
			onReadException(e);
			return;
		}

		if (numRead == 0) {
			buf.recycle();
			return;
		}

		if (numRead == -1) {
			buf.recycle();
			onReadEndOfStream();
			if (isRegistered()) {
				readInterest(false); // prevent spinning if connection is still open
			}
			return;
		}

		if (numRead > 0) {
			readTime = eventloop.currentTimeMillis();
		}

		buf.flip();
		onReadFromChannel(buf);
	}

	private void onReadFromChannel(ByteBuf buf) {
		if (engine != null) {
			try {
				readWithSslEngine(buf);
			} catch (SSLException e) {
				closeSslEngine();
				onReadException(e);
			}
		} else {
			onRead(buf);
		}
	}

	protected void onRead(ByteBuf buf) {
		readQueue.add(buf);
		onRead();
	}

	/**
	 * It processes received ByteBufs
	 * These ByteBufs are in readQueue at the moment of working this function.
	 */
	protected abstract void onRead();

	/*---------------------------------------------------------------------------------------------------------------SSL
	 * One must ensure strict sequential order of handshake messages!
	 * Unexpected sequence order can lead to critical or even fatal results
	 */
	private final SSLEngine engine;
	// 32 * 1024 -- recommended buffer size(it has been stated that any message would not exceed the range)
	private ByteBuffer app2engine = ByteBuffer.allocate(32 * 1024); //  keeps raw app data while handshaking
	private ByteBuffer engine2net = ByteBuffer.allocate(32 * 1024); //  keeps encoded data that is to be send to peer
	private ByteBuffer net2engine = ByteBuffer.allocate(32 * 1024); //  keeps encoded data received from peer
	private ByteBuffer engine2app = ByteBuffer.allocate(32 * 1024); //  keeps decoded data that is to be passed to app

	/*
	 *  Initiates process
	 *
	 *  Is always being initiated by agent(both server responding and client requesting) that is
	 *  willing to pass the data(response/request) to peer.
	 *
	 *  Could only enter this method iff this is client connection and it is sending a request or
	 *  server processed request and responding to client.
	 *
	 *  Naturally initiates handshake process
	 */
	private void writeWithSslEngine(ByteBuf buf) throws SSLException {
		logger.trace("request to write {} bytes of app data", buf.remaining());
		assert engine.getHandshakeStatus() == NOT_HANDSHAKING;

		//  need both to save raw app data and recycle ByteBuf. !!! assert buf size > buffer size
		app2engine.put(buf.array(), buf.position(), buf.limit());
		app2engine.flip();
		buf.recycle();

		SSLEngineResult result;
		boolean written = false;
		while (!written) {
			result = engine.wrap(app2engine, engine2net);
			logger.trace("wrapping app data: {}, new engine status: {}", result.getStatus(), result.getHandshakeStatus());
			switch (result.getStatus()) {
				case BUFFER_OVERFLOW:
					engine2net = enlargePacketBuffer(engine, engine2net);
					break;
				case OK:
					written = true;
					break;
				default:
					throw new SSLException("Invalid operation status: " + result.getStatus());
			}
		}

		// send data
		engine2net.flip();
		writeToChannel(toAllocatedByteBuf(engine2net));
		logger.trace("written {} bytes to channel", engine2net.limit());
		engine2net.clear();
	}

	private ByteBuf toAllocatedByteBuf(ByteBuffer buffer) {
		ByteBuf data = ByteBufPool.allocate(buffer.remaining());
		data.put(buffer.array(), buffer.arrayOffset(), buffer.limit());
		data.flip();
		return data;
	}

	private void readWithSslEngine(ByteBuf buf) throws SSLException {
		logger.trace("reading {} bytes from socket: {}", buf.remaining(), engine.getHandshakeStatus());

		/*
		 *  need to put incoming bytes into transport buffer(ready for writing)
		 */
		if (!net2engine.hasRemaining()) {
			net2engine.clear();
		}

		net2engine.put(buf.array(), buf.position(), buf.limit());
		net2engine.flip();

		// processing with engine
		SSLEngineResult result = engine.unwrap(net2engine, engine2app);
		logger.trace("unwrap {} bytes ({} bytes left) => {}: {}, new engine status: {}",
				net2engine.position(), net2engine.remaining(), engine2app.position(), result.getStatus(), result.getHandshakeStatus());

		// ensuring we managed to read all the data to app buffer successfully
		while (result.getStatus() == BUFFER_OVERFLOW) {
			engine2app = enlargeApplicationBuffer(engine, engine2app);
			result = engine.unwrap(net2engine, engine2app);
			logger.trace("unwrap {} bytes ({} bytes left) => {}: {}, new engine status: {}",
					net2engine.position(), net2engine.remaining(), engine2app.position(), result.getStatus(), result.getHandshakeStatus());
		}

		// need more bytes to proceed --> preparing buffer, exiting
		if (result.getStatus() == BUFFER_UNDERFLOW) {
			net2engine.compact();
			return;
		}

		processSslMessage(result.getHandshakeStatus());
	}

	private void processSslMessage(HandshakeStatus status) throws SSLException {
		boolean processed = false;
		SSLEngineResult result;

		while (status == NEED_WRAP || (status == NEED_UNWRAP && !processed)) {
			if (status == NEED_WRAP) {
				result = engine.wrap(app2engine, engine2net);
				status = result.getHandshakeStatus();
				logger.trace("wrap {} bytes: {}, new engine status: {}", engine2net.position(), result.getStatus(), result.getHandshakeStatus());
				if (result.getStatus() == BUFFER_OVERFLOW) {
					engine2net = enlargePacketBuffer(engine, engine2net);
				} else if (result.getStatus() == OK) {
					engine2net.flip();
					logger.trace("writing to channel: {}", engine2net.limit());
					writeToChannel(toAllocatedByteBuf(engine2net));
					engine2net.clear();
				}
			} else {
				if (net2engine.hasRemaining()) {
					result = engine.unwrap(net2engine, engine2app);
					logger.trace("unwrap {} bytes: {}, new engine status: {}", net2engine.position(), result.getStatus(), result.getHandshakeStatus());
					status = result.getHandshakeStatus();
					if (result.getStatus() == BUFFER_UNDERFLOW) {
						net2engine = handleBufferUnderflow(engine, net2engine);
						net2engine.compact();
						processed = true;
					} else if (result.getStatus() == BUFFER_OVERFLOW) {
						net2engine = enlargePacketBuffer(engine, net2engine);
					}
				} else {
					processed = true;
				}
			}
		}

		// if server side and got unread bytes in net2engine buffer
		if (status == FINISHED && net2engine.hasRemaining()) {
			result = engine.unwrap(net2engine, engine2app);
			while (result.getStatus() == BUFFER_OVERFLOW) {
				engine2app = enlargeApplicationBuffer(engine, engine2app);
				result = engine.unwrap(net2engine, engine2app);
			}
			status = result.getHandshakeStatus();
			if (result.getStatus() == BUFFER_UNDERFLOW) {
				net2engine.compact();
				return;
			}
		}

		if (status == NOT_HANDSHAKING) {
			if (!net2engine.hasRemaining()) {
				engine2app.flip();
				onRead(toAllocatedByteBuf(engine2app));
				return;
			}
			do {
				result = engine.unwrap(net2engine, engine2app);
				logger.trace("unwrap net data: {} bytes, status: {}, {}", engine2app.position(), result.getStatus(), result.getHandshakeStatus());
				if (result.getStatus() == BUFFER_OVERFLOW) {
					engine2app = enlargeApplicationBuffer(engine, engine2app);
				} else if (result.getStatus() == BUFFER_UNDERFLOW) {
					net2engine.compact();
					return;
				} else {
					if (result.getStatus() == CLOSED) {
						engine.closeInbound();
						readInterest(false);
					}
				}
			} while (net2engine.hasRemaining());
			engine2app.flip();
			onRead(toAllocatedByteBuf(engine2app));
		}

		// if client side
		if (status == FINISHED && app2engine.limit() != app2engine.capacity()) {
			result = engine.wrap(app2engine, engine2net);
			if (result.getStatus() == OK) {
				engine2net.flip();
				writeToChannel(toAllocatedByteBuf(engine2net));

				logger.trace("Send client request: {}/{} bytes; {}, new engine status: {}",
						app2engine.limit(), engine2net.limit(), result.getStatus(), result.getHandshakeStatus());

				onWriteFlushed();
				writeInterest(false);

				// clean-up
				app2engine.clear();
				engine2net.clear();
				net2engine.clear();
			}

		}

		if (status == NEED_TASK) {
			logger.trace("task submitted");
			eventloop.submit(new Runnable() {
				@Override
				public void run() {
					engine.getDelegatedTask().run();
					try {
						processSslMessage(engine.getHandshakeStatus());
						logger.trace("task executed");
					} catch (SSLException e) {
						closeSslEngine();
						onReadException(e);
					}
				}
			});
		}
	}

	private static ByteBuffer handleBufferUnderflow(SSLEngine engine, ByteBuffer buffer) {
		if (buffer.position() < buffer.limit()) {
			return buffer;
		} else {
			ByteBuffer replaceBuffer = enlargePacketBuffer(engine, buffer);
			buffer.flip();
			replaceBuffer.put(buffer);
			return replaceBuffer;
		}
	}

	private static ByteBuffer enlargePacketBuffer(SSLEngine engine, ByteBuffer buffer) {
		return enlargeBuffer(buffer, engine.getSession().getPacketBufferSize());
	}

	private static ByteBuffer enlargeApplicationBuffer(SSLEngine engine, ByteBuffer buffer) {
		return enlargeBuffer(buffer, engine.getSession().getApplicationBufferSize());
	}

	private static ByteBuffer enlargeBuffer(ByteBuffer buffer, int proposedCapacity) {
		if (proposedCapacity > buffer.capacity()) {
			buffer = ByteBuffer.allocate(proposedCapacity);
		} else {
			buffer = ByteBuffer.allocate(buffer.capacity() * 2);
		}
		return buffer;
	}

	private void closeSslEngine() {
		// should be called when exception occurred or on connection close so that to try to close the engine
	}
	//---------------------------------------------------------------------------------------------------------------SSL

	/**
	 * Peeks ByteBuf from writeQueue, and sends its bytes to address.
	 */
	private void doWrite() {
		boolean wasWritten = false;

		while (!writeQueue.isEmpty()) {
			ByteBuf buf = writeQueue.peekBuf();
			ByteBuffer byteBuffer = buf.toByteBuffer();
			int remainingOld = buf.remaining();
			try {
				channelWrite(byteBuffer);
				buf.setByteBuffer(byteBuffer);
			} catch (IOException e) {
				onWriteException(e);
				return;
			}

			int remainingNew = buf.remaining();
			if (remainingNew != remainingOld) {
				wasWritten = true;
			}

			if (remainingNew > 0) {
				break;
			}
			writeQueue.take();
			buf.recycle();
		}

		if (wasWritten) {
			writeTime = eventloop.currentTimeMillis();
		}

		if (writeQueue.isEmpty()) { // TODO(arashev) check
			if (engine == null) {
				onWriteFlushed();
				writeInterest(false);
			}
		} else {
			writeInterest(true);
		}
	}

	protected void write(ByteBuf buf) {
		if (engine != null) {
			try {
				writeWithSslEngine(buf);
			} catch (SSLException e) {
				engine.closeOutbound();
				onWriteException(e);
			}
		} else {
			writeToChannel(buf);
		}
	}

	private void writeToChannel(ByteBuf buf) {
		if (writeQueue.isEmpty()) {
			writeQueue.add(buf);
			doWrite();
		} else {
			writeQueue.add(buf);
		}
	}

	protected int channelWrite(ByteBuffer byteBuffer) throws IOException {
		return channel.write(byteBuffer);
	}

	/**
	 * This method is called if writeInterest is on and it is possible to write to the channel.
	 */
	@Override
	public void onWriteReady() {
		doWrite();
	}

	/**
	 * Before closing this connection it clears readQueue and writeQueue.
	 */
	@Override
	public void onClosed() {
		readQueue.clear();
		writeQueue.clear();
	}

	@Override
	public final SelectableChannel getChannel() {
		return this.channel;
	}

	protected void shutdownInput() throws IOException {
		channel.shutdownInput();
	}

	protected void shutdownOutput() throws IOException {
		channel.shutdownOutput();
	}

	@Nullable
	public InetSocketAddress getRemoteSocketAddress() {
		return remoteSocketAddress;
	}

	public String getChannelInfo() {
		return channel.toString();
	}

	@Override
	protected String getDebugName() {
		return super.getDebugName() + "(" + remoteSocketAddress + ")";
	}
}
