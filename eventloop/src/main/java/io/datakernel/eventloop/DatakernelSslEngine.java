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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;

import static io.datakernel.bytebuf.ByteBufPool.allocate;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;
import static javax.net.ssl.SSLEngineResult.Status.*;

/*
 * One must ensure strict sequential order of handshake messages!
 * Unexpected sequence order can lead to critical or even fatal results
 */
public final class DatakernelSslEngine implements TcpFilter {
	private static final Logger logger = LoggerFactory.getLogger(DatakernelSslEngine.class);

	private final SSLEngine engine;
	private TcpSocketConnection conn;

	// 32 * 1024 -- recommended buffer size(it has been stated that any message would not exceed the range)
	private ByteBuffer app2engine = ByteBuffer.allocate(32 * 1024); //  keeps raw app data while handshaking
	private ByteBuffer engine2net = ByteBuffer.allocate(32 * 1024); //  keeps encoded data that is to be send to peer
	private ByteBuffer net2engine = ByteBuffer.allocate(32 * 1024); //  keeps encoded data received from peer
	private ByteBuffer engine2app = ByteBuffer.allocate(32 * 1024); //  keeps decoded data that is to be passed to app

	public DatakernelSslEngine(SSLEngine engine) {
		this.engine = engine;
	}

	@Override
	public void setConnection(TcpSocketConnection conn) {
		this.conn = conn;
	}

	/**
	 * Initiates process
	 * <p>
	 * Is always being initiated by agent(both server responding and client requesting) that is
	 * willing to pass the app raw data(response/request) to peer.
	 * <p>
	 * Could only enter this method iff this is a client connection and it is sends a request or
	 * this is a server connection, succeed to handshake, applied some processing logic and now
	 * eager to encode response so that to pass it to client.
	 */
	@Override
	public void write(ByteBuf buf) throws SSLException {
		logger.trace("on write {} bytes of app data", buf.remaining());
		assert engine.getHandshakeStatus() == NOT_HANDSHAKING;

		app2engine = putInBufferAndRecycle(buf, app2engine);

		SSLEngineResult result;
		boolean isEncoded = false;
		while (!isEncoded) {
			result = engine.wrap(app2engine, engine2net);
			logger.trace("wrap {} bytes ({} bytes left): {}, new engine status: {}",
					engine2net.position(), app2engine.remaining(), result.getStatus(), result.getHandshakeStatus());
			switch (result.getStatus()) {
				case BUFFER_OVERFLOW:
					engine2net = enlargePacketBuffer(engine, engine2net);
					break;
				case OK:
					isEncoded = true;
					break;
				// impossible to get CLOSE and BUFFER_UNDERFLOW
				default:
					throw new SSLException("Invalid operation status: " + result.getStatus());
			}
		}

		engine2net.flip();
		conn.writeToChannel(toAllocatedByteBuf(engine2net));
		logger.trace("write {} bytes to channel: {}", engine2net.limit(), engine.getHandshakeStatus());
		engine2net.clear();
	}

	/*
	 *  Called on new piece of encoded data being received.
	 *
	 *  If manage to unwrap --> pass to buffer, otherwise - wait for more data
	 *
	 */
	@Override
	public void read(ByteBuf buf) throws SSLException {
		logger.trace("on read {} bytes from channel: {}", buf.remaining(), engine.getHandshakeStatus());

		// assume buf in 'read' mode
		net2engine = putInBufferAndRecycle(buf, net2engine);

		SSLEngineResult result = engine.unwrap(net2engine, engine2app);
		logger.trace("unwrap {} bytes ({} bytes left): {}, new engine status: {}",
				net2engine.position(), net2engine.remaining(), result.getStatus(), result.getHandshakeStatus());

		// ensuring we managed to read all the data to app buffer successfully
		while (result.getStatus() == BUFFER_OVERFLOW) {
			engine2app = enlargeApplicationBuffer(engine, engine2app);
			result = engine.unwrap(net2engine, engine2app);
			logger.trace("unwrap {} bytes ({} bytes left): {}, new engine status: {}",
					net2engine.position(), net2engine.remaining(), result.getStatus(), result.getHandshakeStatus());
		}

		// need more bytes to proceed -->  exiting
		if (result.getStatus() == BUFFER_UNDERFLOW) {
			net2engine.compact();
			return;
		}

		processSslMessage(result.getHandshakeStatus());
	}

	@Override
	public void onEndOfStream() throws Exception {
		logger.trace("received \'end of stream\'");
		engine.closeOutbound();
		processSslMessage(engine.getHandshakeStatus());
	}

	// should be called when exception occurred or on connection close so that to try to close the engine
	@Override
	public void close() {
		engine.closeOutbound();
		try {
			processSslMessage(engine.getHandshakeStatus());
		} catch (SSLException e) {
			e.printStackTrace();
		}
		conn.readQueue.clear();
		conn.writeQueue.clear();
	}

	private void processSslMessage(HandshakeStatus status) throws SSLException {
		SSLEngineResult result;

		// doing handshake/closing conn/ etc
		boolean processed = false;
		while (status == NEED_WRAP || (status == NEED_UNWRAP && !processed)) {
			if (status == NEED_WRAP) {
				result = engine.wrap(app2engine, engine2net);
				status = result.getHandshakeStatus();
				logger.trace("wrap {} bytes: {}, new engine status: {}",
						engine2net.position(), result.getStatus(), result.getHandshakeStatus());
				if (result.getStatus() == BUFFER_OVERFLOW) {
					engine2net = enlargePacketBuffer(engine, engine2net);
				} else if (result.getStatus() == OK || result.getStatus() == CLOSED) {
					engine2net.flip();
					logger.trace("write {} bytes to channel: {}", engine2net.limit(), engine.getHandshakeStatus());
					conn.writeToChannel(toAllocatedByteBuf(engine2net));
					engine2net.clear();
				} else {
					throw new SSLException("Illegal state: " + result.getStatus());
				}
			} else {
				if (net2engine.hasRemaining()) {
					result = engine.unwrap(net2engine, engine2app);
					logger.trace("unwrap {} bytes ({} bytes left): {}, new engine status: {}",
							net2engine.position(), net2engine.remaining(), result.getStatus(), result.getHandshakeStatus());
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

		// if server side, finished handshake and still got unread bytes in net2engine buffer
		if (status == FINISHED && net2engine.hasRemaining()) {
			result = engine.unwrap(net2engine, engine2app);

			logger.trace("unwrap {} bytes ({} bytes left): {}, new engine status: {}",
					net2engine.position(), net2engine.remaining(), result.getStatus(), result.getHandshakeStatus());

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

		// if client side, finished handshake --> need to send app data
		if (status == FINISHED && app2engine.limit() != app2engine.capacity()) {
			result = engine.wrap(app2engine, engine2net);
			if (result.getStatus() == OK) {
				engine2net.flip();
				conn.writeToChannel(toAllocatedByteBuf(engine2net));

				logger.trace("Send client request: {}/{} bytes; {}, new engine status: {}",
						app2engine.limit(), engine2net.limit(), result.getStatus(), result.getHandshakeStatus());

				conn.onWriteFlushed();
				conn.writeInterest(false);

				// clean-up
				app2engine.clear();
				engine2net.clear();
				net2engine.clear();
			}

		}

		if (status == NOT_HANDSHAKING) {
			if (!net2engine.hasRemaining()) {
				engine2app.flip();
				conn.onRead(toAllocatedByteBuf(engine2app));
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
						conn.readInterest(false);
					}
				}
			} while (net2engine.hasRemaining());
			engine2app.flip();
			conn.onRead(toAllocatedByteBuf(engine2app));
		}

		if (status == NEED_TASK) {
			logger.trace("task submitted");
			conn.eventloop.submit(new Runnable() {
				@Override
				public void run() {
					engine.getDelegatedTask().run();
					try {
						processSslMessage(engine.getHandshakeStatus());
						logger.trace("task executed");
					} catch (SSLException e) {
						close();
						conn.onReadException(e);
					}
				}
			});
		}
	}

	private ByteBuffer putInBufferAndRecycle(ByteBuf buf, ByteBuffer buffer) {
		if (buffer.remaining() < buf.remaining()) {
			buffer = enlargeBuffer(buffer, buffer.remaining() * 2);
		}
		buffer.put(buf.array(), buf.position(), buf.limit());
		buffer.flip();
		buf.recycle();
		return buffer;
	}

	private ByteBuf toAllocatedByteBuf(ByteBuffer buffer) {
		ByteBuf buf = allocate(buffer.remaining());
		buf.put(buffer.array(), buffer.arrayOffset(), buffer.limit());
		buf.flip();
		return buf;
	}

	private ByteBuffer handleBufferUnderflow(SSLEngine engine, ByteBuffer buffer) {
		if (buffer.position() < buffer.limit()) {
			return buffer;
		} else {
			ByteBuffer replaceBuffer = enlargePacketBuffer(engine, buffer);
			buffer.flip();
			replaceBuffer.put(buffer);
			return replaceBuffer;
		}
	}

	private ByteBuffer enlargePacketBuffer(SSLEngine engine, ByteBuffer buffer) {
		return enlargeBuffer(buffer, engine.getSession().getPacketBufferSize());
	}

	private ByteBuffer enlargeApplicationBuffer(SSLEngine engine, ByteBuffer buffer) {
		return enlargeBuffer(buffer, engine.getSession().getApplicationBufferSize());
	}

	private ByteBuffer enlargeBuffer(ByteBuffer buffer, int proposedCapacity) {
		if (proposedCapacity > buffer.capacity()) {
			buffer = ByteBuffer.allocate(proposedCapacity);
		} else {
			buffer = ByteBuffer.allocate(buffer.capacity() * 2);
		}
		return buffer;
	}
}
