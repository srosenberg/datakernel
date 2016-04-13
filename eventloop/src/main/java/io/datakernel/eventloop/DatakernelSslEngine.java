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
					engine2net.flip();
					isEncoded = true;
					break;
				case CLOSED:
					onClose();
					return;
				default:
					throw new SSLException("Invalid operation status: " + result.getStatus());
			}
		}

		conn.writeToChannel(toByteBuf(engine2net));
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

		// assume buffer to be in 'read' mode
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

		if (result.getStatus() == CLOSED) {
			engine.closeInbound();
			onClose();
		}

		// need more bytes to proceed -->  exiting
		if (result.getStatus() == BUFFER_UNDERFLOW) {
			net2engine.compact();
			return;
		}

		processSslMessage();
	}

	private void onClose() {
		logger.trace("closing connection");
		try {
			engine.closeInbound();
		} catch (SSLException e) {
			conn.onReadException(e);
		}
	}

	private void processSslMessage() throws SSLException {
		HandshakeStatus status = engine.getHandshakeStatus();
		SSLEngineResult result = null;

		while (status != FINISHED && status != NOT_HANDSHAKING && status != NEED_TASK) {
			if (status == NEED_WRAP) {
				result = engine.wrap(app2engine, engine2net);
				status = result.getHandshakeStatus();
				logger.trace("wrap {} bytes: {}, new engine status: {}", engine2net.position(), result.getStatus(), result.getHandshakeStatus());
				if (result.getStatus() == BUFFER_OVERFLOW) {
					engine2net = enlargePacketBuffer(engine, engine2net);
				} else if (result.getStatus() == OK) {
					engine2net.flip();
					logger.trace("write {} bytes to channel", engine2net.limit());
					conn.writeToChannel(toByteBuf(engine2net));
					engine2net.clear();
				} else if (result.getStatus() == CLOSED) {
					net2engine.compact();
					// empty
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
						break;
					} else if (result.getStatus() == BUFFER_OVERFLOW) {
						net2engine = enlargePacketBuffer(engine, net2engine);
					}
				} else {
					break;
				}
			}
		}

		// if server side, finished handshake and still got unread bytes in net2engine buffer
		if (status == FINISHED && net2engine.hasRemaining()) {
			result = engine.unwrap(net2engine, engine2app);
			status = result.getHandshakeStatus();

			logger.trace("unwrap {} bytes ({} bytes left): {}, new engine status: {}",
					net2engine.position(), net2engine.remaining(), result.getStatus(), result.getHandshakeStatus());

			while (result.getStatus() == BUFFER_OVERFLOW) {
				engine2app = enlargeApplicationBuffer(engine, engine2app);
				result = engine.unwrap(net2engine, engine2app);
			}

			if (result.getStatus() == BUFFER_UNDERFLOW) {
				net2engine.compact();
				return;
			}
		}

		// if client side, finished handshake --> need to send app data
		if (status == FINISHED && app2engine.limit() != app2engine.capacity()) {
			sendAppDataToPeer();
			return;
		}

		// means more data is being send through the channel
		if (status == NOT_HANDSHAKING) {
			if (result != null && result.getStatus() == CLOSED) {
				engine2net.flip();
				logger.trace("write {} bytes to channel", engine2net.limit());
				conn.writeToChannel(toByteBuf(engine2net));
				engine2net.clear();
				return;
			}

			if (!net2engine.hasRemaining()) {
				engine2app.flip();
				conn.onRead(toByteBuf(engine2app));
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
					}
				}
			} while (net2engine.hasRemaining());
			engine2app.flip();
			conn.onRead(toByteBuf(engine2app));
		}

		if (status == NEED_TASK) {
			logger.trace("scheduling task");
			conn.eventloop.post(new Runnable() {
				@Override
				public void run() {
					engine.getDelegatedTask().run();
					logger.trace("task executed");
					try {
						processSslMessage();
					} catch (SSLException e) {
						conn.onReadException(e);
					}
				}
			});
		}
	}

	@Override
	public boolean isDataToPeerWrapped() {
		return dataToPeerIsWrapped;
	}

	private boolean dataToPeerIsWrapped;

	private void sendAppDataToPeer() throws SSLException {
		SSLEngineResult result = engine.wrap(app2engine, engine2net);

		if (result.getStatus() == OK) {
			engine2net.flip();

			dataToPeerIsWrapped = true;

			conn.writeToChannel(toByteBuf(engine2net));

			logger.trace("send app data to peer: {}/{} bytes; {}, new engine status: {}",
					app2engine.limit(), engine2net.limit(), result.getStatus(), result.getHandshakeStatus());

			// clean-up
			app2engine.clear();
			engine2net.clear();
			engine2app.clear();
			net2engine.clear();
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

	private ByteBuf toByteBuf(ByteBuffer buffer) {
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
