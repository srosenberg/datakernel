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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
	// TODO(arashev) pass from outside
	private ExecutorService executor = Executors.newCachedThreadPool();
	private TcpSocketConnection conn;

	// 32 * 1024 -- recommended buffer size(it has been stated that any message would not exceed the range)
	private ByteBuffer app2engine = ByteBuffer.allocate(32 * 1024); //  keeps raw app data while handshaking
	private ByteBuffer engine2net = ByteBuffer.allocate(16 * 1024); //  keeps encoded data that is to be send to peer
	private ByteBuffer net2engine = ByteBuffer.allocate(32 * 1024); //  keeps encoded data received from peer
	private ByteBuffer engine2app = ByteBuffer.allocate(16 * 1024); //  keeps decoded data that is to be passed to app

	private boolean isLastPieceSend;

	public DatakernelSslEngine(SSLEngine engine) {
		this.engine = engine;
	}

	// api
	@Override
	public void setConnection(TcpSocketConnection conn) {
		this.conn = conn;
	}

	@Override
	public void writeToChannel(ByteBuf buf) {
		try {
			doWrite(buf);
		} catch (SSLException e) {
			onWriteException(e);
		}
	}

	@Override
	public void onRead(ByteBuf buf) {
		try {
			doRead(buf);
		} catch (SSLException e) {
			onReadException(e);
		}
	}

	@Override
	public void onWriteException(IOException e) {
		conn.onWriteException(e);
	}

	@Override
	public void onReadException(IOException e) {
		conn.onReadException(e);
	}

	@Override
	public void onReadEndOfStream() {
		conn.doClose();
	}

	@Override
	public void onWriteFlushed() {
		if (isLastPieceSend) {
			conn.onWriteFlushed();
		}
	}

	@Override
	public void close() {
		logger.trace("closing ssl engine");
		engine.closeOutbound();
		try {
			while (!engine.isOutboundDone()) {
				engine.wrap(app2engine, engine2net);
				engine2net.flip();
				logger.trace("writing to channel close message: {}", engine2net.limit());
				isLastPieceSend = true;
				conn.writeToChannel(toByteBuf(engine2net));
				engine2net.clear();
			}
			conn.doClose();
		} catch (SSLException e) {
			conn.onWriteException(e);
		}
	}

	/* inner
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	* */

	private void doRead(ByteBuf buf) throws SSLException {
		logger.trace("on read {} bytes from channel: {}", buf.remaining(), engine.getHandshakeStatus());

		net2engine = toBuffer(buf, net2engine);
		SSLEngineResult result = unwrap();

		// ensuring we managed to read all the data to app buffer successfully
		while (result.getStatus() == BUFFER_OVERFLOW) {
			engine2app = enlarge2AppBuffer(engine2app);
			result = unwrap();
		}

		// need more bytes to proceed --> exiting
		if (result.getStatus() == BUFFER_UNDERFLOW) {
			net2engine.compact();
			return;
		}

		if (result.getStatus() == CLOSED) {
			engine.closeOutbound();
			doHandShake();
			conn.doClose();
			return;
		}

		// OK
		if (engine.getHandshakeStatus() != NOT_HANDSHAKING) {
			doHandShake();
		} else {
			sendNetDataToPeer();
		}
	}

	private void doWrite(ByteBuf buf) throws SSLException {
		logger.trace("on write {} bytes of app data", buf.remaining());

		app2engine = toBuffer(buf, app2engine);
		SSLEngineResult result = wrap();

		while (result.getStatus() == BUFFER_OVERFLOW) {
			engine2net = enlarge2NetBuffer(engine2net);
			result = wrap();
		}

		// OK
		if (engine.getHandshakeStatus() != NOT_HANDSHAKING) {
			sendToChannel(engine2net);
		} else {
			sendToChannel(engine2net);
			while (app2engine.hasRemaining()) {
				result = wrap();
				while (result.getStatus() == BUFFER_OVERFLOW) {
					engine2net = enlarge2NetBuffer(engine2net);
					result = wrap();
				}
				sendToChannel(engine2net);
			}
		}
	}

	private void sendAppDataToPeer() throws SSLException {
		SSLEngineResult result;

		do {
			result = engine.wrap(app2engine, engine2net);
			if (result.getStatus() == BUFFER_OVERFLOW) {
				engine2net = enlarge2NetBuffer(engine2net);
			} else if (result.getStatus() == OK) {
				engine2net.flip();
				isLastPieceSend = true;
				conn.writeToChannel(toByteBuf(engine2net));
				logger.trace("send app data to peer: {}/{} bytes; {}, new engine status: {}",
						app2engine.limit(), engine2net.limit(), result.getStatus(), result.getHandshakeStatus());
				engine2net.clear();
			}
		} while (app2engine.hasRemaining());

		assert engine.getHandshakeStatus() == NOT_HANDSHAKING;

		// clean-up
		app2engine.clear();
	}

	private void sendNetDataToPeer() throws SSLException {
		assert engine2app.hasRemaining();
		sendToConnection(engine2app);
		while (net2engine.hasRemaining()) {
			SSLEngineResult result = unwrap();

			while (result.getStatus() == BUFFER_OVERFLOW) {
				engine2app = enlarge2AppBuffer(engine2app);
				result = unwrap();
			}

			// need more bytes to proceed --> exiting
			if (result.getStatus() == BUFFER_UNDERFLOW) {
				net2engine.compact();
				return;
			}

			if (result.getStatus() == CLOSED) {
				engine.closeOutbound();
				doHandShake();
				conn.doClose();
			}

			if (result.getStatus() == OK) {
				sendToConnection(engine2app);
			}
		}
		net2engine.clear();
		assert engine.getHandshakeStatus() == NOT_HANDSHAKING;
	}

	private SSLEngineResult wrap() throws SSLException {
		SSLEngineResult result = engine.wrap(app2engine, engine2net);
		logger.trace("wrap {} bytes ({} bytes left): {}, new engine status: {}",
				engine2net.position(), engine2net.remaining(), result.getStatus(), result.getHandshakeStatus());
		return result;
	}

	private SSLEngineResult unwrap() throws SSLException {
		SSLEngineResult result = engine.unwrap(net2engine, engine2app);
		if (logger.isTraceEnabled()) {
			logger.trace("unwrap {} bytes ({} bytes left): {}, new engine status: {}",
					net2engine.position(), net2engine.remaining(), result.getStatus(), result.getHandshakeStatus());
		}
		return result;
	}

	private void doHandShake() throws SSLException {
		HandshakeStatus status = engine.getHandshakeStatus();
		SSLEngineResult result = null;

		// handshake processes
		while (status != FINISHED && status != NOT_HANDSHAKING && status != NEED_TASK) {
			if (status == NEED_WRAP) {
				result = wrap();
				if (result.getStatus() == BUFFER_OVERFLOW) {
					engine2net = enlarge2NetBuffer(engine2net);
				} else if (result.getStatus() == OK) {
					sendToChannel(engine2net);
				} else if (result.getStatus() == CLOSED) {
					net2engine.compact();
				}
				status = engine.getHandshakeStatus();
			} else {
				if (net2engine.hasRemaining()) {
					result = unwrap();
					status = engine.getHandshakeStatus();
					if (result.getStatus() == BUFFER_UNDERFLOW) {
						net2engine = handleUnderflow(net2engine);
						net2engine.compact();
						break;
					} else if (result.getStatus() == BUFFER_OVERFLOW) {
						net2engine = enlarge2NetBuffer(net2engine);
					}
				} else {
					break;
				}
			}
		}

		// if server side, finished handshake and still got unread bytes in net2engine buffer
		if (status == NOT_HANDSHAKING && net2engine.hasRemaining()) {
			result = unwrap();

			status = engine.getHandshakeStatus();
			while (result.getStatus() == BUFFER_OVERFLOW) {
				engine2app = enlarge2AppBuffer(engine2app);
				result = engine.unwrap(net2engine, engine2app);
			}

			if (result.getStatus() == BUFFER_UNDERFLOW) {
				net2engine.compact();
				return;
			}

			sendToConnection(engine2app);
		}

		// if client side, finished handshake --> need to send app data
		if (status == NOT_HANDSHAKING && app2engine.limit() != app2engine.capacity()) {
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
				sendToConnection(engine2app);
				return;
			}
			do {
				result = engine.unwrap(net2engine, engine2app);
				logger.trace("unwrap net data: {} bytes, status: {}, {}", engine2app.position(), result.getStatus(), result.getHandshakeStatus());
				if (result.getStatus() == BUFFER_OVERFLOW) {
					engine2app = enlarge2AppBuffer(engine2app);
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

		// need task close
		if (status == NEED_TASK) {
			logger.trace("need task");
			Runnable task;
			while ((task = engine.getDelegatedTask()) != null) {
				final Runnable finalTask = task;
				logger.trace("task submitted");
				task.run();
				doHandShake();
//				executor.execute(new Runnable() {
//					@Override
//					public void run() {
//						finalTask.run();
//						logger.trace("task executed");
//						try {
//							doHandShake();
//						} catch (SSLException e) {
//							e.printStackTrace();
//						}
//					}
//				});
			}
		}
	}

	private void sendToChannel(ByteBuffer buffer) {
		buffer.flip();
		conn.writeToChannel(toByteBuf(buffer));
		logger.trace("{} bytes is send to channel: {}", buffer.limit(), engine.getHandshakeStatus());
		buffer.clear();
	}

	private void sendToConnection(ByteBuffer buffer) {
		buffer.flip();
		conn.onRead(toByteBuf(buffer));
		logger.trace("{} bytes is send to connection: {}", buffer.limit(), engine.getHandshakeStatus());
		buffer.clear();
	}


	/*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	*
	* */

	private ByteBuf toByteBuf(ByteBuffer buffer) {
		ByteBuf buf = allocate(buffer.remaining());
		buf.put(buffer.array(), buffer.arrayOffset(), buffer.limit());
		buf.flip();
		return buf;
	}

	private ByteBuffer toBuffer(ByteBuf buf, ByteBuffer buffer) {
		if (buffer.remaining() < buf.remaining()) {
			buffer = enlargeBuffer(buffer, buf.remaining() * 2);
		}
		buffer.put(buf.array(), buf.position(), buf.limit());
		buffer.flip();
		buf.recycle();
		return buffer;
	}

	private ByteBuffer handleUnderflow(ByteBuffer buffer) {
		if (buffer.position() < buffer.limit()) {
			return buffer;
		} else {
			ByteBuffer replaceBuffer = enlarge2NetBuffer(buffer);
			buffer.flip();
			replaceBuffer.put(buffer);
			return replaceBuffer;
		}
	}

	private ByteBuffer enlarge2NetBuffer(ByteBuffer buffer) {
		return enlargeBuffer(buffer, engine.getSession().getPacketBufferSize());
	}

	private ByteBuffer enlarge2AppBuffer(ByteBuffer buffer) {
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
