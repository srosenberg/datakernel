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
import io.datakernel.bytebuf.ByteBufQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.datakernel.bytebuf.ByteBufPool.allocate;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;
import static javax.net.ssl.SSLEngineResult.Status.*;

/*
* One must ensure strict sequential order of handshake messages!
* Unexpected sequence order can lead to critical or even fatal results
* */
public final class DatakernelSslEngine implements TcpFilter {
	private static final Logger logger = LoggerFactory.getLogger(DatakernelSslEngine.class);

	private final SSLEngine engine;
	private final ExecutorService executor;
	private TcpSocketConnection conn;

	/*
	*   reusable queue analogue used to keep state
	*   32 * 1024 -- buffer size that depends on the byteBufs sizes being send to the engine
	*   bufs that that are being used by the engine are expected not to exceed 16kb size
	*/
	private ByteBuffer app2engine = ByteBuffer.allocate(32 * 1024); //  used to store raw app data
	private ByteBuffer engine2net = ByteBuffer.allocate(16 * 1024); //  filled by SSLEngine with encoded data that is to be send to a remote peer
	private ByteBuffer net2engine = ByteBuffer.allocate(32 * 1024); //  encoded data received from peer that is to be passed to SSLEngine
	private ByteBuffer engine2app = ByteBuffer.allocate(16 * 1024); //  decoded data that is to be passed to app

	//   used to denote whether the engine finished with data transfer
	private boolean isLastPieceSend;

	public DatakernelSslEngine(SSLEngine engine, ExecutorService executor) {
		this.engine = engine;
		this.executor = executor;
	}

	@Override
	public void setConnection(TcpSocketConnection conn) {
		this.conn = conn;
	}

	@Override
	public void writeToChannel(ByteBuf buf) {
		logger.trace("on write {} bytes of app data", buf.remaining());
		// bug? concurrent app2engine modification(in executor after completion of a delegated task(small possibility, still exists))
		app2engine = toBuffer(buf, app2engine);
		try {
			doWrite();
		} catch (SSLException e) {
			onWriteException(e);
		}
	}

	private ByteBufQueue readQueue = new ByteBufQueue();
	private AtomicBoolean isProcessing = new AtomicBoolean(false);

	@Override
	public void onRead(ByteBuf buf) {
		logger.trace("on read {} bytes from channel: {}", buf.remaining(), engine.getHandshakeStatus());
		/*
		*   should prevent from concurrent modification of state engine as delegated task would be executed in the separate thread
		*   !!! RACE
		*   when the delegated task changes state of the engine?
		*   what if the buffer is being read at the moment?
		*   !!!bug concurrent net2engine modification
		* */
		if (isProcessing.get()) {
			readQueue.add(buf);
			return;
		}

		net2engine = toBuffer(buf, net2engine);

		try {
			doRead();
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
		logger.trace("received end of stream: {}", engine.getHandshakeStatus());
		try {
			engine.closeInbound();
		} catch (SSLException e) {
			logger.warn("trying to close connection without receiving a proper close notification message");
			closeConnection();
		}
	}

	/*
	*   Is being called on the last piece of app data send
	* */
	@Override
	public void onWriteFlushed() {
		logger.trace("on empty write queue, send user data: {}", isLastPieceSend);
		if (isLastPieceSend) {
			isLastPieceSend = false; // prevent from firing onWriteFlushed() for close auxiliary messages
			conn.onWriteFlushed();
		}
	}

	@Override
	public void close() {
		logger.trace("closing ssl engine");
		engine.closeOutbound();
		try {
			doHandShake();
			conn.doClose();
		} catch (SSLException e) {
			logger.warn("exception while trying to close: {}", e.getMessage());
		}
	}

	private void doWrite() throws SSLException {
		SSLEngineResult result = wrap();

		if (result.getStatus() == CLOSED || result.getStatus() == BUFFER_UNDERFLOW) {
			throw new SSLException("Illegal status while trying to write app data: " + result.getStatus());
		}

		if (result.getHandshakeStatus() == NOT_HANDSHAKING && !app2engine.hasRemaining()) {
			isLastPieceSend = true;
		}

		sendPieceToNet(engine2net);

		if (result.getHandshakeStatus() == NOT_HANDSHAKING) {
			writeToNet();
		}
	}

	private void doRead() throws SSLException {
		SSLEngineResult result = unwrap();

		// shows that we need more bytes to proceed -> preparing buffer and exiting
		if (result.getStatus() == BUFFER_UNDERFLOW) {
			net2engine = handleUnderflow(net2engine);
			net2engine.compact();
			return;
		}

		// remote peer wants to close the connection
		if (result.getStatus() == CLOSED) {
			logger.trace("received close message from peer");
			engine.closeInbound();  // peer has closed the connection -> no more data would be send
			engine.closeOutbound(); // closing this side -> need wrap
		}

		// OK
		if (engine.getHandshakeStatus() != NOT_HANDSHAKING) {
			doHandShake();
		} else {
			// consider ready to read from server / ready to send app data to server
			if (canReadFrom(app2engine)) {
				writeToNet();
			} else {
				writeToApp();
			}
		}
	}

	private void doHandShake() throws SSLException {
		HandshakeStatus status = engine.getHandshakeStatus();
		SSLEngineResult result;

		// handshake processes
		while (status != FINISHED && status != NOT_HANDSHAKING && status != NEED_TASK) {
			if (status == NEED_WRAP) {
				result = wrap();
				if (result.getStatus() == OK) {
					sendPieceToNet(engine2net);
				} else if (result.getStatus() == CLOSED) {
					sendPieceToNet(engine2net);
					net2engine.clear();
				} else {
					throw new SSLException("Illegal status while trying to write engine data: " + result.getStatus());
				}
			} else {
				if (canReadFrom(net2engine)) {
					result = unwrap();
					if (result.getStatus() == OK && result.getHandshakeStatus() == FINISHED) {
						logger.trace("finished unwrap");
					} else if (result.getStatus() == BUFFER_UNDERFLOW) {
						net2engine = handleUnderflow(net2engine);
						net2engine.compact();
						return;
					} else if (result.getStatus() == CLOSED) {
						// unpack close message while handshaking
						engine.closeOutbound(); //  --> NEED_WRAP
						net2engine.clear();
					}
				} else {
					net2engine.clear();
					return;
				}
			}
			status = engine.getHandshakeStatus();
		}

		// if client side, finished handshake --> need to send app data
		if (status == NOT_HANDSHAKING && canReadFrom(app2engine)) {
			doWrite();
			return;
		}

		// if server side, finished handshake and still got unread bytes in net2engine buffer
		if (status == NOT_HANDSHAKING && canReadFrom(net2engine)) {
			writeToNet();
			return;
		}

		// need task
		if (status == NEED_TASK) {
			isProcessing.set(true);
			logger.trace("need task");
			Runnable task;
			while ((task = engine.getDelegatedTask()) != null) {
				final Runnable finalTask = task;
				executor.execute(new Runnable() {
					@Override
					public void run() {
						finalTask.run();
						logger.trace("task executed");
						try {
							doHandShake();
							if (readQueue.isEmpty()) {
								isProcessing.set(false);
							} else {
								net2engine = toBuffer(readQueue.takeRemaining(), net2engine);
								readQueue.clear();
								doRead();
							}
						} catch (SSLException e) {
							onReadException(e);
						}
					}
				});
			}
		}
	}

	private boolean canReadFrom(ByteBuffer buffer) {
		return buffer.hasRemaining() && buffer.limit() != buffer.capacity();
	}

	private void writeToNet() throws SSLException {
		SSLEngineResult result;
		while (app2engine.hasRemaining() && engine.getHandshakeStatus() == NOT_HANDSHAKING) {
			result = wrap();

			if (result.getStatus() == CLOSED || result.getStatus() == BUFFER_UNDERFLOW) {
				throw new SSLException("Illegal status while trying to write app data: " + result.getStatus());
			}

			if (!app2engine.hasRemaining()) {
				isLastPieceSend = true;
			}

			sendPieceToNet(engine2net);
		}
		app2engine.clear();
	}

	private void writeToApp() throws SSLException {
		sendPieceToApp(engine2app);
		while (canReadFrom(net2engine)) {
			SSLEngineResult result = unwrap();

			if (result.getStatus() == BUFFER_UNDERFLOW) {
				net2engine = handleUnderflow(net2engine);
				net2engine.compact();
				return;
			}

			if (result.getStatus() == CLOSED) {
				logger.trace("received close message from peer");
				engine.closeInbound(); // peer has closed the connection
				engine.closeOutbound();
				doHandShake();
				return;
			}

			if (result.getStatus() == OK) {
				sendPieceToApp(engine2app);
			}
		}
		net2engine.clear();
	}

	private void sendPieceToApp(ByteBuffer buffer) {
		buffer.flip();
		logger.trace("{} bytes send to app: {}", buffer.limit(), engine.getHandshakeStatus());
		conn.onRead(toBuf(buffer));
		buffer.clear();
	}

	private void sendPieceToNet(ByteBuffer buffer) {
		buffer.flip();
		logger.trace("{} bytes send to channel: {}", buffer.limit(), engine.getHandshakeStatus());
		conn.writeToChannel(toBuf(buffer));
		buffer.clear();
	}

	private SSLEngineResult wrap() throws SSLException {
		SSLEngineResult result = engine.wrap(app2engine, engine2net);
		logger.trace("wrap {} bytes ({} bytes left): {}, new engine status: {}",
				app2engine.position(), app2engine.remaining(), result.getStatus(), result.getHandshakeStatus());
		while (result.getStatus() == BUFFER_OVERFLOW) {
			engine2net = enlargeNetBuffer(engine2net);
			result = engine.wrap(app2engine, engine2net);
			logger.trace("wrap {} bytes ({} bytes left): {}, new engine status: {}",
					engine2net.position(), app2engine.remaining(), result.getStatus(), result.getHandshakeStatus());
		}
		return result;
	}

	private SSLEngineResult unwrap() throws SSLException {
		SSLEngineResult result = engine.unwrap(net2engine, engine2app);
		logger.trace("unwrap {} bytes ({} bytes left): {}, new engine status: {}",
				net2engine.position(), engine2app.position(), result.getStatus(), result.getHandshakeStatus());
		while (result.getStatus() == BUFFER_OVERFLOW) {
			engine2app = enlargeAppBuffer(engine2app);
			result = engine.unwrap(net2engine, engine2app);
			logger.trace("unwrap {} bytes ({} bytes left): {}, new engine status: {}",
					net2engine.position(), engine2app.position(), result.getStatus(), result.getHandshakeStatus());
		}
		return result;
	}

	private void closeConnection() {
		engine.closeOutbound();
		try {
			doHandShake();
		} catch (SSLException e) {
			onReadException(e);
		} finally {
			conn.doClose();
		}
	}

	//==================================================================================================================
	private ByteBuf toBuf(ByteBuffer buffer) {
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
			ByteBuffer replaceBuffer = enlargeNetBuffer(buffer);
			buffer.flip();
			replaceBuffer.put(buffer);
			return replaceBuffer;
		}
	}

	private ByteBuffer enlargeNetBuffer(ByteBuffer buffer) {
		return enlargeBuffer(buffer, engine.getSession().getPacketBufferSize());
	}

	private ByteBuffer enlargeAppBuffer(ByteBuffer buffer) {
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
