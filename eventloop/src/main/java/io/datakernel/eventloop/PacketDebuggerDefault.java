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
import io.datakernel.bytebuf.BytesHexFormatter;
import io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public final class PacketDebuggerDefault implements PacketDebugger {
	private final BytesHexFormatter hexFormatter;
	private final Writer writer;
	private final Filter filter;

	// region builders
	private PacketDebuggerDefault(BytesHexFormatter hexFormatter, Writer writer, Filter filter) {
		this.hexFormatter = hexFormatter;
		this.writer = writer;
		this.filter = filter;
	}

	public static PacketDebuggerDefault create() {
		BytesHexFormatter hexFormatter = BytesHexFormatter.create()
				.withMaxLine(32)
				.withMaxColumn(16)
				.withMaxHead(32 * 4)
				.withMaxTail(32 * 2);

		Writer writer = WriterToLogger.create(LoggerFactory.getLogger(PacketDebuggerDefault.class));

		Filter filter = new Filter() {
			@Override
			public boolean acceptRead(AsyncTcpSocketDebug socket, ByteBuf packet) {
				return true;
			}

			@Override
			public boolean acceptWrite(AsyncTcpSocketDebug socket, ByteBuf packet, boolean partialFlush) {
				return true;
			}
		};

		return new PacketDebuggerDefault(hexFormatter, writer, filter);
	}

	public PacketDebuggerDefault withBytesHexFormatter(BytesHexFormatter formatter) {
		return new PacketDebuggerDefault(formatter, writer, filter);
	}

	public PacketDebuggerDefault withLogger(Logger logger) {
		return new PacketDebuggerDefault(hexFormatter, WriterToLogger.create(logger), filter);
	}

	public PacketDebuggerDefault withWriter(Writer writer) {
		return new PacketDebuggerDefault(hexFormatter, writer, filter);
	}

	public PacketDebuggerDefault withFilter(Filter filter) {
		return new PacketDebuggerDefault(hexFormatter, writer, filter);
	}
	// endregion

	@Override
	public void onRead(AsyncTcpSocketDebug socket, ByteBuf packet) {
		Eventloop eventloop = socket.getEventloop();
		SocketChannel channel = socket.getSocketChannel();
		EventHandler handler = socket.getEventHandler();

		StringBuilder debugInfo = new StringBuilder();

		// header
		String transport = formatTransportProtocol(socket);
		int globalTick = eventloop.getGlobalTick();
		int localTick = eventloop.getLocalTick();
		debugInfo.append(String.format("@%d.%d %s Read\n", globalTick, localTick, transport));

		// addresses
		try {
			String addresses = formatAddresses(channel);
			debugInfo.append(addresses);
		} catch (Exception e) {
			writer.write("Address cannot be determined: " + e.toString());
		}

		// event handler
		String handlerInfo = formatHandler(handler);
		debugInfo.append(handlerInfo);

		// bytebuf
		debugInfo.append(hexFormatter.format(packet.array(), packet.readPosition(), packet.readRemaining()));

		writer.write(debugInfo.toString());
	}

	@Override
	public void onWrite(AsyncTcpSocketDebug socket, ByteBuf packet, boolean partialFlush) {
		Eventloop eventloop = socket.getEventloop();
		SocketChannel channel = socket.getSocketChannel();
		EventHandler handler = socket.getEventHandler();

		StringBuilder debugInfo = new StringBuilder();

		// header
		String transport = formatTransportProtocol(socket);
		int globalTick = eventloop.getGlobalTick();
		int localTick = eventloop.getLocalTick();
		debugInfo.append(String.format("@%d.%d %s Write", globalTick, localTick, transport));
		if (partialFlush) {
			debugInfo.append(" PF");
		}
		debugInfo.append("\n");

		// addresses
		try {
			String addresses = formatAddresses(channel);
			debugInfo.append(addresses);
		} catch (Exception e) {
			writer.write("Address cannot be determined: " + e.toString());
		}

		// event handler
		String handlerInfo = formatHandler(handler);
		debugInfo.append(handlerInfo);

		// bytebuf
		debugInfo.append(hexFormatter.format(packet.array(), packet.readPosition(), packet.readRemaining()));

		writer.write(debugInfo.toString());
	}

	private static String formatHandler(EventHandler handler) {
		String handlerInfo = "";
		if (handler != null) {
			handlerInfo = String.format("Handler: %s  %s\n", handler.getClass().getName(), handler.toString());
		}
		return handlerInfo;
	}

	private static String formatAddresses(SocketChannel channel) throws IOException {
		String localAddr = channel.getLocalAddress().toString();
		String remoteAddr = channel.getRemoteAddress().toString();
		return String.format("Local address: %s  Remote address: %s\n", localAddr, remoteAddr);
	}

	private static String formatTransportProtocol(AsyncTcpSocketDebug socket) {
		String transport = "";
		if (socket instanceof AsyncTcpSocketImpl) {
			transport = "TCP";
		} else if (socket instanceof AsyncSslSocket) {
			transport = "SSL";
		}
		return transport;
	}

	public interface Writer {
		void write(String info);
	}

	private static final class WriterToLogger implements Writer {
		private final Logger logger;

		private WriterToLogger(Logger logger) {
			this.logger = logger;
		}

		public static WriterToLogger create(Logger logger) {
			return new WriterToLogger(logger);
		}

		@Override
		public void write(String info) {
			logger.debug(info);
		}
	}

	public interface Filter {
		boolean acceptRead(AsyncTcpSocketDebug socket, ByteBuf packet);

		boolean acceptWrite(AsyncTcpSocketDebug socket, ByteBuf packet, boolean partialFlush);
	}
}
