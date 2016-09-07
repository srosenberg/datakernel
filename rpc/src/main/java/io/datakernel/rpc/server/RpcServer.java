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

package io.datakernel.rpc.server;

import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.util.*;

import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

public final class RpcServer extends AbstractServer<RpcServer> {
	private Logger logger = getLogger(RpcServer.class);
	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS = new ServerSocketSettings(16384);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = new SocketSettings().tcpNoDelay(true);

	private final Map<Class<?>, RpcRequestHandler<?, ?>> handlers = new HashMap<>();
	private RpcProtocolFactory protocolFactory = streamProtocol();
	private SerializerBuilder serializerBuilder;
	private final Set<Class<?>> messageTypes = new LinkedHashSet<>();

	private final List<RpcServerConnection> connections = new ArrayList<>();

	private BufferSerializer<RpcMessage> serializer;

	// jmx
	private CountStats connectionsCount = new CountStats();
	private EventStats totalConnects = new EventStats();
	private Map<InetAddress, EventStats> connectsPerAddress = new HashMap<>();
	private EventStats successfulRequests = new EventStats();
	private EventStats failedRequests = new EventStats();
	private ValueStats requestHandlingTime = new ValueStats();
	private ExceptionStats lastRequestHandlingException = new ExceptionStats();
	private ExceptionStats lastProtocolError = new ExceptionStats();
	private boolean monitoring;

	private RpcServer(Eventloop eventloop) {
		super(eventloop);
		withServerSocketSettings(DEFAULT_SERVER_SOCKET_SETTINGS);
		withSocketSettings(DEFAULT_SOCKET_SETTINGS);
	}

	public static RpcServer create(Eventloop eventloop) {
		return new RpcServer(eventloop);
	}

	public RpcServer withMessageTypes(Class<?>... messageTypes) {
		return withMessageTypes(Arrays.asList(messageTypes));
	}

	public RpcServer withMessageTypes(List<Class<?>> messageTypes) {
		this.messageTypes.addAll(messageTypes);
		return this;
	}

	public RpcServer withSerializerBuilder(SerializerBuilder serializerBuilder) {
		this.serializerBuilder = serializerBuilder;
		return this;
	}

	public RpcServer withLogger(Logger logger) {
		this.logger = checkNotNull(logger);
		return this;
	}

	public RpcServer withProtocol(RpcProtocolFactory protocolFactory) {
		this.protocolFactory = protocolFactory;
		return this;
	}

	@SuppressWarnings("unchecked")
	public <I> RpcServer withHandlerFor(Class<I> requestClass, RpcRequestHandler<I, ?> handler) {
		handlers.put(requestClass, handler);
		return this;
	}

	private BufferSerializer<RpcMessage> getSerializer() {
		if (serializer == null) {
			SerializerBuilder serializerBuilder = this.serializerBuilder != null ?
					this.serializerBuilder :
					SerializerBuilder.newDefaultInstance(ClassLoader.getSystemClassLoader());
			serializerBuilder.setExtraSubclasses("extraRpcMessageData", messageTypes);
			serializer = serializerBuilder.create(RpcMessage.class);
		}
		return serializer;
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		BufferSerializer<RpcMessage> messageSerializer = getSerializer();
		RpcServerConnection connection = new RpcServerConnection(eventloop, this, asyncTcpSocket,
				messageSerializer, handlers, protocolFactory);
		add(connection);

		// jmx
		ensureConnectStats(asyncTcpSocket.getRemoteSocketAddress().getAddress()).recordEvent();
		totalConnects.recordEvent();
		connectionsCount.setCount(connections.size());

		return connection.getSocketConnection();
	}

	@Override
	protected void onClose() {
		for (final RpcServerConnection connection : new ArrayList<>(connections)) {
			connection.close();
		}
	}

	void add(RpcServerConnection connection) {
		if (logger.isInfoEnabled())
			logger.info("Client connected on {}", connection);

		if (monitoring) {
			connection.startMonitoring();
		}

		connections.add(connection);
	}

	void remove(RpcServerConnection connection) {
		if (logger.isInfoEnabled())
			logger.info("Client disconnected on {}", connection);
		connections.remove(connection);

		// jmx
		connectionsCount.setCount(connections.size());
	}

	// JMX
	@JmxOperation(description = "enable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, requestHandlingTime stats are collected only when monitoring is enabled) ]")
	public void startMonitoring() {
		monitoring = true;
		for (RpcServerConnection connection : connections) {
			connection.startMonitoring();
		}
	}

	@JmxOperation(description = "disable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, requestHandlingTime stats are collected only when monitoring is enabled) ]")
	public void stopMonitoring() {
		monitoring = false;
		for (RpcServerConnection connection : connections) {
			connection.stopMonitoring();
		}
	}

	@JmxAttribute(description = "when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, requestHandlingTime stats are collected only when monitoring is enabled)")
	public boolean isMonitoring() {
		return monitoring;
	}

	@JmxAttribute(description = "current number of connections")
	public CountStats getConnectionsCount() {
		return connectionsCount;
	}

	@JmxAttribute
	public EventStats getTotalConnects() {
		return totalConnects;
	}

	@JmxAttribute(description = "number of connects/reconnects per client address")
	public Map<InetAddress, EventStats> getConnectsPerAddress() {
		return connectsPerAddress;
	}

	private EventStats ensureConnectStats(InetAddress address) {
		EventStats stats = connectsPerAddress.get(address);
		if (stats == null) {
			stats = new EventStats();
			connectsPerAddress.put(address, stats);
		}
		return stats;
	}

	@JmxAttribute(description = "detailed information about connections")
	public List<RpcServerConnection> getConnections() {
		return connections;
	}

	@JmxAttribute(description = "number of requests which were processed correctly")
	public EventStats getSuccessfulRequests() {
		return successfulRequests;
	}

	@JmxAttribute(description = "request with error responses (number of requests which were handled with error)")
	public EventStats getFailedRequests() {
		return failedRequests;
	}

	@JmxAttribute(description = "time for handling one request in milliseconds (both successful and failed)")
	public ValueStats getRequestHandlingTime() {
		return requestHandlingTime;
	}

	@JmxAttribute(description = "exception that occurred because of business logic error " +
			"(in RpcRequestHandler implementation)")
	public ExceptionStats getLastRequestHandlingException() {
		return lastRequestHandlingException;
	}

	@JmxAttribute(description = "exception that occurred because of protocol error " +
			"(serialization, deserialization, compression, decompression, etc)")
	public ExceptionStats getLastProtocolError() {
		return lastProtocolError;
	}
}

