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

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.InetAddressRange;
import io.datakernel.jmx.*;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.*;
import static java.lang.ClassLoader.getSystemClassLoader;
import static java.util.Arrays.asList;

/**
 * An RPC server that works asynchronously. This server uses fast serializers
 * and custom optimized communication protocol, which improves application
 * performance.
 * <p>
 * In order to set up a server it's mandatory to create it using
 * {@link #create(Eventloop)}, indicate a types of messages, and specify
 * an appropriate {@link RpcRequestHandler request handlers} for that types.
 * <p>
 * {@link RpcRequestHandler} is responsible for processing requests according to business
 * logic and passing result to callback.
 * <p>
 * There are two ways of starting a server:
 * <ul>
 *     <li>
 *         Manually: set up the server and call {@code listen()}
 *     </li>
 *     <li>
 *         Using {@code Launcher} and {@code ServiceGraph}: create a module for
 *         your RPC server and pass it to a launcher along with
 *         {@code ServiceGraph}.
 *     </li>
 * </ul>
 * The usage of second approach simplifies the development significantly,
 * especially when a number of modules growth.
 * <p>
 * An RPC server has a lot of builder methods which adjusts a server.
 * //TODO 21.11.2016 add server example, which works with RpcClient example
 */
public final class RpcServer extends AbstractServer<RpcServer> {
	private final Logger logger;
	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS
			= ServerSocketSettings.create(16384);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.create().withTcpNoDelay(true);

	private final Map<Class<?>, RpcRequestHandler<?, ?>> handlers;
	private final RpcProtocolFactory protocolFactory;
	private final SerializerBuilder serializerBuilder;
	private final List<Class<?>> messageTypes;

	private final List<RpcServerConnection> connections = new ArrayList<>();

	private BufferSerializer<RpcMessage> serializer;

	private CompletionCallback closeCallback;

	// jmx
	private CountStats connectionsCount = CountStats.create();
	private EventStats totalConnects = EventStats.create();
	private Map<InetAddress, EventStats> connectsPerAddress = new HashMap<>();
	private EventStats successfulRequests = EventStats.create();
	private EventStats failedRequests = EventStats.create();
	private ValueStats requestHandlingTime = ValueStats.create();
	private ExceptionStats lastRequestHandlingException = ExceptionStats.create();
	private ExceptionStats lastProtocolError = ExceptionStats.create();
	private boolean monitoring;

	// region builders
	private RpcServer(Eventloop eventloop, Map<Class<?>, RpcRequestHandler<?, ?>> handlers,
	                  RpcProtocolFactory protocolFactory, SerializerBuilder serializerBuilder,
	                  List<Class<?>> messageTypes, Logger logger) {
		super(eventloop);
		this.handlers = handlers;
		this.protocolFactory = protocolFactory;
		this.serializerBuilder = serializerBuilder;
		this.messageTypes = messageTypes;
		this.logger = logger;
	}

	private RpcServer(Eventloop eventloop,
	                  ServerSocketSettings serverSocketSettings, SocketSettings socketSettings,
	                  boolean acceptOnce, Collection<InetSocketAddress> listenAddresses,
	                  InetAddressRange range, Collection<InetAddress> bannedAddresses,
	                  SSLContext sslContext, ExecutorService sslExecutor,
	                  Collection<InetSocketAddress> sslListenAddresses,
	                  RpcServer previousInstance) {
		super(eventloop, serverSocketSettings, socketSettings, acceptOnce, listenAddresses,
				range, bannedAddresses, sslContext, sslExecutor, sslListenAddresses);
		this.handlers = previousInstance.handlers;
		this.protocolFactory = previousInstance.protocolFactory;
		this.serializerBuilder = previousInstance.serializerBuilder;
		this.messageTypes = previousInstance.messageTypes;
		this.logger = previousInstance.logger;
	}

	private RpcServer(RpcServer previousInstance,
	                  Map<Class<?>, RpcRequestHandler<?, ?>> handlers,
	                  RpcProtocolFactory protocolFactory, SerializerBuilder serializerBuilder,
	                  List<Class<?>> messageTypes, Logger logger) {
		super(previousInstance);
		this.handlers = handlers;
		this.protocolFactory = protocolFactory;
		this.serializerBuilder = serializerBuilder;
		this.messageTypes = messageTypes;
		this.logger = logger;
	}

	public static RpcServer create(Eventloop eventloop) {
		HashMap<Class<?>, RpcRequestHandler<?, ?>> requestHandlers = new HashMap<>();
		RpcProtocolFactory protocolFactory = streamProtocol();
		SerializerBuilder serializerBuilder = SerializerBuilder.create(getSystemClassLoader());
		List<Class<?>> messageTypes = null;
		Logger logger = LoggerFactory.getLogger(RpcServer.class);

		return new RpcServer(eventloop, requestHandlers, protocolFactory, serializerBuilder, messageTypes, logger)
				.withServerSocketSettings(DEFAULT_SERVER_SOCKET_SETTINGS)
				.withSocketSettings(DEFAULT_SOCKET_SETTINGS);
	}

	/**
	 * Creates a server with capability of specified message types processing.
	 *
	 * @param messageTypes classes of messages processed by a server
	 * @return server instance of
	 */
	public RpcServer withMessageTypes(Class<?>... messageTypes) {
		checkNotNull(messageTypes);
		return withMessageTypes(asList(messageTypes));
	}

	/**
	 * Creates a server with capability of specified message types processing.
	 *
	 * @param messageTypes a list of message types processed by a server
	 * @return server instance of
	 */
	public RpcServer withMessageTypes(List<Class<?>> messageTypes) {
		checkNotNull(messageTypes);
		checkArgument(new HashSet<>(messageTypes).size() == messageTypes.size(), "Message types must be unique");
		return new RpcServer(this, handlers, protocolFactory, serializerBuilder, messageTypes, logger);
	}

	public RpcServer withSerializerBuilder(SerializerBuilder serializerBuilder) {
		checkNotNull(serializerBuilder);
		return new RpcServer(this, handlers, protocolFactory, serializerBuilder, messageTypes, logger);
	}

	public RpcServer withLogger(Logger logger) {
		checkNotNull(logger);
		return new RpcServer(this, handlers, protocolFactory, serializerBuilder, messageTypes, logger);
	}

	public RpcServer withProtocol(RpcProtocolFactory protocolFactory) {
		checkNotNull(protocolFactory);
		return new RpcServer(this, handlers, protocolFactory, serializerBuilder, messageTypes, logger);
	}

	public RpcServer withStreamProtocol(int defaultPacketSize, int maxPacketSize, boolean compression) {
		return withProtocol(streamProtocol(defaultPacketSize, maxPacketSize, compression));
	}

	public RpcServer withStreamProtocol(MemSize defaultPacketSize, MemSize maxPacketSize, boolean compression) {
		return withProtocol(streamProtocol(defaultPacketSize, maxPacketSize, compression));
	}

	/**
	 * Adds handler for a specified request-response pair.
	 *
	 * @param requestClass
	 * @param responseClass
	 * @param handler
	 * @param <I> class of request
	 * @param <O> class of response
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <I, O> RpcServer withHandler(Class<I> requestClass, Class<O> responseClass, RpcRequestHandler<I, O> handler) {
		handlers.put(requestClass, handler);
		return this;
	}

	@Override
	protected RpcServer recreate(Eventloop eventloop, ServerSocketSettings serverSocketSettings, SocketSettings socketSettings,
	                             boolean acceptOnce,
	                             Collection<InetSocketAddress> listenAddresses,
	                             InetAddressRange range, Collection<InetAddress> bannedAddresses,
	                             SSLContext sslContext, ExecutorService sslExecutor,
	                             Collection<InetSocketAddress> sslListenAddresses) {
		return new RpcServer(eventloop, serverSocketSettings, socketSettings, acceptOnce, listenAddresses,
				range, bannedAddresses, sslContext, sslExecutor, sslListenAddresses, this);
	}
	// endregion

	private BufferSerializer<RpcMessage> getSerializer() {
		checkState(messageTypes != null, "Message types must be specified");
		if (serializer == null) {
			serializer = serializerBuilder.withSubclasses(RpcMessage.MESSAGE_TYPES, messageTypes).build(RpcMessage.class);
		}
		return serializer;
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		BufferSerializer<RpcMessage> messageSerializer = getSerializer();
		RpcServerConnection connection = RpcServerConnection.create(eventloop, this, asyncTcpSocket,
				messageSerializer, handlers, protocolFactory);
		add(connection);

		// jmx
		ensureConnectStats(asyncTcpSocket.getRemoteSocketAddress().getAddress()).recordEvent();
		totalConnects.recordEvent();
		connectionsCount.setCount(connections.size());

		return connection.getSocketConnection();
	}

	@Override
	protected void onClose(final CompletionCallback completionCallback) {
		if (connections.size() == 0) {
			logger.info("RpcServer is closing. Active connections count: 0.");
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					completionCallback.setComplete();
				}
			});
		} else {
			logger.info("RpcServer is closing. Active connections count: " + connections.size());
			for (final RpcServerConnection connection : new ArrayList<>(connections)) {
				connection.close();
			}
			closeCallback = completionCallback;
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

		if (closeCallback != null) {
			logger.info("RpcServer is closing. One more connection was closed. " +
					"Active connections count: " + connections.size());

			if (connections.size() == 0) {
				closeCallback.setComplete();
			}
		}

		// jmx
		connectionsCount.setCount(connections.size());
	}

	// region JMX
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
			stats = EventStats.create();
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
	// endregion
}

