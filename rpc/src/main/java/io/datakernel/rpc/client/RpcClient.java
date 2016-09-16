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

package io.datakernel.rpc.client;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.eventloop.*;
import io.datakernel.jmx.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.client.jmx.RpcConnectStats;
import io.datakernel.rpc.client.jmx.RpcRequestStats;
import io.datakernel.rpc.client.sender.RpcNoSenderException;
import io.datakernel.rpc.client.sender.RpcSender;
import io.datakernel.rpc.client.sender.RpcStrategy;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocolFactory;
import io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.postCompletion;
import static io.datakernel.async.AsyncCallbacks.postException;
import static io.datakernel.eventloop.AsyncSslSocket.wrapClientSocket;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public final class RpcClient implements EventloopService, EventloopJmxMBean {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.create().withTcpNoDelay(true);
	public static final int DEFAULT_CONNECT_TIMEOUT = 10 * 1000;
	public static final int DEFAULT_RECONNECT_INTERVAL = 1 * 1000;

	private final Logger logger;

	private final Eventloop eventloop;
	private final SocketSettings socketSettings;

	// SSL
	private final SSLContext sslContext;
	private final ExecutorService sslExecutor;

	private final RpcStrategy strategy;
	private final List<InetSocketAddress> addresses;
	private final Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();
	private final RpcProtocolFactory protocolFactory;
	private final Set<Class<?>> messageTypes;
	private final int connectTimeoutMillis;
	private final int reconnectIntervalMillis;

	private final SerializerBuilder serializerBuilder;
	private BufferSerializer<RpcMessage> serializer;

	private RpcSender requestSender;

	private CompletionCallback startCallback;
	private CompletionCallback stopCallback;
	private boolean running;

	private final RpcClientConnectionPool pool = new RpcClientConnectionPool() {
		@Override
		public RpcClientConnection get(InetSocketAddress address) {
			return connections.get(address);
		}
	};

	// jmx
	private boolean monitoring = false;
	private final RpcRequestStats generalRequestsStats = RpcRequestStats.create();
	private final RpcConnectStats generalConnectsStats = RpcConnectStats.create();
	private final Map<Class<?>, RpcRequestStats> requestStatsPerClass = new HashMap<>();
	private final Map<InetSocketAddress, RpcConnectStats> connectsStatsPerAddress = new HashMap<>();
	private final ExceptionStats lastProtocolError = ExceptionStats.create();

	// region builders
	private RpcClient(Eventloop eventloop, SocketSettings socketSettings, Collection<Class<?>> messageTypes,
	                  SerializerBuilder serializerBuilder, RpcStrategy strategy, RpcProtocolFactory protocol,
	                  Logger logger, int connectTimeoutMillis, int reconnectIntervalMillis,
	                  SSLContext sslContext, ExecutorService executor) {
		this.eventloop = eventloop;
		this.socketSettings = socketSettings;
		this.messageTypes = new LinkedHashSet<>(messageTypes);
		this.serializerBuilder = serializerBuilder;
		this.protocolFactory = protocol;
		this.logger = logger;
		this.connectTimeoutMillis = connectTimeoutMillis;
		this.reconnectIntervalMillis = reconnectIntervalMillis;
		this.sslContext = sslContext;
		this.sslExecutor = executor;
		this.strategy = strategy;
		this.addresses = new ArrayList<>(strategy.getAddresses());

		// jmx
		for (InetSocketAddress address : this.addresses) {
			if (!connectsStatsPerAddress.containsKey(address)) {
				connectsStatsPerAddress.put(address, RpcConnectStats.create());
			}
		}
	}

	@SuppressWarnings("ConstantConditions")
	public static RpcClient create(final Eventloop eventloop) {
		checkNotNull(eventloop);

		List<Class<?>> defaultMessageTypes = emptyList();
		SerializerBuilder serializerBuilder = SerializerBuilder.newDefaultInstance(ClassLoader.getSystemClassLoader());
		RpcStrategy defaultStrategy = new NoServersStrategy();
		RpcStreamProtocolFactory defaultProtocol = streamProtocol();
		Logger defaultLogger = LoggerFactory.getLogger(RpcClient.class);
		SSLContext nullSslContext = null;
		ExecutorService nullSslExecutor = null;

		return new RpcClient(
				eventloop, DEFAULT_SOCKET_SETTINGS, defaultMessageTypes, serializerBuilder,
				defaultStrategy, defaultProtocol, defaultLogger,
				DEFAULT_CONNECT_TIMEOUT, DEFAULT_RECONNECT_INTERVAL,
				nullSslContext, nullSslExecutor);
	}

	public RpcClient withSocketSettings(SocketSettings socketSettings) {
		checkNotNull(socketSettings);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis,
				sslContext, sslExecutor);
	}

	public RpcClient withMessageTypes(Class<?>... messageTypes) {
		checkNotNull(messageTypes);
		return new RpcClient(
				eventloop, socketSettings, asList(messageTypes), serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis,
				sslContext, sslExecutor);
	}

	public RpcClient withMessageTypes(List<Class<?>> messageTypes) {
		checkNotNull(messageTypes);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis,
				sslContext, sslExecutor);
	}

	public RpcClient withSerializerBuilder(SerializerBuilder serializerBuilder) {
		checkNotNull(serializerBuilder);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis,
				sslContext, sslExecutor);
	}

	public RpcClient withStrategy(RpcStrategy requestSendingStrategy) {
		checkNotNull(requestSendingStrategy);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				requestSendingStrategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis,
				sslContext, sslExecutor);
	}

	public RpcClient withProtocol(RpcProtocolFactory protocolFactory) {
		checkNotNull(protocolFactory);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis,
				sslContext, sslExecutor);
	}

	public RpcClient withLogger(Logger logger) {
		checkNotNull(logger);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis,
				sslContext, sslExecutor);
	}

	public RpcClient withConnectTimeoutMillis(int connectTimeoutMillis) {
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis,
				sslContext, sslExecutor);
	}

	public RpcClient withReconnectIntervalMillis(int reconnectIntervalMillis) {
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis,
				sslContext, sslExecutor);
	}

	public RpcClient withSslEnabled(SSLContext sslContext, ExecutorService executor) {
		checkNotNull(sslContext);
		checkNotNull(executor);
		return new RpcClient(
				eventloop, socketSettings, messageTypes, serializerBuilder,
				strategy, protocolFactory, logger, connectTimeoutMillis, reconnectIntervalMillis,
				sslContext, executor);
	}
	// endregion

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		checkNotNull(callback);
		checkState(!running);
		running = true;
		startCallback = callback;
		if (connectTimeoutMillis != 0) {
			eventloop.scheduleBackground(eventloop.currentTimeMillis() + connectTimeoutMillis, new Runnable() {
				@Override
				public void run() {
					if (running && startCallback != null) {
						String errorMsg = String.format("Some of the required servers did not respond within %.1f sec",
								connectTimeoutMillis / 1000.0);
						postException(eventloop, startCallback, new InterruptedException(errorMsg));
						running = false;
						startCallback = null;
					}
				}
			});
		}

		for (InetSocketAddress address : addresses) {
			connect(address);
		}
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkNotNull(callback);
		checkState(eventloop.inEventloopThread());
		checkState(running);

		running = false;
		if (startCallback != null) {
			postException(eventloop, startCallback, new InterruptedException("Start aborted"));
			startCallback = null;
		}

		if (connections.size() == 0) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					callback.onComplete();
				}
			});
		} else {
			stopCallback = callback;
			for (RpcClientConnection connection : new ArrayList<>(connections.values())) {
				connection.close();
			}
		}
	}

	private BufferSerializer<RpcMessage> createSerializer() {
		serializerBuilder.addExtraSubclasses("extraRpcMessageData", messageTypes);
		return serializerBuilder.create(RpcMessage.class);
	}

	private void connect(final InetSocketAddress address) {
		if (!running) {
			return;
		}

		logger.info("Connecting {}", address);

		eventloop.connect(address, 0, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocketImpl = wrapChannel(eventloop, socketChannel, socketSettings);
				AsyncTcpSocket asyncTcpSocket = sslContext != null ? wrapClientSocket(eventloop, asyncTcpSocketImpl, sslContext, sslExecutor) : asyncTcpSocketImpl;
				RpcClientConnection connection = RpcClientConnection.create(eventloop, RpcClient.this,
						asyncTcpSocket, address,
						createSerializer(), protocolFactory);
				asyncTcpSocket.setEventHandler(connection.getSocketConnection());
				asyncTcpSocketImpl.register();

				addConnection(address, connection);

				// jmx
				generalConnectsStats.getSuccessfulConnects().recordEvent();
				connectsStatsPerAddress.get(address).getSuccessfulConnects().recordEvent();

				logger.info("Connection to {} established", address);
				if (startCallback != null) {
					postCompletion(eventloop, startCallback);
					startCallback = null;
				}
			}

			@Override
			public void onException(Exception e) {
				//jmx
				generalConnectsStats.getFailedConnects().recordEvent();
				connectsStatsPerAddress.get(address).getFailedConnects().recordEvent();

				if (running) {
					if (logger.isWarnEnabled()) {
						logger.warn("Connection failed, reconnecting to {}: {}", address, e.toString());
					}
					eventloop.scheduleBackground(eventloop.currentTimeMillis() + reconnectIntervalMillis, new Runnable() {
						@Override
						public void run() {
							if (running) {
								connect(address);
							}
						}
					});
				}
			}
		});
	}

	private void addConnection(InetSocketAddress address, RpcClientConnection connection) {
		connections.put(address, connection);

		// jmx
		if (isMonitoring()) {
			connection.startMonitoring();
		}
		RpcSender sender = strategy.createSender(pool);
		requestSender = sender != null ? sender : new Sender();
	}

	public void removeConnection(final InetSocketAddress address) {
		logger.info("Connection to {} closed", address);

		connections.remove(address);

		if (stopCallback != null && connections.size() == 0) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					stopCallback.onComplete();
					stopCallback = null;
				}
			});
		}

		RpcSender sender = strategy.createSender(pool);
		requestSender = sender != null ? sender : new Sender();

		// jmx
		generalConnectsStats.getClosedConnects().recordEvent();
		connectsStatsPerAddress.get(address).getClosedConnects().recordEvent();

		eventloop.scheduleBackground(eventloop.currentTimeMillis() + reconnectIntervalMillis, new Runnable() {
			@Override
			public void run() {
				if (running) {
					connect(address);
				}
			}
		});
	}

	public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
		ResultCallback<T> requestCallback = callback;
		requestSender.sendRequest(request, timeout, requestCallback);

	}

	public <T> ResultCallbackFuture<T> sendRequestFuture(final Object request, final int timeout) {
		final ResultCallbackFuture<T> future = ResultCallbackFuture.create();
		eventloop.execute(new Runnable() {
			@Override
			public void run() {
				sendRequest(request, timeout, future);
			}
		});
		return future;
	}

	// visible for testing
	public RpcSender getRequestSender() {
		return requestSender;
	}

	static final class Sender implements RpcSender {
		@SuppressWarnings("ThrowableInstanceNeverThrown")
		private static final RpcNoSenderException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderException("No senders available");

		@Override
		public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
			callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
		}
	}

	private static final class NoServersStrategy implements RpcStrategy {

		@Override
		public Set<InetSocketAddress> getAddresses() {
			return Collections.emptySet();
		}

		@Override
		public RpcSender createSender(RpcClientConnectionPool pool) {
			return new Sender();
		}
	}

	// jmx
	@JmxOperation(description = "enable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, responseTime and requestsStatsPerClass are collected only when monitoring is enabled) ]")
	public void startMonitoring() {
		monitoring = true;
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = connections.get(address);
			if (connection != null) {
				connection.startMonitoring();
			}
		}
	}

	@JmxOperation(description = "disable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, responseTime and requestsStatsPerClass are collected only when monitoring is enabled) ]")
	public void stopMonitoring() {
		monitoring = false;
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = connections.get(address);
			if (connection != null) {
				connection.stopMonitoring();
			}
		}
	}

	@JmxAttribute(description = "when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, responseTime and requestsStatsPerClass are collected only when monitoring is enabled)")
	private boolean isMonitoring() {
		return monitoring;
	}

	@JmxOperation
	public void resetStats() {
		generalRequestsStats.resetStats();
		for (InetSocketAddress address : connectsStatsPerAddress.keySet()) {
			connectsStatsPerAddress.get(address).reset();
		}
		for (Class<?> requestClass : requestStatsPerClass.keySet()) {
			requestStatsPerClass.get(requestClass).resetStats();
		}

		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = connections.get(address);
			if (connection != null) {
				connection.resetStats();
			}
		}
	}

	@JmxAttribute(name = "requests")
	public RpcRequestStats getGeneralRequestsStats() {
		return generalRequestsStats;
	}

	@JmxAttribute(name = "connects")
	public RpcConnectStats getGeneralConnectsStats() {
		return generalConnectsStats;
	}

	@JmxAttribute(description = "request stats distributed by request class")
	public Map<Class<?>, RpcRequestStats> getRequestsStatsPerClass() {
		return requestStatsPerClass;
	}

	@JmxAttribute
	public Map<InetSocketAddress, RpcConnectStats> getConnectsStatsPerAddress() {
		return connectsStatsPerAddress;
	}

	@JmxAttribute(description = "request stats for current connections (when connection is closed stats are removed)")
	public Map<InetSocketAddress, RpcClientConnection> getRequestStatsPerConnection() {
		return connections;
	}

	@JmxAttribute(name = "activeConnections")
	public CountStats getActiveConnectionsCount() {
		CountStats countStats = CountStats.create();
		countStats.setCount(connections.size());
		return countStats;
	}

	@JmxAttribute(description = "exception that occurred because of protocol error " +
			"(serialization, deserialization, compression, decompression, etc)")
	public ExceptionStats getLastProtocolError() {
		return lastProtocolError;
	}

	RpcRequestStats ensureRequestStatsPerClass(Class<?> requestClass) {
		if (!requestStatsPerClass.containsKey(requestClass)) {
			requestStatsPerClass.put(requestClass, RpcRequestStats.create());
		}
		return requestStatsPerClass.get(requestClass);
	}
}
