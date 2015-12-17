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
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
import io.datakernel.net.SocketSettings;
import io.datakernel.rpc.client.RpcClientConnection.StatusListener;
import io.datakernel.rpc.client.jmx.RpcJmxClientConncetion;
import io.datakernel.rpc.client.jmx.RpcJmxClient;
import io.datakernel.rpc.client.jmx.RpcJmxStatsManager;
import io.datakernel.rpc.client.sender.RpcNoSenderException;
import io.datakernel.rpc.client.sender.RpcSender;
import io.datakernel.rpc.client.sender.RpcStrategy;
import io.datakernel.rpc.protocol.*;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.datakernel.async.AsyncCallbacks.postCompletion;
import static io.datakernel.async.AsyncCallbacks.postException;
import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

public final class RpcClient implements NioService, RpcJmxClient {
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = new SocketSettings().tcpNoDelay(true);
	public static final int DEFAULT_CONNECT_TIMEOUT = 10 * 1000;
	public static final int DEFAULT_RECONNECT_INTERVAL = 1 * 1000;

	private Logger logger = LoggerFactory.getLogger(RpcClient.class);

	private final NioEventloop eventloop;
	private RpcStrategy strategy;
	private List<InetSocketAddress> addresses;
	private final Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();
	private RpcProtocolFactory protocolFactory = streamProtocol();
	private final RpcSerializer serializer;
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;
	private int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT;
	private int reconnectIntervalMillis = DEFAULT_RECONNECT_INTERVAL;

	private RpcSender requestSender;

	private CompletionCallback startCallback;
	private boolean running;

	// JMX
	private RpcJmxStatsManager jmxStatsManager;

	private final RpcClientConnectionPool pool = new RpcClientConnectionPool() {
		@Override
		public RpcClientConnection get(InetSocketAddress key) {
			return connections.get(key);
		}
	};

	private RpcClient(NioEventloop eventloop, RpcSerializer serializer) {
		this.eventloop = eventloop;
		this.serializer = serializer;
	}

	public static RpcClient create(final NioEventloop eventloop, final RpcSerializer serializerFactory) {
		return new RpcClient(eventloop, serializerFactory);
	}

	public RpcClient strategy(RpcStrategy requestSendingStrategy) {
		this.strategy = requestSendingStrategy;
		this.addresses = new ArrayList<>(requestSendingStrategy.getAddresses());
		return this;
	}

	public RpcClient protocol(RpcProtocolFactory protocolFactory) {
		this.protocolFactory = protocolFactory;
		return this;
	}

	public RpcClient socketSettings(SocketSettings socketSettings) {
		this.socketSettings = checkNotNull(socketSettings);
		return this;
	}

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}

	public RpcClient logger(Logger logger) {
		this.logger = checkNotNull(logger);
		return this;
	}

	public RpcClient connectTimeoutMillis(int connectTimeoutMillis) {
		this.connectTimeoutMillis = connectTimeoutMillis;
		return this;
	}

	public RpcClient reconnectIntervalMillis(int reconnectIntervalMillis) {
		this.reconnectIntervalMillis = reconnectIntervalMillis;
		return this;
	}

	@Override
	public NioEventloop getNioEventloop() {
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

	public void stop() {
		checkState(eventloop.inEventloopThread());
		checkState(running);
		running = false;
		if (startCallback != null) {
			postException(eventloop, startCallback, new InterruptedException("Start aborted"));
			startCallback = null;
		}
		closeConnections();
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkNotNull(callback);
		stop();
		callback.onComplete();
	}

	private void connect(final InetSocketAddress address) {
		if (!running) {
			return;
		}

		logger.info("Connecting {}", address);
		eventloop.connect(address, socketSettings, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				StatusListener statusListener = new StatusListener() {
					@Override
					public void onOpen(RpcClientConnection connection) {
						addConnection(address, connection);
					}

					@Override
					public void onClosed() {
						logger.info("Connection to {} closed", address);
						removeConnection(address);
						if (isMonitoring()) {
							jmxStatsManager.recordClosedConnect(address);
						}
						connect(address);
					}
				};
				RpcClientConnection connection = new RpcImplClientConncetion(eventloop, socketChannel,
						serializer.createSerializer(), protocolFactory, statusListener);
				connection.getSocketConnection().register();
				if (isMonitoring()) {
					jmxStatsManager.recordSuccessfulConnect(address);
				}
				logger.info("Connection to {} established", address);
				if (startCallback != null) {
					postCompletion(eventloop, startCallback);
					startCallback = null;
				}
			}

			@Override
			public void onException(Exception exception) {
				if (isMonitoring()) {
					jmxStatsManager.recordFailedConnect(address);
				}
				if (running) {
					if (logger.isWarnEnabled()) {
						logger.warn("Connection failed, reconnecting to {}: {}", address, exception.toString());
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
		if (isMonitoring()) {
			if (connection instanceof RpcJmxClientConncetion)
				((RpcJmxClientConncetion) connection).startMonitoring(jmxStatsManager.getAddressStatsManager(address));
		}
		RpcSender sender = strategy.createSender(pool);
		requestSender = sender != null ? sender : new Sender();
	}

	private void removeConnection(InetSocketAddress address) {
		connections.remove(address);
		RpcSender sender = strategy.createSender(pool);
		requestSender = sender != null ? sender : new Sender();
	}

	private void closeConnections() {
		for (RpcClientConnection connection : new ArrayList<>(connections.values())) {
			connection.close();
		}
	}

	public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
		ResultCallback<T> requestCallback = callback;
		if (isMonitoring()) {
			jmxStatsManager.recordNewRequest(request.getClass());
			requestCallback = new JmxMonitoringResultCallback<>(request.getClass(), callback);
		}
		requestSender.sendRequest(request, timeout, requestCallback);

	}

	public <T> ResultCallbackFuture<T> sendRequestFuture(final Object request, final int timeout) {
		final ResultCallbackFuture<T> future = new ResultCallbackFuture<>();
		eventloop.postConcurrently(new Runnable() {
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

	/**
	 * Thread-safe operation
	 */
	@Override
	public void startMonitoring(final RpcJmxStatsManager jmxStatsManager) {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				RpcClient.this.jmxStatsManager = jmxStatsManager;
				for (InetSocketAddress address : addresses) {
					RpcJmxClientConncetion connection = (RpcJmxClientConncetion) pool.get(address);
					connection.startMonitoring(jmxStatsManager.getAddressStatsManager(address));
				}
			}
		});
	}

	/**
	 * Thread-safe operation
	 */
	@Override
	public void stopMonitoring() {
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				RpcClient.this.jmxStatsManager = null;
				for (InetSocketAddress address : addresses) {
					RpcJmxClientConncetion connection = (RpcJmxClientConncetion) pool.get(address);
					connection.stopMonitoring();
				}
			}
		});
	}

	private boolean isMonitoring() {
		return jmxStatsManager != null;
	}

	private final class JmxMonitoringResultCallback<T> implements ResultCallback<T> {

		private Stopwatch stopwatch;
		private final Class<?> requestClass;
		private final ResultCallback<T> callback;

		public JmxMonitoringResultCallback(Class<?> requestClass, ResultCallback<T> callback) {
			this.stopwatch = Stopwatch.createStarted();
			this.requestClass = requestClass;
			this.callback = callback;
		}

		@Override
		public void onResult(T result) {
			if (isMonitoring()) {
				jmxStatsManager.recordSuccessfulRequest(requestClass, timeElapsed());
			}
			callback.onResult(result);
		}

		@Override
		public void onException(Exception exception) {
			if (isMonitoring()) {
				if (exception instanceof RpcTimeoutException) {
					jmxStatsManager.recordExpiredRequest(requestClass);
				} else if (exception instanceof RpcOverloadException) {
					jmxStatsManager.recordRejectedRequest(requestClass);
				} else if (exception instanceof RpcRemoteException) {
					// TODO(vmykhalko): maybe there should be something more informative instead of null (as causedObject)?
					jmxStatsManager.recordFailedRequest(requestClass, exception, null, timeElapsed());
				}
			}
			callback.onException(exception);
		}

		private int timeElapsed() {
			return (int)(stopwatch.elapsed(TimeUnit.MILLISECONDS));
		}
	}
}
