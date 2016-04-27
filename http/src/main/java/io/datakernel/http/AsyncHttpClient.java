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

package io.datakernel.http;

import io.datakernel.async.*;
import io.datakernel.dns.DnsClient;
import io.datakernel.dns.DnsException;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.http.ExposedLinkedList.Node;
import io.datakernel.jmx.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.Stopwatch;
import io.datakernel.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.datakernel.http.AbstractHttpConnection.MAX_HEADER_LINE_SIZE;
import static io.datakernel.net.SocketSettings.defaultSocketSettings;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Arrays.asList;

@SuppressWarnings("ThrowableInstanceNeverThrown, unused")
public class AsyncHttpClient implements EventloopService, EventloopJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(AsyncHttpClient.class);

	private static final long CHECK_PERIOD = 1000L;
	private static final long MAX_IDLE_CONNECTION_TIME = 30 * 1000L;

	private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();
	private static final BindException BIND_EXCEPTION = new BindException();
	private static final IOException MAX_CONNECTIONS_EXCEPTION = new IOException("Maximum connections per ip reached");

	private final Eventloop eventloop;
	private final DnsClient dnsClient;
	private final SocketSettings socketSettings;
	private final char[] headerChars;

	protected final ExposedLinkedList<AbstractHttpConnection> connectionsList; // active + cached
	private final HashMap<InetSocketAddress, ExposedLinkedList<HttpClientConnection>> cachedIpConnectionLists = new HashMap<>(); // cached per ip

	private final Runnable expiredConnectionsTask = createExpiredConnectionsTask();
	private AsyncCancellable scheduleExpiredConnectionCheck;

	private boolean blockLocalAddresses = false;
	private long bindExceptionBlockTimeout = 24 * 60 * 60 * 1000L;
	private int maxHttpMessageSize = Integer.MAX_VALUE;

	private boolean running;

	private static class AddressConnectsMonitor {
		static class AddressConnects {
			private int pending;
			private int active;
			private int cached;

			int total() {
				return pending + active + cached;
			}

			void addPending() {
				pending++;
			}

			void removePending() {
				pending--;
			}

			void pending2active() {
				pending--;
				active++;
			}

			void active2cached() {
				active--;
				cached++;
			}

			void cached2active() {
				cached--;
				active++;
			}

			void removeActive() {
				active--;
			}

			void removeCached() {
				cached--;
			}

			@Override
			public String toString() {
				return "active: " + active
						+ ", cached: " + cached
						+ ", pending: " + pending
						+ ", total: " + total();
			}
		}

		private int maxConnectionsPerIp = Integer.MAX_VALUE;

		private HashMap<InetSocketAddress, AddressConnects> addressConnects = new HashMap<>();

		void addPending(InetSocketAddress address) {
			AddressConnects connects = addressConnects.get(address);
			if (connects == null) {
				connects = new AddressConnects();
				addressConnects.put(address, connects);
			}
			connects.addPending();
		}

		void removePending(InetSocketAddress address) {
			AddressConnects connects = addressConnects.get(address);
			connects.removePending();
			if (connects.total() == 0) {
				addressConnects.remove(address);
			}
		}

		void pending2active(InetSocketAddress address) {
			addressConnects.get(address).pending2active();
		}

		void active2cached(InetSocketAddress address) {
			addressConnects.get(address).active2cached();
		}

		void cached2active(InetSocketAddress address) {
			addressConnects.get(address).cached2active();
		}

		void removeActive(InetSocketAddress address) {
			AddressConnects connects = this.addressConnects.get(address);
			connects.removeActive();
			if (connects.total() == 0) {
				addressConnects.remove(address);
			}
		}

		void removeCached(InetSocketAddress address) {
			AddressConnects connects = this.addressConnects.get(address);
			connects.removeCached();
			if (connects.total() == 0) {
				addressConnects.remove(address);
			}
		}

		boolean isConnectionAllowed(InetSocketAddress address) {
			AddressConnects connects = addressConnects.get(address);
			return connects == null || connects.total() < maxConnectionsPerIp;
		}

		@Override
		public String toString() {
			return addressConnects.toString();
		}
	}

	private AddressConnectsMonitor addressConnectsMonitor = new AddressConnectsMonitor();
	private final Map<InetSocketAddress, Long> bindExceptionBlockedHosts = new HashMap<>();

	// JMX
	private final ValueStats timeCheckExpired = new ValueStats();
	private final ValueStats expiredConnections = new ValueStats();
	private final EventStats totalRequests = new EventStats();
	private final EventStats blockedRequests = new EventStats();
	private final ExceptionStats dnsErrors = new ExceptionStats();
	private CountStats pendingSocketConnect = new CountStats();
	private boolean jmxMonitoring;

	private int inetAddressIdx = 0;

	// creators & builder methods
	public AsyncHttpClient(Eventloop eventloop, DnsClient dnsClient) {
		this(eventloop, dnsClient, defaultSocketSettings());
	}

	public AsyncHttpClient(Eventloop eventloop, DnsClient dnsClient, SocketSettings socketSettings) {
		this.eventloop = eventloop;
		this.dnsClient = dnsClient;
		this.socketSettings = checkNotNull(socketSettings);
		this.connectionsList = new ExposedLinkedList<>();
		char[] chars = eventloop.get(char[].class);
		if (chars == null || chars.length < MAX_HEADER_LINE_SIZE) {
			chars = new char[MAX_HEADER_LINE_SIZE];
			eventloop.set(char[].class, chars);
		}
		this.headerChars = chars;
	}

	public AsyncHttpClient setBindExceptionBlockTimeout(long bindExceptionBlockTimeout) {
		this.bindExceptionBlockTimeout = bindExceptionBlockTimeout;
		return this;
	}

	public AsyncHttpClient setBlockLocalAddresses(boolean blockLocalAddresses) {
		this.blockLocalAddresses = blockLocalAddresses;
		return this;
	}

	public AsyncHttpClient setMaxHttpMessageSize(int size) {
		this.maxHttpMessageSize = size;
		return this;
	}

	public AsyncHttpClient setMaxConnectionsPerIp(int maxConnectionsPerIp) {
		this.addressConnectsMonitor.maxConnectionsPerIp = maxConnectionsPerIp;
		return this;
	}

	// eventloop service
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(final CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		checkState(!running);
		running = true;
		callback.onComplete();
	}

	@Override
	public void stop(final CompletionCallback callback) {
		checkState(eventloop.inEventloopThread());
		if (running) {
			running = false;
			close();
		}
		callback.onComplete();
	}

	public void close() {
		checkState(eventloop.inEventloopThread());
		if (scheduleExpiredConnectionCheck != null)
			scheduleExpiredConnectionCheck.cancel();

		ExposedLinkedList.Node<AbstractHttpConnection> node = connectionsList.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert connection.getEventloop().inEventloopThread();
			connection.close();
		}
	}

	public CompletionCallbackFuture closeFuture() {
		return AsyncCallbacks.stopFuture(this);
	}

	// core
	public void execute(HttpRequest request, int timeout, ResultCallback<HttpResponse> callback) {
		checkNotNull(request);
		assert eventloop.inEventloopThread();
		totalRequests.recordEvent();
		getUrlAsync(request, timeout, callback);
	}

	private void getUrlAsync(final HttpRequest request, final int timeout, final ResultCallback<HttpResponse> callback) {
		dnsClient.resolve4(request.getUrl().getHost(), new ResultCallback<InetAddress[]>() {
			@Override
			public void onResult(InetAddress[] inetAddresses) {
				getUrlForHostAsync(request, timeout, inetAddresses, callback);
			}

			@Override
			public void onException(Exception e) {
				if (e.getClass() == DnsException.class || e.getClass() == TimeoutException.class) {
					if (logger.isWarnEnabled()) {
						logger.warn("DNS exception for '{}': {}", request, e.getMessage());
					}
				} else {
					if (logger.isErrorEnabled()) {
						logger.error("Unexpected DNS exception for " + request, e);
					}
				}
				dnsErrors.recordException(e, request);
				callback.onException(e);
			}
		});
	}

	private void getUrlForHostAsync(final HttpRequest request, int timeout, final InetAddress[] inetAddresses, final ResultCallback<HttpResponse> callback) {
		String host = request.getUrl().getHost();
		InetAddress inetAddress = getNextInetAddress(inetAddresses);
		if (!isValidHost(host, inetAddress, blockLocalAddresses)) {
			callback.onException(new IOException("Invalid IP address " + inetAddress + " for host " + host));
			return;
		}
		final InetSocketAddress address = new InetSocketAddress(inetAddress, request.getUrl().getPort());

		if (isBindExceptionBlocked(address)) {
			callback.onException(BIND_EXCEPTION);
			return;
		}

		final long timeoutTime = eventloop.currentTimeMillis() + timeout;

		HttpClientConnection connection = getFreeConnection(address);
		if (connection != null) {
			addressConnectsMonitor.cached2active(address);
			sendRequest(connection, request, timeoutTime, callback);
			return;
		}

		if (addressConnectsMonitor.isConnectionAllowed(address)) {
			pendingSocketConnect.increment();
			addressConnectsMonitor.addPending(address);
			eventloop.connect(address, socketSettings, timeout, new ConnectCallback() {
				@Override
				public void onConnect(SocketChannel socketChannel) {
					pendingSocketConnect.decrement();
					addressConnectsMonitor.pending2active(address);
					HttpClientConnection connection = createConnection(socketChannel);
					connection.register();
					if (timeoutTime <= eventloop.currentTimeMillis()) {
						// timeout for this request, reuse for other requests
						addToIpPool(connection);
						callback.onException(TIMEOUT_EXCEPTION);
						return;
					}
					sendRequest(connection, request, timeoutTime, callback);
				}

				@Override
				public void onException(Exception e) {
					pendingSocketConnect.decrement();
					addressConnectsMonitor.removePending(address);
					if (e instanceof BindException) {
						if (bindExceptionBlockTimeout != 0) {
							bindExceptionBlockedHosts.put(address, eventloop.currentTimeMillis());
						}
					}
					if (logger.isWarnEnabled()) {
						logger.warn("Connect error to {} : {}", address, e.getMessage());
					}
					callback.onException(e);
				}

				@Override
				public String toString() {
					return address.toString();
				}
			});
		} else {
			blockedRequests.recordEvent();
			callback.onException(MAX_CONNECTIONS_EXCEPTION);
		}
	}

	private void sendRequest(HttpClientConnection connection, HttpRequest request, long timeoutTime, ResultCallback<HttpResponse> callback) {
		connectionsList.moveNodeToLast(connection.connectionsListNode); // back-order connections
		connection.request(request, timeoutTime, callback);
	}

	// expired check methods
	private Runnable createExpiredConnectionsTask() {
		return new Runnable() {
			@Override
			public void run() {
				checkExpiredConnections();
				if (!connectionsList.isEmpty())
					scheduleCheck();
			}
		};
	}

	private void scheduleCheck() {
		scheduleExpiredConnectionCheck = eventloop.schedule(eventloop.currentTimeMillis() + CHECK_PERIOD, expiredConnectionsTask);
	}

	private int checkExpiredConnections() {
		scheduleExpiredConnectionCheck = null;
		Stopwatch stopwatch = (jmxMonitoring) ? Stopwatch.createStarted() : null;
		int count = 0;
		try {
			final long now = eventloop.currentTimeMillis();

			ExposedLinkedList.Node<AbstractHttpConnection> node = connectionsList.getFirstNode();
			while (node != null) {
				AbstractHttpConnection connection = node.getValue();
				node = node.getNext();

				assert connection.getEventloop().inEventloopThread();
				long idleTime = now - connection.getActivityTime();
				if (idleTime < MAX_IDLE_CONNECTION_TIME)
					break; // connections must back ordered by activity
				connection.close();
				count++;
			}
			expiredConnections.recordValue(count);
		} finally {
			if (stopwatch != null)
				timeCheckExpired.recordValue((int) stopwatch.elapsed(TimeUnit.MICROSECONDS));
		}
		return count;
	}

	// provide connections
	private HttpClientConnection createConnection(SocketChannel socketChannel) {
		HttpClientConnection connection = new HttpClientConnection(eventloop, socketChannel, this, headerChars, maxHttpMessageSize);
		if (connectionsList.isEmpty()) {
			scheduleCheck();
		}
		return connection;
	}

	private HttpClientConnection getFreeConnection(InetSocketAddress address) {
		ExposedLinkedList<HttpClientConnection> list = cachedIpConnectionLists.get(address);
		if (list == null) {
			return null;
		}
		for (; ; ) {
			HttpClientConnection connection = list.removeFirstValue();
			if (connection == null) {
				break;
			}
			if (connection.isRegistered()) {
				connection.ipConnectionListNode = null;
				return connection;
			}
		}
		return null;
	}

	protected void addToIpPool(HttpClientConnection connection) {
		assert connection.isRegistered();
		assert connection.ipConnectionListNode == null;

		InetSocketAddress address = connection.getRemoteSocketAddress();
		if (address == null) {
			connection.close();
			return;
		}

		addressConnectsMonitor.active2cached(address);

		ExposedLinkedList<HttpClientConnection> list = cachedIpConnectionLists.get(address);
		if (list == null) {
			list = new ExposedLinkedList<>();
			cachedIpConnectionLists.put(address, list);
		}
		connection.ipConnectionListNode = list.addLastValue(connection);
	}

	protected void removeFromIpPool(HttpClientConnection connection) {
		InetSocketAddress address = connection.getRemoteSocketAddress();

		if (connection.ipConnectionListNode == null) {
			addressConnectsMonitor.removeActive(address);
			return;
		}

		ExposedLinkedList<HttpClientConnection> list = cachedIpConnectionLists.get(address);
		if (list != null) {
			addressConnectsMonitor.removeCached(address);
			list.removeNode(connection.ipConnectionListNode);
			connection.ipConnectionListNode = null;
			if (list.isEmpty()) {
				cachedIpConnectionLists.remove(address);
			}
		}
	}

	private InetAddress getNextInetAddress(InetAddress[] inetAddresses) {
		return inetAddresses[((inetAddressIdx++) & Integer.MAX_VALUE) % inetAddresses.length];
	}

	private boolean isValidHost(String host, InetAddress inetAddress, boolean blockLocalAddresses) {
		byte[] addressBytes = inetAddress.getAddress();
		if (addressBytes == null || addressBytes.length != 4)
			return false;
		// 0.0.0.*
		if (addressBytes[0] == 0 && addressBytes[1] == 0 && addressBytes[2] == 0)
			return false;
		if (!blockLocalAddresses)
			return true;
		// 127.0.0.1
		if (addressBytes[0] == 127 && addressBytes[1] == 0 && addressBytes[2] == 0 && addressBytes[3] == 1)
			return false;
		if ("localhost".equals(host) || "127.0.0.1".equals(host))
			return false;
		return true;
	}

	private boolean isBindExceptionBlocked(InetSocketAddress inetAddress) {
		if (bindExceptionBlockTimeout == 0L)
			return false;
		Long bindExceptionTimestamp = bindExceptionBlockedHosts.get(inetAddress);
		if (bindExceptionTimestamp == null)
			return false;
		if (eventloop.currentTimeMillis() < bindExceptionTimestamp + bindExceptionBlockTimeout)
			return true;
		bindExceptionBlockedHosts.remove(inetAddress);
		return false;
	}

	// JMX
/*
*   public void resetStats() {
*		timeCheckExpired.resetStats();
*	}
*/

	@JmxOperation
	public void startMonitoring() {
		jmxMonitoring = true;
	}

	@JmxOperation
	public void stopMonitoring() {
		jmxMonitoring = false;
	}

	@JmxAttribute
	public EventStats getTotalRequests() {
		return totalRequests;
	}

	@JmxAttribute
	public EventStats getBlockedRequests() {
		return blockedRequests;
	}

	@JmxAttribute
	public ExceptionStats getDnsErrors() {
		return dnsErrors;
	}

	@JmxAttribute
	public ValueStats getTimeCheckExpiredMicros() {
		return timeCheckExpired;
	}

	@JmxAttribute
	public List<String> getCachedAddressConnections() {
		if (cachedIpConnectionLists.isEmpty()) {
			return null;
		}
		List<String> result = new ArrayList<>();
		result.add("SocketAddress,CachedConnectionsCount");
		for (Entry<InetSocketAddress, ExposedLinkedList<HttpClientConnection>> entry : cachedIpConnectionLists.entrySet()) {
			InetSocketAddress address = entry.getKey();
			ExposedLinkedList<HttpClientConnection> connections = entry.getValue();
			result.add(address + "," + connections.size());
		}
		return result;
	}

	@JmxAttribute
	public int getConnectionsCount() {
		return connectionsList.size();
	}

	@JmxAttribute
	public List<String> getConnections() {
		List<String> info = new ArrayList<>();
		info.add("RemoteSocketAddress,isRegistered,LifeTime,ActivityTime,KeepAlive");
		for (Node<AbstractHttpConnection> node = connectionsList.getFirstNode(); node != null; node = node.getNext()) {
			AbstractHttpConnection connection = node.getValue();
			String string = StringUtils.join(",",
					asList(
							connection.getRemoteSocketAddress(),
							connection.isRegistered(),
							MBeanFormat.formatPeriodAgo(connection.getLifeTime()),
							MBeanFormat.formatPeriodAgo(connection.getActivityTime()),
							connection.keepAlive
					)
			);
			info.add(string);
		}
		return info;
	}

	@JmxAttribute
	public ValueStats getExpiredConnectionsStats() {
		return expiredConnections;
	}

	@JmxAttribute
	public CountStats getPendingConnectionsCount() {
		return pendingSocketConnect;
	}

	@JmxAttribute
	public List<String> getAddressConnections() {
		if (addressConnectsMonitor.addressConnects.isEmpty()) {
			return null;
		}
		List<String> result = new ArrayList<>();
		result.add("Address,Connects");
		for (Entry<InetSocketAddress, AddressConnectsMonitor.AddressConnects> entry : addressConnectsMonitor.addressConnects.entrySet()) {
			result.add(entry.getKey() + "," + entry.getValue());
		}
		return result;
	}

	@JmxOperation
	public String getAddressConnections(@JmxParameter("host") String host, @JmxParameter("port") int port) {
		try {
			if (port == 0) port = 80;
			InetSocketAddress address = new InetSocketAddress(host, port);
			AddressConnectsMonitor.AddressConnects addressConnects = addressConnectsMonitor.addressConnects.get(address);
			return addressConnects == null ? null : addressConnects.toString();
		} catch (Exception e) {
			return e.getMessage();
		}
	}

	@JmxAttribute
	public List<String> getBindExceptionBlockedHosts() {
		if (bindExceptionBlockedHosts.isEmpty()) {
			return null;
		}
		List<String> result = new ArrayList<>();
		result.add("Address,DateTime");
		for (Entry<InetSocketAddress, Long> entry : bindExceptionBlockedHosts.entrySet()) {
			result.add(entry.getKey() + "," + MBeanFormat.formatDateTime(entry.getValue()));
		}
		return result;
	}
}
