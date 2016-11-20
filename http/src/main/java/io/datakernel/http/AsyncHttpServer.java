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

import io.datakernel.async.AsyncCancellable;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.*;
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.EventStats;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxReducers;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;

import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static io.datakernel.http.AbstractHttpConnection.MAX_HEADER_LINE_SIZE;
import static io.datakernel.jmx.MBeanFormat.formatDuration;

/**
 * A server which works asynchronously. An instance of {@code AsyncHttpServer}
 * can be created by calling {@link #create(Eventloop, AsyncServlet)} method
 * and providing an {@link Eventloop} instance and an implementation of
 * {@link AsyncServlet}.
 * <p>
 * The creation of asynchronous http server implies few steps:
 * <ol>
 *     <li>Create an {@code eventloop} for a server</li>
 *     <li>Create a {@code servlet}, which will respond to received request</li>
 *     <li>Create a {@code server} with these instances</li>
 * </ol>
 * For example, consider an {@code AsyncHttpServer} with default
 * {@code eventloop} and anonymous implementation of {@code AsyncServlet}.
 * <pre>
 * <code>final {@link Eventloop Eventloop} eventloop = Eventloop.create();
 * final {@link AsyncServlet AsyncServlet} servlet = new AsyncServlet() {
 *    {@literal @}Override
 *     public void serve({@link HttpRequest HttpRequest} request, final {@link ResultCallback ResultCallback<HttpResponse>} callback) {
 *     	final HttpResponse response = HttpResponse.ok200().withBody(ByteBufStrings.encodeAscii("Hello, client!"));
 *     		eventloop.post(new Runnable() {
 *		   {@literal @}Override
 *  		    public void run() {
 *  		    System.out.println("Request body: " + request.getBody().toString());
 *     			callback.setResult(response);
 *     		    }
 *  		});
 * 	}
 * };
 * AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet).withListenPort(40000);
 * server.listen();
 * eventloop.run(); //eventloop runs in current thread
 * </code>
 * </pre>
 * Now server is ready for accepting requests and responding to clients with
 * <pre>"Hello, client!"</pre> message. For the sake of code simplicity, a standard Java
 * {@link Socket} will be used to send a request.
 * <pre>
 * <code>//create a new thread for eventloop
 * Socket socket = new Socket();
 * socket.connect(new InetSocketAddress(40000));
 * {@link HttpRequest HttpRequest} request = HttpRequest.post("http://127.0.0.1:" + 40000)
 * 	.withBody("Java socket request".getBytes());
 * socket.getOutputStream().write(request.toByteBuf().array());
 * server.closeFuture().get();
 * socket.close();
 * </code>
 * </pre>
 * It's easy to create a client for this example using
 * {@link AsyncHttpClient} or send a request with, for example, {@link AsyncTcpSocketImpl}.
 */
public final class AsyncHttpServer extends AbstractServer<AsyncHttpServer> {
	public static final long DEFAULT_KEEP_ALIVE_MILLIS = 30 * 1000L;

	private static final HttpExceptionFormatter DEFAULT_ERROR_FORMATTER = new HttpExceptionFormatter() {
		@Override
		public HttpResponse formatException(Exception e) {
			if (e instanceof HttpException) {
				HttpException httpException = (HttpException) e;
				return HttpResponse.ofCode(httpException.getCode()).withNoCache();
			}
			if (e instanceof ParseException) {
				return HttpResponse.ofCode(400).withNoCache();
			}
			return HttpResponse.ofCode(500).withNoCache();
		}
	};

	private final AsyncServlet servlet;
	private final HttpExceptionFormatter errorFormatter;
	private final int maxHttpMessageSize;
	private final long keepAliveTimeMillis;
	private final ExposedLinkedList<AbstractHttpConnection> keepAlivePool;
	private final char[] headerChars;

	private AsyncCancellable expiredConnectionsCheck;

	// jmx
	private final EventStats totalRequests = EventStats.create();
	private final EventStats httpsRequests = EventStats.create();
	private final EventStats httpRequests = EventStats.create();
	private final EventStats keepAliveRequests = EventStats.create();
	private final EventStats nonKeepAliveRequests = EventStats.create();
	private final EventStats expiredConnections = EventStats.create();
	private final EventStats httpProtocolErrors = EventStats.create();
	private final EventStats applicationErrors = EventStats.create();
	private final Map<HttpServerConnection, UrlWithTimestamp> currentRequestHandlingStart = new HashMap<>();
	private boolean monitorCurrentRequestsHandlingDuration = false;

	// region builders
	private AsyncHttpServer(Eventloop eventloop,
	                        ServerSocketSettings serverSocketSettings, SocketSettings socketSettings,
	                        boolean acceptOnce, Collection<InetSocketAddress> listenAddresses,
	                        InetAddressRange range, Collection<InetAddress> bannedAddresses,
	                        SSLContext sslContext, ExecutorService sslExecutor,
	                        Collection<InetSocketAddress> sslListenAddresses,
	                        AsyncHttpServer prevInstance) {

		super(eventloop, serverSocketSettings, socketSettings, acceptOnce, listenAddresses,
				range, bannedAddresses, sslContext, sslExecutor, sslListenAddresses);
		this.servlet = prevInstance.servlet;
		this.errorFormatter = prevInstance.errorFormatter;
		this.keepAliveTimeMillis = prevInstance.keepAliveTimeMillis;
		this.maxHttpMessageSize = prevInstance.maxHttpMessageSize;
		this.keepAlivePool = prevInstance.keepAlivePool;
		this.headerChars = prevInstance.headerChars;
	}

	private AsyncHttpServer(AsyncHttpServer previousInstance, AsyncServlet servlet, HttpExceptionFormatter errorFormatter,
	                        long keepAliveTimeMillis, int maxHttpMessageSize,
	                        ExposedLinkedList<AbstractHttpConnection> keepAlivePool, char[] headerChars) {
		super(previousInstance);
		this.servlet = servlet;
		this.errorFormatter = errorFormatter;
		this.keepAlivePool = keepAlivePool;
		this.keepAliveTimeMillis = keepAliveTimeMillis;
		this.maxHttpMessageSize = maxHttpMessageSize;
		this.headerChars = headerChars;
	}

	private AsyncHttpServer(Eventloop eventloop, AsyncServlet servlet, HttpExceptionFormatter errorFormatter,
	                        long keepAliveTimeMillis, int maxHttpMessageSize,
	                        ExposedLinkedList<AbstractHttpConnection> keepAlivePool, char[] headerChars) {
		super(eventloop);
		this.servlet = servlet;
		this.errorFormatter = errorFormatter;
		this.keepAlivePool = keepAlivePool;
		this.keepAliveTimeMillis = keepAliveTimeMillis;
		this.maxHttpMessageSize = maxHttpMessageSize;
		this.headerChars = headerChars;
	}

	public static AsyncHttpServer create(Eventloop eventloop, AsyncServlet servlet) {
		ExposedLinkedList<AbstractHttpConnection> pool = ExposedLinkedList.create();
		char[] chars = new char[MAX_HEADER_LINE_SIZE];
		int maxHttpMessageSize = Integer.MAX_VALUE;
		return new AsyncHttpServer(eventloop, servlet, DEFAULT_ERROR_FORMATTER, DEFAULT_KEEP_ALIVE_MILLIS, maxHttpMessageSize, pool, chars);
	}

	public AsyncHttpServer withKeepAliveTimeMillis(long maxIdleConnectionTime) {
		return new AsyncHttpServer(this, servlet, errorFormatter, maxIdleConnectionTime, maxHttpMessageSize, keepAlivePool, headerChars);
	}

	public AsyncHttpServer withNoKeepAlive() {
		return withKeepAliveTimeMillis(0);
	}

	public AsyncHttpServer withMaxHttpMessageSize(int maxHttpMessageSize) {
		return new AsyncHttpServer(this, servlet, errorFormatter, keepAliveTimeMillis, maxHttpMessageSize, keepAlivePool, headerChars);
	}

	public AsyncHttpServer withMaxHttpMessageSize(MemSize size) {
		return withMaxHttpMessageSize((int) size.get());
	}

	public AsyncHttpServer withHttpErrorFormatter(HttpExceptionFormatter httpExceptionFormatter) {
		return new AsyncHttpServer(this, servlet, httpExceptionFormatter, keepAliveTimeMillis, maxHttpMessageSize, keepAlivePool, headerChars);
	}

	@Override
	protected AsyncHttpServer recreate(Eventloop eventloop, ServerSocketSettings serverSocketSettings, SocketSettings socketSettings,
	                                   boolean acceptOnce,
	                                   Collection<InetSocketAddress> listenAddresses,
	                                   InetAddressRange range, Collection<InetAddress> bannedAddresses,
	                                   SSLContext sslContext, ExecutorService sslExecutor,
	                                   Collection<InetSocketAddress> sslListenAddresses) {
		return new AsyncHttpServer(eventloop, serverSocketSettings, socketSettings, acceptOnce, listenAddresses,
				range, bannedAddresses, sslContext, sslExecutor, sslListenAddresses, this);
	}
	// endregion

	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck = eventloop.scheduleBackground(eventloop.currentTimeMillis() + 1000L, new Runnable() {
			@Override
			public void run() {
				expiredConnectionsCheck = null;
				checkExpiredConnections();
				if (!keepAlivePool.isEmpty()) {
					scheduleExpiredConnectionsCheck();
				}
			}
		});
	}

	private int checkExpiredConnections() {
		int count = 0;
		final long now = eventloop.currentTimeMillis();

		ExposedLinkedList.Node<AbstractHttpConnection> node = keepAlivePool.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert eventloop.inEventloopThread();
			long idleTime = now - connection.keepAliveTimestamp;
			if (idleTime > keepAliveTimeMillis) {
				connection.close(); // self removing from this pool
				count++;
			}
		}
		expiredConnections.recordEvents(count);
		return count;
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
		assert eventloop.inEventloopThread();
		return HttpServerConnection.create(
				eventloop, asyncTcpSocket.getRemoteSocketAddress().getAddress(), asyncTcpSocket,
				this, servlet, keepAlivePool, headerChars, maxHttpMessageSize);
	}

	@Override
	protected void onClose(final CompletionCallback completionCallback) {
		ExposedLinkedList.Node<AbstractHttpConnection> node = keepAlivePool.getFirstNode();
		while (node != null) {
			AbstractHttpConnection connection = node.getValue();
			node = node.getNext();

			assert eventloop.inEventloopThread();
			connection.close();
		}

		// TODO(vmykhalko): consider proper usage of completion callback
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				completionCallback.setComplete();
			}
		});
	}

	void returnToPool(final HttpServerConnection connection) {
		if (!isRunning() || keepAliveTimeMillis == 0) {
			eventloop.execute(new Runnable() {
				@Override
				public void run() {
					connection.close();
				}
			});
			return;
		}

		keepAlivePool.addLastNode(connection.poolNode);
		connection.keepAliveTimestamp = eventloop.currentTimeMillis();

		if (expiredConnectionsCheck == null) {
			scheduleExpiredConnectionsCheck();
		}
	}

	void removeFromPool(HttpServerConnection connection) {
		keepAlivePool.removeNode(connection.poolNode);
		connection.keepAliveTimestamp = 0L;
	}

	// jmx
	void recordRequestEvent(boolean https, boolean keepAlive) {
		totalRequests.recordEvent();

		if (https) {
			httpsRequests.recordEvent();
		} else {
			httpRequests.recordEvent();
		}

		if (keepAlive) {
			keepAliveRequests.recordEvent();
		} else {
			nonKeepAliveRequests.recordEvent();
		}
	}

	void recordHttpProtocolError() {
		httpProtocolErrors.recordEvent();
	}

	void recordApplicationError() {
		applicationErrors.recordEvent();
	}

	@JmxAttribute(
			description = "current number of connections",
			reducer = JmxReducers.JmxReducerSum.class
	)
	public int getConnectionsCount() {
		return keepAlivePool.size();
	}

	@JmxAttribute()
	public EventStats getTotalRequests() {
		return totalRequests;
	}

	@JmxAttribute(description = "requests that was sent over secured connection (https)")
	public EventStats getHttpsRequests() {
		return httpsRequests;
	}

	@JmxAttribute(description = "requests that was sent over unsecured connection (http)")
	public EventStats getHttpRequests() {
		return httpRequests;
	}

	@JmxAttribute(description = "after handling this type of request connection is returned to pool")
	public EventStats getKeepAliveRequests() {
		return keepAliveRequests;
	}

	@JmxAttribute(description = "after handling this type of request connection is closed")
	public EventStats getNonKeepAliveRequests() {
		return nonKeepAliveRequests;
	}

	@JmxAttribute(description = "number of expired connections in pool (after appropriate timeout)")
	public EventStats getExpiredConnections() {
		return expiredConnections;
	}

	@JmxAttribute(description = "Number of requests which were invalid according to http protocol. " +
			"Responses were not sent for this requests")
	public EventStats getHttpProtocolErrors() {
		return httpProtocolErrors;
	}

	@JmxAttribute(description = "Number of requests which were valid according to http protocol, " +
			"but application produced error during handling this request " +
			"(responses with 4xx and 5xx HTTP status codes)")
	public EventStats getApplicationErrors() {
		return applicationErrors;
	}

	@JmxAttribute
	public boolean isMonitorCurrentRequestsHandlingDuration() {
		return monitorCurrentRequestsHandlingDuration;
	}

	@JmxAttribute
	public void setMonitorCurrentRequestsHandlingDuration(boolean monitor) {
		if (!monitor) {
			currentRequestHandlingStart.clear();
		}
		this.monitorCurrentRequestsHandlingDuration = monitor;
	}

	@JmxAttribute(
			description = "shows duration of current requests handling" +
					"in case when monitorCurrentRequestsHandlingDuration == true"
	)
	public List<String> getCurrentRequestsDuration() {
		SortedSet<UrlWithDuration> durations = new TreeSet<>();
		for (HttpServerConnection conn : currentRequestHandlingStart.keySet()) {
			UrlWithTimestamp urlWithTimestamp = currentRequestHandlingStart.get(conn);
			int duration = (int) (eventloop.currentTimeMillis() - urlWithTimestamp.getTimestamp());
			String url = urlWithTimestamp.getUrl();
			durations.add(new UrlWithDuration(url, duration));
		}

		List<String> formattedDurations = new ArrayList<>(durations.size());
		formattedDurations.add("Duration       Url");
		for (UrlWithDuration urlWithDuration : durations) {
			String url = urlWithDuration.getUrl();
			String duration = formatDuration(urlWithDuration.getDuration());
			String line = String.format("%s   %s", duration, url);
			formattedDurations.add(line);
		}
		return formattedDurations;
	}

	void requestHandlingStarted(HttpServerConnection conn, HttpRequest request) {
		if (isMonitorCurrentRequestsHandlingDuration()) {
			String url = request.getFullUrl();
			long timestamp = eventloop.currentTimeMillis();
			currentRequestHandlingStart.put(conn, new UrlWithTimestamp(url, timestamp));
		}
	}

	void requestHandlingFinished(HttpServerConnection conn) {
		if (isMonitorCurrentRequestsHandlingDuration()) {
			currentRequestHandlingStart.remove(conn);
		}
	}

	HttpResponse formatHttpError(Exception e) {
		return errorFormatter.formatException(e);
	}

	private static final class UrlWithTimestamp {
		private final String url;
		private final long timestamp;

		public UrlWithTimestamp(String url, long timestamp) {
			this.url = url;
			this.timestamp = timestamp;
		}

		public String getUrl() {
			return url;
		}

		public long getTimestamp() {
			return timestamp;
		}
	}

	private static final class UrlWithDuration implements Comparable<UrlWithDuration> {
		private final String url;
		private final int duration;

		public UrlWithDuration(String url, int duration) {
			this.url = url;
			this.duration = duration;
		}

		public String getUrl() {
			return url;
		}

		public int getDuration() {
			return duration;
		}

		@Override
		public int compareTo(UrlWithDuration other) {
			return -Integer.compare(duration, other.duration);
		}
	}
}
