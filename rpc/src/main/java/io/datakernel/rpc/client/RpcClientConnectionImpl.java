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

import io.datakernel.async.AsyncCancellable;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.rpc.client.jmx.RpcClientConnectionJmx;
import io.datakernel.rpc.client.jmx.RpcJmxStatsManager;
import io.datakernel.rpc.protocol.*;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.time.CurrentTimeProvider;
import org.slf4j.Logger;

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;

import static org.slf4j.LoggerFactory.getLogger;

public final class RpcClientConnectionImpl implements RpcClientConnection, RpcClientConnectionJmx {
	public static final int DEFAULT_TIMEOUT_PRECISION = 10; //ms

	private final class TimeoutCookie implements Comparable<TimeoutCookie> {
		private final int timeout;
		private final long timestamp;
		private final int cookie;

		public TimeoutCookie(int cookie, int timeout) {
			this.timeout = timeout;
			this.timestamp = eventloop.currentTimeMillis() + timeout;
			this.cookie = cookie;
		}

		public int getTimeoutMillis() {
			return timeout;
		}

		public boolean isExpired() {
			return timestamp < eventloop.currentTimeMillis();
		}

		public int getCookie() {
			return cookie;
		}

		public int getElapsedTime() {
			return (int) (eventloop.currentTimeMillis() - timestamp + timeout);
		}

		@Override
		public int compareTo(TimeoutCookie o) {
			return Long.compare(timestamp, o.timestamp);
		}
	}

	private static final Logger logger = getLogger(RpcClientConnection.class);
	@SuppressWarnings("ThrowableInstanceNeverThrown")
	private static final RpcOverloadException OVERLOAD_EXCEPTION =
			new RpcOverloadException("Write connection is overloaded");
	private final NioEventloop eventloop;
	private final RpcProtocol protocol;
	private final StatusListener statusListener;
	private final Map<Integer, ResultCallback<?>> requests = new HashMap<>();
	private final PriorityQueue<TimeoutCookie> timeoutCookies = new PriorityQueue<>();
	private final Runnable expiredResponsesTask = createExpiredResponsesTask();

	private AsyncCancellable scheduleExpiredResponsesTask;
	private int cookieCounter = 0;
	private boolean closing;

	// JMX
	private RpcJmxStatsManager.RpcAddressStatsManager addressStatsManager;
	private final Map<Integer, Long> requestToStartTimestamp = new HashMap<>();
	private final CurrentTimeProvider timeProvider;

	public RpcClientConnectionImpl(NioEventloop eventloop, SocketChannel socketChannel,
	                               BufferSerializer<RpcMessage> messageSerializer,
	                               RpcProtocolFactory protocolFactory, StatusListener statusListener) {
		this.eventloop = eventloop;
		this.statusListener = statusListener;
		this.protocol = protocolFactory.create(this, socketChannel, messageSerializer, false);

		// JMX
		this.timeProvider = eventloop;
	}

	@Override
	public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
		assert eventloop.inEventloopThread();

		if (isMonitoring()) {
			addressStatsManager.recordNewRequest();
		}

		if (!(request instanceof RpcMandatoryData) && protocol.isOverloaded()) {
			if (isMonitoring()) {
				addressStatsManager.recordRejectedRequest();
			}
			if (logger.isWarnEnabled())
				logger.warn(OVERLOAD_EXCEPTION.getMessage());
			returnProtocolError(callback, OVERLOAD_EXCEPTION);
			return;
		}
		sendMessageData(request, timeout, callback);
	}

	private void sendMessageData(Object request, int timeout, ResultCallback<?> callback) {
		cookieCounter++;
		if (requests.containsKey(cookieCounter)) {
			String msg = "Request ID " + cookieCounter + " is already in use";
			if (logger.isErrorEnabled())
				logger.error(msg);
			returnProtocolError(callback, new IllegalStateException(msg));
			return;
		}
		TimeoutCookie timeoutCookie = new TimeoutCookie(cookieCounter, timeout);
		addTimeoutCookie(timeoutCookie);
		requests.put(cookieCounter, callback);
		if (isMonitoring()) {
			requestToStartTimestamp.put(cookieCounter, timeProvider.currentTimeMillis());
		}
		try {
			protocol.sendMessage(new RpcMessage(cookieCounter, request));
		} catch (Exception e) {
			if (logger.isErrorEnabled())
				logger.error("Send RpcMessage {} failed", request, e);
			removeTimeoutCookie(timeoutCookie);
			returnProtocolError(requests.remove(cookieCounter), e);
		}
	}

	private void addTimeoutCookie(TimeoutCookie timeoutCookie) {
		if (timeoutCookies.isEmpty())
			scheduleExpiredResponsesTask();
		timeoutCookies.add(timeoutCookie);
	}

	private void scheduleExpiredResponsesTask() {
		if (closing)
			return;
		scheduleExpiredResponsesTask = eventloop.schedule(eventloop.currentTimeMillis() + DEFAULT_TIMEOUT_PRECISION, expiredResponsesTask);
	}

	private Runnable createExpiredResponsesTask() {
		return new Runnable() {
			@Override
			public void run() {
				checkExpiredResponses();
				if (!timeoutCookies.isEmpty())
					scheduleExpiredResponsesTask();
			}
		};
	}

	private void checkExpiredResponses() {
		while (!timeoutCookies.isEmpty()) {
			TimeoutCookie timeoutCookie = timeoutCookies.peek();
			if (timeoutCookie == null)
				break;
			if (!requests.containsKey(timeoutCookie.getCookie())) {
				timeoutCookies.remove();
				continue;
			}
			if (!timeoutCookie.isExpired())
				break;
			doTimeout(timeoutCookie);
			timeoutCookies.remove();
		}
	}

	private void doTimeout(TimeoutCookie timeoutCookie) {
		ResultCallback<?> callback = requests.remove(timeoutCookie.getCookie());
		if (callback == null)
			return;
		if (isMonitoring()) {
			addressStatsManager.recordExpiredRequest();
		}
		returnTimeout(callback, new RpcTimeoutException("Timeout (" + timeoutCookie.getElapsedTime() + "/" + timeoutCookie.getTimeoutMillis()
				+ " ms) for server response for request ID " + timeoutCookie.getCookie()));
	}

	private void removeTimeoutCookie(TimeoutCookie timeoutCookie) {
		timeoutCookies.remove(timeoutCookie);
	}

	private void returnTimeout(ResultCallback<?> callback, Exception exception) {
		returnError(callback, exception);
	}

	private void returnProtocolError(ResultCallback<?> callback, Exception exception) {
		returnError(callback, exception);
	}

	private void returnError(ResultCallback<?> callback, Exception exception) {
		if (callback != null) {
			callback.onException(exception);
		}
	}

	@Override
	public void onReceiveMessage(RpcMessage message) {
		if (message.getData().getClass() == RpcRemoteException.class) {
			RpcRemoteException remoteException = (RpcRemoteException) message.getData();
			processError(message, remoteException);
		} else {
			processResponse(message);
		}
	}

	private void processError(RpcMessage message, RpcRemoteException exception) {
		if (isMonitoring() && requestToStartTimestamp.containsKey(message.getCookie())) {
			int responseTime = (int) (timeProvider.currentTimeMillis() - requestToStartTimestamp.get(message.getCookie()));
			addressStatsManager.recordFailedRequest(exception, null, responseTime);
		}
		ResultCallback<?> callback = getResultCallback(message);
		if (callback == null)
			return;
		returnError(callback, exception);
	}

	private void processResponse(RpcMessage message) {
		ResultCallback<Object> callback = getResultCallback(message);
		if (callback == null)
			return;
		if (isMonitoring() && requestToStartTimestamp.containsKey(message.getCookie())) {
			int responseTime = (int) (timeProvider.currentTimeMillis() - requestToStartTimestamp.get(message.getCookie()));
			addressStatsManager.recordSuccessfulRequest(responseTime);
		}
		callback.onResult(message.getData());
	}

	@SuppressWarnings("unchecked")
	private <T> ResultCallback<T> getResultCallback(RpcMessage message) {
		return (ResultCallback<T>) requests.remove(message.getCookie());
	}

	@Override
	public void close() {
		closing = true;
		protocol.close();
	}

	@Override
	public void ready() {
		statusListener.onOpen(this);
	}

	@Override
	public void onClosed() {
		if (scheduleExpiredResponsesTask != null)
			scheduleExpiredResponsesTask.cancel();
		if (!requests.isEmpty()) {
			closeNotify();
		}
		statusListener.onClosed();
	}

	private void closeNotify() {
		for (Integer cookie : new HashSet<>(requests.keySet())) {
			returnProtocolError(requests.remove(cookie), new RpcException("Connection closed."));
		}
	}

	@Override
	public NioEventloop getEventloop() {
		return eventloop;
	}

	@Override
	public SocketConnection getSocketConnection() {
		return protocol.getSocketConnection();
	}

	// JMX

	@Override
	public void startMonitoring(RpcJmxStatsManager.RpcAddressStatsManager addressStatsManager) {
		this.addressStatsManager = addressStatsManager;
	}

	@Override
	public void stopMonitoring() {
		this.addressStatsManager = null;
	}

	private boolean isMonitoring() {
		return addressStatsManager != null;
	}
}
