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

package io.datakernel.rpc.client.jmx;

import io.datakernel.jmx.CompositeDataBuilder;
import io.datakernel.jmx.EventsCounter;
import io.datakernel.jmx.LastExceptionCounter;
import io.datakernel.jmx.StatsCounter;
import io.datakernel.time.CurrentTimeProvider;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Thread safe class
 */
public final class RpcJmxStatsManager implements RpcJmxStatsManagerMBean {

	// CompositeData keys
	public static final String REQUEST_CLASS_KEY = "Request class";
	public static final String ADDRESS_KEY = "Address";
	public static final String TOTAL_REQUESTS_KEY = "Total requests";
	public static final String PENDING_REQUESTS_KEY = "Pending requests";
	public static final String SUCCESSFUL_REQUESTS_KEY = "Successful requests";
	public static final String FAILED_REQUESTS_KEY = "Failed requests";
	public static final String REJECTED_REQUESTS_KEY = "Rejected requests";
	public static final String EXPIRED_REQUESTS_KEY = "Expired requests";
	public static final String RESPONSE_TIME_KEY = "Response time";
	public static final String LAST_SERVER_EXCEPTION_KEY = "Last server exception";
	public static final String TOTAL_EXCEPTIONS_KEY = "Total exceptions";
	public static final String SUCCESSFUL_CONNECTS_KEY = "Successful connects";
	public static final String FAILED_CONNECTS_KEY = "Failed connects";
	public static final String CLOSED_CONNECTS_KEY = "Closed connects";

	private static final String LAST_SERVER_EXCEPTION_COUNTER_NAME = "Server exception";
	private static final String REQUEST_CLASS_COMPOSITE_DATA_NAME = "Request class stats";
	private static final String ADDRESS_COMPOSITE_DATA_NAME = "Address stats";

	// settings
	private volatile boolean monitoring;    // TODO(vmykhalko): add thread-safety
	private double smoothingWindow;
	private double smoothingPrecision;
	private CurrentTimeProvider timeProvider;
	private List<RpcJmxClient> rpcClients;

	// stats per connection and per request class
	private Map<InetSocketAddress, RpcAddressStatsManager> statsPerAddress;
	private Map<Class<?>, ParticularStats> statsPerRequestClass;

	// general stats
	private final EventsCounter totalRequests;
	private final EventsCounter successfulRequests;
	private final EventsCounter failedRequests;
	private final EventsCounter rejectedRequests;
	private final EventsCounter expiredRequests;
	private final StatsCounter pendingRequests;
	private final StatsCounter responseTimeStats;
	private final LastExceptionCounter lastServerException;

	private final EventsCounter successfulConnects;
	private final EventsCounter failedConnects;
	private final EventsCounter closedConnects;

	public RpcJmxStatsManager(List<RpcJmxClient> rpcClients, double smoothingWindow, double smoothingPrecision,
	                          CurrentTimeProvider timeProvider) {
		this.monitoring = false;
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;
		this.timeProvider = timeProvider;
		this.rpcClients = rpcClients;

		this.statsPerAddress = new HashMap<>();
		this.statsPerRequestClass = new HashMap<>();

		this.totalRequests = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.successfulRequests = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.failedRequests = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.rejectedRequests = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.expiredRequests = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.pendingRequests = new StatsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.responseTimeStats = new StatsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.lastServerException = new LastExceptionCounter(LAST_SERVER_EXCEPTION_COUNTER_NAME);

		this.successfulConnects = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.failedConnects = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.closedConnects = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
	}

	// stats manager api
	public synchronized void recordNewRequest(Class<?> requestClass) {
		// TODO(vmykhalko): is it needed to check whether monitoring flag is true
		ParticularStats requestClassStats = getRequestClassStats(requestClass);

		totalRequests.recordEvent();
		requestClassStats.getTotalRequests().recordEvent();

		incrementStatsCounter(requestClassStats.getPendingRequests());
		incrementStatsCounter(pendingRequests);
	}

	public synchronized void recordSuccessfulRequest(Class<?> requestClass, int responseTime) {
		preprocessFinishedRequest(requestClass);
		updateResponseTime(requestClass, responseTime);

		successfulRequests.recordEvent();
		getRequestClassStats(requestClass).getSuccessfulRequests().recordEvent();
	}

	public synchronized void recordFailedRequest(Class<?> requestClass, Exception exception, Object causedObject, int responseTime) {
		preprocessFinishedRequest(requestClass);
		updateResponseTime(requestClass, responseTime);

		failedRequests.recordEvent();
		getRequestClassStats(requestClass).getFailedRequests().recordEvent();

		lastServerException.update(exception, causedObject, timeProvider.currentTimeMillis());
		getRequestClassStats(requestClass).getLastServerExceptionCounter()
				.update(exception, causedObject, timeProvider.currentTimeMillis());
	}

	public synchronized void recordRejectedRequest(Class<?> requestClass) {
		preprocessFinishedRequest(requestClass);
		rejectedRequests.recordEvent();
		getRequestClassStats(requestClass).getRejectedRequests().recordEvent();
	}

	public synchronized void recordExpiredRequest(Class<?> requestClass) {
		preprocessFinishedRequest(requestClass);
		expiredRequests.recordEvent();
		getRequestClassStats(requestClass).getExpiredRequests().recordEvent();
	}

	public synchronized void recordSuccessfulConnect(InetSocketAddress address) {
		successfulConnects.recordEvent();
		getAddressStatsManager(address).recordSuccessfulConnect();
	}

	public synchronized void recordFailedConnect(InetSocketAddress address) {
		failedConnects.recordEvent();
		getAddressStatsManager(address).recordFailedConnect();
	}

	public synchronized void recordClosedConnect(InetSocketAddress address) {
		closedConnects.recordEvent();
		getAddressStatsManager(address).recordClosedConnect();
	}

	// TODO(vmykhalko): maybe it will be better to set addresses only once in constructor / or in resetStats() ?
	public synchronized RpcAddressStatsManager getAddressStatsManager(InetSocketAddress address) {
		if (!statsPerAddress.containsKey(address)) {
			statsPerAddress.put(address, new RpcAddressStatsManager(smoothingWindow, smoothingPrecision, timeProvider));
		}
		return statsPerAddress.get(address);
	}

	// jmx api
	@Override
	public synchronized void startMonitoring() {
		monitoring = true;
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.startMonitoring(this);
		}
	}

	@Override
	public synchronized void stopMonitoring() {
		monitoring = false;
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.stopMonitoring();
		}
	}

	@Override
	public synchronized boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public synchronized void resetStats() {
		resetStats(smoothingWindow, smoothingPrecision);
	}

	@Override
	public synchronized void resetStats(double smoothingWindow, double smoothingPrecision) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;

		statsPerAddress = new HashMap<>();
		statsPerRequestClass = new HashMap<>();

		totalRequests.reset(smoothingWindow, smoothingPrecision);
		successfulRequests.reset(smoothingWindow, smoothingPrecision);
		failedRequests.reset(smoothingWindow, smoothingPrecision);
		rejectedRequests.reset(smoothingWindow, smoothingPrecision);
		expiredRequests.reset(smoothingWindow, smoothingPrecision);
		pendingRequests.reset(smoothingWindow, smoothingPrecision);
		responseTimeStats.reset(smoothingWindow, smoothingPrecision);
		lastServerException.reset();

		successfulConnects.reset(smoothingWindow, smoothingPrecision);
		failedConnects.reset(smoothingWindow, smoothingPrecision);
		closedConnects.reset(smoothingWindow, smoothingPrecision);
	}

	@Override
	public synchronized String getAddresses() {
		StringBuilder result = new StringBuilder();
		String separator = ", ";
		for (InetSocketAddress address : statsPerAddress.keySet()) {
			result.append(address.toString());
			result.append(separator);
		}
		result.delete(result.lastIndexOf(separator), result.length());
		return result.toString();
	}

	@Override
	public synchronized int getActiveConnectionsCount() {
		int activeConnections = 0;
		for (RpcAddressStatsManager rpcAddressStatsManager : statsPerAddress.values()) {
			if (rpcAddressStatsManager.isConnectionActive()) {
				++activeConnections;
			}
		}
		return activeConnections;
	}

	@Override
	public synchronized CompositeData[] getAddressesStats() throws OpenDataException {
		List<CompositeData> compositeDataList = new ArrayList<>();
		for (InetSocketAddress address : statsPerAddress.keySet()) {
			RpcAddressStatsManager addressStats = statsPerAddress.get(address);
			Throwable lastException = addressStats.getLastServerException().getLastException();
			compositeDataList.add(CompositeDataBuilder.builder(ADDRESS_COMPOSITE_DATA_NAME)
							.add(ADDRESS_KEY, SimpleType.STRING, address.toString())
							.add(TOTAL_REQUESTS_KEY, SimpleType.STRING, addressStats.getTotalRequests().toString())
							.add(PENDING_REQUESTS_KEY, SimpleType.STRING, addressStats.getPendingRequests().toString())
							.add(SUCCESSFUL_REQUESTS_KEY, SimpleType.STRING, addressStats.getSuccessfulRequests().toString())
							.add(FAILED_REQUESTS_KEY, SimpleType.STRING, addressStats.getFailedRequests().toString())
							.add(REJECTED_REQUESTS_KEY, SimpleType.STRING, addressStats.getRejectedRequests().toString())
							.add(EXPIRED_REQUESTS_KEY, SimpleType.STRING, addressStats.getExpiredRequests().toString())
							.add(RESPONSE_TIME_KEY, SimpleType.STRING, addressStats.getResponseTimeStats().toString())
							.add(LAST_SERVER_EXCEPTION_KEY, SimpleType.STRING,
									lastException != null ? lastException.toString() : "")
							.add(TOTAL_EXCEPTIONS_KEY, SimpleType.STRING,
									Integer.toString(addressStats.getLastServerException().getTotal()))
							.add(SUCCESSFUL_CONNECTS_KEY, SimpleType.STRING,
									addressStats.getSuccessfulConnectsStats().toString())
							.add(FAILED_CONNECTS_KEY, SimpleType.STRING,
									addressStats.getFailedConnectsStats().toString())
							.add(CLOSED_CONNECTS_KEY, SimpleType.STRING,
									addressStats.getClosedConnectsStats().toString())
							.build()
			);
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
	}

	@Override
	public synchronized CompositeData[] getRequestClassesStats() throws OpenDataException {
		List<CompositeData> compositeDataList = new ArrayList<>();
		for (Class<?> requestClass : statsPerRequestClass.keySet()) {
			ParticularStats requestClassStats = statsPerRequestClass.get(requestClass);
			Throwable lastException = requestClassStats.getLastServerExceptionCounter().getLastException();
			compositeDataList.add(CompositeDataBuilder.builder(REQUEST_CLASS_COMPOSITE_DATA_NAME)
							.add(REQUEST_CLASS_KEY, SimpleType.STRING, requestClass.getName())
							.add(TOTAL_REQUESTS_KEY, SimpleType.STRING, requestClassStats.getTotalRequests().toString())
							.add(PENDING_REQUESTS_KEY, SimpleType.STRING, requestClassStats.getPendingRequests().toString())
							.add(SUCCESSFUL_REQUESTS_KEY, SimpleType.STRING, requestClassStats.getSuccessfulRequests().toString())
							.add(FAILED_REQUESTS_KEY, SimpleType.STRING, requestClassStats.getFailedRequests().toString())
							.add(REJECTED_REQUESTS_KEY, SimpleType.STRING, requestClassStats.getRejectedRequests().toString())
							.add(EXPIRED_REQUESTS_KEY, SimpleType.STRING, requestClassStats.getExpiredRequests().toString())
							.add(RESPONSE_TIME_KEY, SimpleType.STRING, requestClassStats.getResponseTimeStats().toString())
							.add(LAST_SERVER_EXCEPTION_KEY, SimpleType.STRING,
									lastException != null ? lastException.toString() : "")
							.add(TOTAL_EXCEPTIONS_KEY, SimpleType.STRING,
									Integer.toString(requestClassStats.getLastServerExceptionCounter().getTotal()))
							.build()
			);
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
	}

	@Override
	public synchronized String getTotalRequestsStats() {
		return totalRequests.toString();
	}

	@Override
	public synchronized String getSuccessfulRequestsStats() {
		return successfulRequests.toString();
	}

	@Override
	public synchronized String getFailedRequestsStats() {
		return failedRequests.toString();
	}

	@Override
	public synchronized String getRejectedRequestsStats() {
		return rejectedRequests.toString();
	}

	@Override
	public synchronized String getExpiredRequestsStats() {
		return expiredRequests.toString();
	}

	@Override
	public synchronized String getPendingRequestsStats() {
		return pendingRequests.toString();
	}

	@Override
	public synchronized String getSuccessfulConnectsStats() {
		return successfulConnects.toString();
	}

	@Override
	public synchronized String getFailedConnectsStats() {
		return failedConnects.toString();
	}

	@Override
	public synchronized String getClosedConnectsStats() {
		return closedConnects.toString();
	}

	@Override
	public synchronized String getAverageResponseTimeStats() {
		return responseTimeStats.toString();
	}

	@Override
	public synchronized CompositeData getLastServerException() {
		return lastServerException.compositeData();
	}

	@Override
	public synchronized int getExceptionsCount() {
		return lastServerException.getTotal();
	}

	// helpers
	private ParticularStats getRequestClassStats(Class<?> requestClass) {
		if (!statsPerRequestClass.containsKey(requestClass)) {
			statsPerRequestClass.put(requestClass, new ParticularStats(smoothingWindow, smoothingPrecision, timeProvider));
		}
		return statsPerRequestClass.get(requestClass);
	}

	private static void incrementStatsCounter(StatsCounter statsCounter) {
		statsCounter.recordValue(statsCounter.getLastValue() + 1);
	}

	private static void decrementStatsCounter(StatsCounter statsCounter) {
		statsCounter.recordValue(statsCounter.getLastValue() - 1);
	}

	private void preprocessFinishedRequest(Class<?> requestClass) {
		decrementStatsCounter(getRequestClassStats(requestClass).getPendingRequests());
		decrementStatsCounter(pendingRequests);
	}

	private void updateResponseTime(Class<?> requestClass, int responseTime) {
		getRequestClassStats(requestClass).getResponseTimeStats().recordValue(responseTime);
		responseTimeStats.recordValue(responseTime);
	}

	private static class ParticularStats {
		private final EventsCounter totalRequests;
		private final EventsCounter successfulRequests;
		private final EventsCounter failedRequests;
		private final EventsCounter rejectedRequests;
		private final EventsCounter expiredRequests;
		private final StatsCounter pendingRequests;
		private final StatsCounter responseTimeStats;
		private final LastExceptionCounter lastServerException;

		public ParticularStats(double window, double precision, CurrentTimeProvider timeProvider) {
			this.totalRequests = new EventsCounter(window, precision, timeProvider);
			this.successfulRequests = new EventsCounter(window, precision, timeProvider);
			this.failedRequests = new EventsCounter(window, precision, timeProvider);
			this.rejectedRequests = new EventsCounter(window, precision, timeProvider);
			this.expiredRequests = new EventsCounter(window, precision, timeProvider);
			this.pendingRequests = new StatsCounter(window, precision, timeProvider);
			this.responseTimeStats = new StatsCounter(window, precision, timeProvider);
			this.lastServerException = new LastExceptionCounter(LAST_SERVER_EXCEPTION_COUNTER_NAME);
		}

		public EventsCounter getTotalRequests() {
			return totalRequests;
		}

		public EventsCounter getSuccessfulRequests() {
			return successfulRequests;
		}

		public EventsCounter getFailedRequests() {
			return failedRequests;
		}

		public EventsCounter getRejectedRequests() {
			return rejectedRequests;
		}

		public EventsCounter getExpiredRequests() {
			return expiredRequests;
		}

		public StatsCounter getPendingRequests() {
			return pendingRequests;
		}

		public StatsCounter getResponseTimeStats() {
			return responseTimeStats;
		}

		public LastExceptionCounter getLastServerExceptionCounter() {
			return lastServerException;
		}
	}

	/**
	 * Not thread safe class
	 */
	public static class RpcAddressStatsManager {
		private final CurrentTimeProvider timeProvider;

		private final EventsCounter totalRequests;
		private final EventsCounter successfulRequest;
		private final EventsCounter failedRequest;
		private final EventsCounter rejectedRequest;
		private final EventsCounter expiredRequest;
		private final StatsCounter pendingRequests;
		private final StatsCounter responseTimeStats;
		private final LastExceptionCounter lastServerException;

		private final EventsCounter successfulConnects;
		private final EventsCounter failedConnects;
		private final EventsCounter closedConnects;

		private volatile boolean connectionActive;

		public RpcAddressStatsManager(double window, double precision, CurrentTimeProvider timeProvider) {
			this.timeProvider = timeProvider;

			this.totalRequests = new EventsCounter(window, precision, timeProvider);
			this.successfulRequest = new EventsCounter(window, precision, timeProvider);
			this.failedRequest = new EventsCounter(window, precision, timeProvider);
			this.rejectedRequest = new EventsCounter(window, precision, timeProvider);
			this.expiredRequest = new EventsCounter(window, precision, timeProvider);
			this.pendingRequests = new StatsCounter(window, precision, timeProvider);
			this.responseTimeStats = new StatsCounter(window, precision, timeProvider);
			this.lastServerException = new LastExceptionCounter(LAST_SERVER_EXCEPTION_COUNTER_NAME);

			this.successfulConnects = new EventsCounter(window, precision, timeProvider);
			this.failedConnects = new EventsCounter(window, precision, timeProvider);
			this.closedConnects = new EventsCounter(window, precision, timeProvider);

			this.connectionActive = false;
		}

		// public api
		public void recordNewRequest() {
			totalRequests.recordEvent();
			incrementStatsCounter(pendingRequests);
		}

		public void recordSuccessfulRequest(int responseTime) {
			decrementStatsCounter(pendingRequests);
			responseTimeStats.recordValue(responseTime);
			successfulRequest.recordEvent();
		}

		public void recordFailedRequest(Exception exception, Object causedObject, int responseTime) {
			decrementStatsCounter(pendingRequests);
			responseTimeStats.recordValue(responseTime);
			failedRequest.recordEvent();
			lastServerException.update(exception, causedObject, timeProvider.currentTimeMillis());
		}

		public void recordRejectedRequest() {
			decrementStatsCounter(pendingRequests);
			rejectedRequest.recordEvent();
		}

		public void recordExpiredRequest() {
			decrementStatsCounter(pendingRequests);
			expiredRequest.recordEvent();
		}

		// inner-private api (for RpcJmxStatsManager)
		private void recordSuccessfulConnect() {
			successfulConnects.recordEvent();
			connectionActive = true;
		}

		private void recordFailedConnect() {
			failedConnects.recordEvent();
			connectionActive = false;
		}

		private void recordClosedConnect() {
			closedConnects.recordEvent();
			connectionActive = false;
		}

		private EventsCounter getTotalRequests() {
			return totalRequests;
		}

		private EventsCounter getSuccessfulRequests() {
			return successfulRequest;
		}

		private EventsCounter getFailedRequests() {
			return failedRequest;
		}

		private EventsCounter getRejectedRequests() {
			return rejectedRequest;
		}

		private EventsCounter getExpiredRequests() {
			return expiredRequest;
		}

		private StatsCounter getPendingRequests() {
			return pendingRequests;
		}

		private StatsCounter getResponseTimeStats() {
			return responseTimeStats;
		}

		private LastExceptionCounter getLastServerException() {
			return lastServerException;
		}

		public EventsCounter getSuccessfulConnectsStats() {
			return successfulConnects;
		}

		public EventsCounter getFailedConnectsStats() {
			return failedConnects;
		}

		public EventsCounter getClosedConnectsStats() {
			return closedConnects;
		}

		public boolean isConnectionActive() {
			return connectionActive;
		}
	}
}