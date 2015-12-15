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

import io.datakernel.jmx.EventsCounter;
import io.datakernel.jmx.LastExceptionCounter;
import io.datakernel.jmx.StatsCounter;
import io.datakernel.time.CurrentTimeProvider;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class RpcJmxStatsManager implements RpcJmxStatsManagerMBean {

	// settings
	private volatile boolean monitoring;    // TODO(vmykhalko): add thread-safety
	private double smoothingWindow;
	private double smoothingPrecision;
	private CurrentTimeProvider timeProvider;
	private List<RpcClientJmx> rpcClients;

	// stats per connection and per request class
	private Map<InetSocketAddress, RpcConnectionStatsManager> statsPerAddress;
	private Map<Class<?>, ParticularStats> statsPerRequestClass;

	// general stats
	private final EventsCounter totalRequests;
	private final EventsCounter successfulRequests;
	private final EventsCounter failedRequests;
	private final EventsCounter rejectedRequests;
	private final EventsCounter expiredRequests;
	private final StatsCounter pendingRequests;
	private final StatsCounter responseTimeStats;
	private final LastExceptionCounter lastRemoteException;
	private final EventsCounter successfulConnects;
	private final EventsCounter failedConnects;
	private final EventsCounter closedConnects;

	public RpcJmxStatsManager(List<RpcClientJmx> rpcClients, double smoothingWindow, double smoothingPrecision,
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
		this.lastRemoteException = new LastExceptionCounter("Remote Exception");

		this.successfulConnects = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.failedConnects = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.closedConnects = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
	}

	// stats manager api
	public void recordNewRequest(Class<?> requestClass) {
		// TODO(vmykhalko): is it needed to check whether monitoring flag is true
		ParticularStats requestClassStats = getRequestClassStats(requestClass);

		totalRequests.recordEvent();
		requestClassStats.getTotalRequests().recordEvent();

		incrementStatsCounter(requestClassStats.getPendingRequests());
		incrementStatsCounter(pendingRequests);
	}

	public void recordSuccessfulRequest(Class<?> requestClass, int responseTime) {
		preprocessFinishedRequest(requestClass);
		updateResponseTime(requestClass, responseTime);

		successfulRequests.recordEvent();
		getRequestClassStats(requestClass).getSuccessfulRequests().recordEvent();
	}

	public void recordFailedRequest(Class<?> requestClass, Exception exception, Object causedObject, int responseTime) {
		preprocessFinishedRequest(requestClass);
		updateResponseTime(requestClass, responseTime);

		failedRequests.recordEvent();
		getRequestClassStats(requestClass).getFailedRequests().recordEvent();

		lastRemoteException.update(exception, causedObject, timeProvider.currentTimeMillis());
		getRequestClassStats(requestClass).getLastRemoteException()
				.update(exception, causedObject, timeProvider.currentTimeMillis());
	}

	public void recordRejectedRequest(Class<?> requestClass) {
		preprocessFinishedRequest(requestClass);
		rejectedRequests.recordEvent();
		getRequestClassStats(requestClass).getRejectedRequests().recordEvent();
	}

	public void recordExpiredRequest(Class<?> requestClass) {
		preprocessFinishedRequest(requestClass);
		expiredRequests.recordEvent();
		getRequestClassStats(requestClass).getExpiredRequests().recordEvent();
	}

	public void recordSuccessfulConnect(InetSocketAddress address) {
		successfulConnects.recordEvent();
		getConnectionStatsManager(address).recordSuccessfulConnect();
	}

	public void recordFailedConnect(InetSocketAddress address) {
		failedConnects.recordEvent();
		getConnectionStatsManager(address).recordFailedConnect();
	}

	public void recordClosedConnect(InetSocketAddress address) {
		closedConnects.recordEvent();
		getConnectionStatsManager(address).recordClosedConnect();
	}

	// TODO(vmykhalko): maybe it will be better to set addresses only once in constructor / or in resetStats() ?
	public RpcConnectionStatsManager getConnectionStatsManager(InetSocketAddress address) {
		if (!statsPerAddress.containsKey(address)) {
			statsPerAddress.put(address, new RpcConnectionStatsManager(smoothingWindow, smoothingPrecision, timeProvider));
		}
		return statsPerAddress.get(address);
	}









	// jmx api
	// TODO(vmykhalko):

	@Override
	public void startMonitoring() {
		monitoring = true;
		for (RpcClientJmx rpcClient : rpcClients) {
			rpcClient.startMonitoring(this);
		}
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		for (RpcClientJmx rpcClient : rpcClients) {
			rpcClient.stopMonitoring();
		}
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public void resetStats() {
		// TODO
	}

	@Override
	public void resetStats(double smoothingWindow, double smoothingPrecision) {
		// TODO
	}

	@Override
	public String getAddresses() {
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
	public int getActiveConnectionsCount() {
		int activeConnections = 0;
		for (RpcConnectionStatsManager rpcConnectionStatsManager : statsPerAddress.values()) {
			if (rpcConnectionStatsManager.isConnectionActive()) {
				++activeConnections;
			}
		}
		return activeConnections;
	}

	@Override
	public CompositeData[] getAddressesStats() throws OpenDataException {
		// TODO
		return new CompositeData[0];
	}

	@Override
	public CompositeData[] getRequestClassesStats() throws OpenDataException {
		// TODO
		return new CompositeData[0];
	}

	@Override
	public String getTotalRequestsStats() {
		return totalRequests.toString();
	}

	@Override
	public String getSuccessfulRequestsStats() {
		return successfulRequests.toString();
	}

	@Override
	public String getFailedRequestsStats() {
		return failedRequests.toString();
	}

	@Override
	public String getRejectedRequestsStats() {
		return rejectedRequests.toString();
	}

	@Override
	public String getExpiredRequestsStats() {
		return expiredRequests.toString();
	}

	@Override
	public String getPendingRequestsStats() {
		return pendingRequests.toString();
	}


	@Override
	public String getSuccessfulConnectsStats() {
		return successfulConnects.toString();
	}

	@Override
	public String getFailedConnectsStats() {
		return failedConnects.toString();
	}

	@Override
	public String getClosedConnectsStats() {
		return closedConnects.toString();
	}

	@Override
	public String getAverageResponseTimeStats() {
		return responseTimeStats.toString();
	}

	@Override
	public CompositeData getLastServerException() {
		// TODO
		return null;
	}

	@Override
	public int getExceptionsCount() {
		// TODO
		return 0;
	}











//
//	// helper methods
//	private ParticularStats getConnectionStats(RpcClientConnection connection) {
//		if (!statsPerAddress.containsKey(connection)) {
//			statsPerAddress.put(connection, new ParticularStats(smoothingWindow, smoothingPrecision, timeProvider));
//		}
//		return statsPerAddress.get(connection);
//	}

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
		private final LastExceptionCounter lastRemoteException;
		
		public ParticularStats(double window, double precision, CurrentTimeProvider timeProvider) {
			this.totalRequests = new EventsCounter(window, precision, timeProvider);
			this.successfulRequests = new EventsCounter(window, precision, timeProvider);
			this.failedRequests = new EventsCounter(window, precision, timeProvider);
			this.rejectedRequests = new EventsCounter(window, precision, timeProvider);
			this.expiredRequests = new EventsCounter(window, precision, timeProvider);
			this.pendingRequests = new StatsCounter(window, precision, timeProvider);
			this.responseTimeStats = new StatsCounter(window, precision, timeProvider);
			this.lastRemoteException = new LastExceptionCounter("Remote Exception");
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

		public LastExceptionCounter getLastRemoteException() {
			return lastRemoteException;
		}
	}

	public static class RpcConnectionStatsManager {
		private final CurrentTimeProvider timeProvider;

		private final StatsCounter pendingRequests;
		private final StatsCounter responseTimeStats;
		private final EventsCounter successfulRequest;
		private final EventsCounter failedRequest;
		private final EventsCounter rejectedRequest;
		private final EventsCounter expiredRequest;
		private final LastExceptionCounter lastRemoteException;

		private final EventsCounter successfulConnects;
		private final EventsCounter failedConnects;
		private final EventsCounter closedConnects;

		private volatile boolean connectionActive;

		public RpcConnectionStatsManager(double window, double precision, CurrentTimeProvider timeProvider) {
			this.timeProvider = timeProvider;

			this.pendingRequests = new StatsCounter(window, precision, timeProvider);
			this.responseTimeStats = new StatsCounter(window, precision, timeProvider);
			this.successfulRequest = new EventsCounter(window, precision, timeProvider);
			this.failedRequest = new EventsCounter(window, precision, timeProvider);
			this.rejectedRequest = new EventsCounter(window, precision, timeProvider);
			this.expiredRequest = new EventsCounter(window, precision, timeProvider);
			this.lastRemoteException = new LastExceptionCounter("Remote Exception");

			this.successfulConnects = new EventsCounter(window, precision, timeProvider);
			this.failedConnects = new EventsCounter(window, precision, timeProvider);
			this.closedConnects = new EventsCounter(window, precision, timeProvider);

			this.connectionActive = false;
		}

		// public api
		public void recordNewRequest() {
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
			lastRemoteException.update(exception, causedObject, timeProvider.currentTimeMillis());
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

		private StatsCounter getPendingRequests() {
			return pendingRequests;
		}

		private StatsCounter getResponseTimeStats() {
			return responseTimeStats;
		}

		private EventsCounter getSuccessfulRequest() {
			return successfulRequest;
		}

		private EventsCounter getFailedRequest() {
			return failedRequest;
		}

		private EventsCounter getRejectedRequest() {
			return rejectedRequest;
		}

		private EventsCounter getExpiredRequest() {
			return expiredRequest;
		}

		private LastExceptionCounter getLastRemoteException() {
			return lastRemoteException;
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