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
	private boolean monitoring;
	private double smoothingWindow;
	private double smoothingPrecision;
	private CurrentTimeProvider timeProvider;
	private List<RpcClientJmx> rpcClients;

	// stats per connection and per request class
	private Map<InetSocketAddress, RpcConnectionStatsManager> statsPerAddress;
	private Map<Class<?>, ParticularStats> statsPerRequestClass;

	// general stats
	private final StatsCounter pendingRequests;
	private final StatsCounter responseTimeStats;
	private final EventsCounter successfulRequest;
	private final EventsCounter failedRequest;
	private final EventsCounter rejectedRequest;
	private final EventsCounter expiredRequest;
	private final LastExceptionCounter lastRemoteException;

	public RpcJmxStatsManager(List<RpcClientJmx> rpcClients, double smoothingWindow, double smoothingPrecision,
	                          CurrentTimeProvider timeProvider) {
		this.monitoring = false;
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;
		this.timeProvider = timeProvider;
		this.rpcClients = rpcClients;

		this.statsPerAddress = new HashMap<>();
		this.statsPerRequestClass = new HashMap<>();

		this.pendingRequests = new StatsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.responseTimeStats = new StatsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.successfulRequest = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.failedRequest = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.rejectedRequest = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.expiredRequest = new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
		this.lastRemoteException = new LastExceptionCounter("Remote Exception");
	}

	// stats manager api
	public void recordNewRequest(Class<?> requestClass) {
		// if (monitoring == true)   // TODO
		incrementStatsCounter(getRequestClassStats(requestClass).getPendingRequests());
		incrementStatsCounter(pendingRequests);
	}

	public void recordSuccessfulRequest(Class<?> requestClass, int responseTime) {
		preprocessFinishedRequest(requestClass);
		updateResponseTime(requestClass, responseTime);

		successfulRequest.recordEvent();
		getRequestClassStats(requestClass).getSuccessfulRequest().recordEvent();
	}

	public void recordFailedRequest(Class<?> requestClass, Exception exception, Object causedObject, int responseTime) {
		preprocessFinishedRequest(requestClass);
		updateResponseTime(requestClass, responseTime);

		failedRequest.recordEvent();
		getRequestClassStats(requestClass).getFailedRequest().recordEvent();

		lastRemoteException.update(exception, causedObject, timeProvider.currentTimeMillis());
		getRequestClassStats(requestClass).getLastRemoteException()
				.update(exception, causedObject, timeProvider.currentTimeMillis());
	}

	public void recordRejectedRequest(Class<?> requestClass) {
		preprocessFinishedRequest(requestClass);
		rejectedRequest.recordEvent();
		getRequestClassStats(requestClass).getRejectedRequest().recordEvent();
	}

	public void recordExpiredRequest(Class<?> requestClass) {
		preprocessFinishedRequest(requestClass);
		expiredRequest.recordEvent();
		getRequestClassStats(requestClass).getExpiredRequest().recordEvent();
	}

	public RpcConnectionStatsManager getConnectionStatsManager(InetSocketAddress address) {
//		return statsPerAddress.get(address);
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
			rpcClient.setRpcJmxStatsManager(this);
		}
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		for (RpcClientJmx rpcClient : rpcClients) {
			rpcClient.setRpcJmxStatsManager(null);
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
		// TODO
		return null;
	}

	@Override
	public int getConnectionsCount() {
		// TODO
		return 0;
	}

	@Override
	public CompositeData[] getAddressesStats() throws OpenDataException {
		// TODO
		return new CompositeData[0];
	}

	@Override
	public CompositeData[] getRequestClassStats() throws OpenDataException {
		// TODO
		return new CompositeData[0];
	}

	@Override
	public long getTotalSuccessfulRequests() {
		// TODO
		return 1;
	}

	@Override
	public long getTotalPendingRequests() {
		// TODO
		return 0;
	}

	@Override
	public long getTotalRejectedRequests() {
		// TODO
		return 0;
	}

	@Override
	public long getTotalFailedRequests() {
		// TODO
		return 0;
	}

	@Override
	public long getTotalExpiredRequests() {
		// TODO
		return 0;
	}

	@Override
	public int getSuccessfulConnects() {
		// TODO
		return 0;
	}

	@Override
	public int getFailedConnects() {
		// TODO
		return 0;
	}

	@Override
	public int getClosedConnects() {
		// TODO
		return 0;
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

	@Override
	public double getRequestsRate() {
		// TODO
		return 0;
	}

	@Override
	public double getAvgResponseTime() {
		// TODO
		return 0;
	}

	// TODO:  (end of jmx api, that needs to be implemented)















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
		private final StatsCounter pendingRequests;
		private final StatsCounter responseTimeStats;
		private final EventsCounter successfulRequest;
		private final EventsCounter failedRequest;
		private final EventsCounter rejectedRequest;
		private final EventsCounter expiredRequest;
		private final LastExceptionCounter lastRemoteException;
		
		public ParticularStats(double window, double precision, CurrentTimeProvider timeProvider) {
			this.pendingRequests = new StatsCounter(window, precision, timeProvider);
			this.responseTimeStats = new StatsCounter(window, precision, timeProvider);
			this.successfulRequest = new EventsCounter(window, precision, timeProvider);
			this.failedRequest = new EventsCounter(window, precision, timeProvider);
			this.rejectedRequest = new EventsCounter(window, precision, timeProvider);
			this.expiredRequest = new EventsCounter(window, precision, timeProvider);
			this.lastRemoteException = new LastExceptionCounter("Remote Exception");
		}

		public StatsCounter getPendingRequests() {
			return pendingRequests;
		}

		public StatsCounter getResponseTimeStats() {
			return responseTimeStats;
		}

		public EventsCounter getSuccessfulRequest() {
			return successfulRequest;
		}

		public EventsCounter getFailedRequest() {
			return failedRequest;
		}

		public EventsCounter getRejectedRequest() {
			return rejectedRequest;
		}

		public EventsCounter getExpiredRequest() {
			return expiredRequest;
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

		// TODO(vmykhalko): recordValue fields for reconnects count and so on

		public RpcConnectionStatsManager(double window, double precision, CurrentTimeProvider timeProvider) {
			this.timeProvider = timeProvider;

			this.pendingRequests = new StatsCounter(window, precision, timeProvider);
			this.responseTimeStats = new StatsCounter(window, precision, timeProvider);
			this.successfulRequest = new EventsCounter(window, precision, timeProvider);
			this.failedRequest = new EventsCounter(window, precision, timeProvider);
			this.rejectedRequest = new EventsCounter(window, precision, timeProvider);
			this.expiredRequest = new EventsCounter(window, precision, timeProvider);
			this.lastRemoteException = new LastExceptionCounter("Remote Exception");
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

		// private getters for RpcJmxStatsManager usage
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
	}
}