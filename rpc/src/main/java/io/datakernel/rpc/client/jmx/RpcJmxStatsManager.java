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
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.time.CurrentTimeProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class RpcJmxStatsManager implements RpcJmxStatsManagerMBean {

	// settings
	private boolean monitoring;
	private double smoothingWindow;
	private double smoothingPrecision;
	private CurrentTimeProvider timeProvider;
	private List<RpcClient> rpcClients;

	// stats per connection and per request class
	private Map<RpcClientConnection, ParticularStats> statsPerConnection;
	private Map<Class<?>, ParticularStats> statsPerRequestClass;

	// general stats
	private final StatsCounter pendingRequests;
	private final StatsCounter responseTimeStats;
	private final EventsCounter successfulRequest;
	private final EventsCounter failedRequest;
	private final EventsCounter rejectedRequest;
	private final EventsCounter expiredRequest;
	private final LastExceptionCounter lastRemoteException;

	public RpcJmxStatsManager(List<RpcClient> rpcClients, double smoothingWindow, double smoothingPrecision,
	                          CurrentTimeProvider timeProvider) {
		this.monitoring = false;
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;
		this.timeProvider = timeProvider;
		this.rpcClients = rpcClients;

		this.statsPerConnection = new HashMap<>();
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
	public void recordNewRequest(RpcClientConnection connection, Class<?> requestClass) {
		incrementStatsCounter(getConnectionStats(connection).getPendingRequests());
		incrementStatsCounter(getRequestClassStats(requestClass).getPendingRequests());
		incrementStatsCounter(pendingRequests);
	}

	public void recordSuccessfulRequest(RpcClientConnection connection, Class<?> requestClass, int responseTime) {
		preprocessFinishedRequest(connection, requestClass);
		updateResponseTime(connection, requestClass, responseTime);

		getConnectionStats(connection).getSuccessfulRequest().recordEvent();
		getRequestClassStats(requestClass).getSuccessfulRequest().recordEvent();
		successfulRequest.recordEvent();
	}

	public void recordFailedRequest(RpcClientConnection connection, Class<?> requestClass,
	                                Exception exception, Object casedObject, int responseTime) {
		preprocessFinishedRequest(connection, requestClass);
		updateResponseTime(connection, requestClass, responseTime);

		getConnectionStats(connection).getFailedRequest().recordEvent();
		getRequestClassStats(requestClass).getFailedRequest().recordEvent();
		failedRequest.recordEvent();

		getConnectionStats(connection).getLastRemoteException()
				.update(exception, casedObject, timeProvider.currentTimeMillis());
		getRequestClassStats(requestClass).getLastRemoteException()
				.update(exception, casedObject, timeProvider.currentTimeMillis());
		lastRemoteException.update(exception, casedObject, timeProvider.currentTimeMillis());
	}

	public void recordRejectedRequest(RpcClientConnection connection, Class<?> requestClass) {
		preprocessFinishedRequest(connection, requestClass);

		getConnectionStats(connection).getRejectedRequest().recordEvent();
		getRequestClassStats(requestClass).getRejectedRequest().recordEvent();
		rejectedRequest.recordEvent();
	}

	public void recordExpiredRequest(RpcClientConnection connection, Class<?> requestClass) {
		preprocessFinishedRequest(connection, requestClass);

		getConnectionStats(connection).getExpiredRequest().recordEvent();
		getRequestClassStats(requestClass).getExpiredRequest().recordEvent();
		expiredRequest.recordEvent();
	}





	// jmx api
	// TODO(vmykhalko):





	// helper methods
	private ParticularStats getConnectionStats(RpcClientConnection connection) {
		if (!statsPerConnection.containsKey(connection)) {
			statsPerConnection.put(connection, new ParticularStats(smoothingWindow, smoothingPrecision, timeProvider));
		}
		return statsPerConnection.get(connection);
	}

	private ParticularStats getRequestClassStats(Class<?> requestClass) {
		if (!statsPerRequestClass.containsKey(requestClass)) {
			statsPerRequestClass.put(requestClass, new ParticularStats(smoothingWindow, smoothingPrecision, timeProvider));
		}
		return statsPerRequestClass.get(requestClass);
	}

	private static void incrementStatsCounter(StatsCounter statsCounter) {
		statsCounter.add(statsCounter.getLastValue() + 1);
	}

	private static void decrementStatsCounter(StatsCounter statsCounter) {
		statsCounter.add(statsCounter.getLastValue() - 1);
	}

	private void preprocessFinishedRequest(RpcClientConnection connection, Class<?> requestClass) {
		decrementStatsCounter(getConnectionStats(connection).getPendingRequests());
		decrementStatsCounter(getRequestClassStats(requestClass).getPendingRequests());
		decrementStatsCounter(pendingRequests);
	}

	private void updateResponseTime(RpcClientConnection connection, Class<?> requestClass, int responseTime) {
		getConnectionStats(connection).getResponseTime().add(responseTime);
		getRequestClassStats(requestClass).getResponseTime().add(responseTime);
		responseTimeStats.add(responseTime);
	}

	private static class ParticularStats {
		private final StatsCounter pendingRequests;
		private final StatsCounter responseTime;
		private final EventsCounter successfulRequest;
		private final EventsCounter failedRequest;
		private final EventsCounter rejectedRequest;
		private final EventsCounter expiredRequest;
		private final LastExceptionCounter lastRemoteException;
		
		public ParticularStats(double window, double precision, CurrentTimeProvider timeProvider) {
			this.pendingRequests = new StatsCounter(window, precision, timeProvider);
			this.responseTime = new StatsCounter(window, precision, timeProvider);
			this.successfulRequest = new EventsCounter(window, precision, timeProvider);
			this.failedRequest = new EventsCounter(window, precision, timeProvider);
			this.rejectedRequest = new EventsCounter(window, precision, timeProvider);
			this.expiredRequest = new EventsCounter(window, precision, timeProvider);
			this.lastRemoteException = new LastExceptionCounter("Remote Exception");
		}

		public StatsCounter getPendingRequests() {
			return pendingRequests;
		}

		public StatsCounter getResponseTime() {
			return responseTime;
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

	public static class ConnectionStatsManager {

	}
}