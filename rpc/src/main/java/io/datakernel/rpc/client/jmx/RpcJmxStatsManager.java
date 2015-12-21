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

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

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

	private static final String REQUEST_CLASS_COMPOSITE_DATA_NAME = "Request class stats";
	private static final String ADDRESS_COMPOSITE_DATA_NAME = "Address stats";

	private static final int BLOCKING_QUEUE_CAPACITY = 10000;

	// settings
	private volatile boolean monitoring;    // TODO(vmykhalko): add thread-safety
	private volatile double smoothingWindow;
	private volatile double smoothingPrecision;
	private final List<RpcJmxClient> rpcClients;

	public RpcJmxStatsManager(double smoothingWindow, double smoothingPrecision, List<RpcJmxClient> rpcClients) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;
		this.rpcClients = new ArrayList<>(rpcClients);
	}

	// jmx api
	@Override
	public void startMonitoring() {
		monitoring = true;
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.startMonitoring();
		}
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.stopMonitoring();
		}
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public void resetStats() {
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.reset();
		}
	}

	@Override
	public void resetStats(double smoothingWindow, double smoothingPrecision) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.reset(smoothingWindow, smoothingPrecision);
		}
	}

	@Override
	public CompositeData[] getAddresses() throws OpenDataException {
		List<InetSocketAddress> addresses = getClientsAddresses();

		List<CompositeData> compositeDataList = new ArrayList<>();
		for (InetSocketAddress address : addresses) {
			CompositeData compositeData = CompositeDataBuilder.builder(ADDRESS_COMPOSITE_DATA_NAME)
					.add(ADDRESS_KEY, SimpleType.STRING, address.toString())
					.build();
			compositeDataList.add(compositeData);
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
	}

	@Override
	public int getActiveConnectionsCount() {
		List<Integer> connectionsCountPerClient = getConnectionsCountPerClient();
		int totalConnectionsCount = 0;
		for (int connectionCount : connectionsCountPerClient) {
			totalConnectionsCount += connectionCount;
		}
		return totalConnectionsCount;
	}

	@Override
	public CompositeData[] getAddressesStats() throws OpenDataException {
		List<CompositeData> compositeDataList = new ArrayList<>();
		Map<InetSocketAddress, List<RpcJmxRequestsStatsSet>> addressToGatheredRequestsStats = getGatheredRequestsStatsPerAddress();
		Map<InetSocketAddress, List<RpcJmxConnectsStatsSet>> addressToGatheredConnectsStats = getGatheredConnectsStatsPerAddress();
		List<InetSocketAddress> addresses = getClientsAddresses();
		for (InetSocketAddress address : addresses) {
			CompositeDataBuilder.Builder builder = CompositeDataBuilder.builder(ADDRESS_COMPOSITE_DATA_NAME)
					.add(ADDRESS_KEY, SimpleType.STRING, address.toString());

			List<RpcJmxRequestsStatsSet> requestsStatsSets = addressToGatheredRequestsStats.get(address);
			List<RpcJmxConnectsStatsSet> connectsStatsSets = addressToGatheredConnectsStats.get(address);

			if (requestsStatsSets != null && requestsStatsSets.size() > 0) {
				AggregatedExceptionCounter aggregatedExceptionCounter = aggregateExceptionCounters(requestsStatsSets);
				Throwable lastException = aggregatedExceptionCounter.getLastException();
				builder = builder.add(TOTAL_REQUESTS_KEY, SimpleType.STRING, aggregateTotalRequestsCounters(requestsStatsSets).toString())
						.add(SUCCESSFUL_REQUESTS_KEY, SimpleType.STRING, aggregateSuccessfulRequestsCounters(requestsStatsSets).toString())
						.add(FAILED_REQUESTS_KEY, SimpleType.STRING, aggregateFailedRequestsCounters(requestsStatsSets).toString())
						.add(REJECTED_REQUESTS_KEY, SimpleType.STRING, aggregateRejectedRequestsCounters(requestsStatsSets).toString())
						.add(EXPIRED_REQUESTS_KEY, SimpleType.STRING, aggregateExpiredRequestsCounters(requestsStatsSets).toString())
						.add(RESPONSE_TIME_KEY, SimpleType.STRING, aggregatedResponseTimeCounters(requestsStatsSets).toString())
						.add(LAST_SERVER_EXCEPTION_KEY, SimpleType.STRING,
								lastException != null ? lastException.toString() : "")
						.add(TOTAL_EXCEPTIONS_KEY, SimpleType.STRING,
								Integer.toString(aggregatedExceptionCounter.getTotalExceptions()));
			}

			if (connectsStatsSets != null && connectsStatsSets.size() > 0) {
				builder = builder
						.add(SUCCESSFUL_CONNECTS_KEY, SimpleType.STRING,
								aggregateSuccessfulConnectsCounters(connectsStatsSets).toString())
						.add(FAILED_CONNECTS_KEY, SimpleType.STRING,
								aggregateFailedConnectsCounters(connectsStatsSets).toString())
						.add(CLOSED_CONNECTS_KEY, SimpleType.STRING,
								aggregateClosedConnectsCounters(connectsStatsSets).toString());
			}

			compositeDataList.add(builder.build());
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);
	}

	@Override
	public CompositeData[] getRequestClassesStats() throws OpenDataException {
		List<CompositeData> compositeDataList = new ArrayList<>();
		Map<Class<?>, List<RpcJmxRequestsStatsSet>> classToGatheredStats = getGatheredStatsPerClass();
		for (Class<?> requestClass : classToGatheredStats.keySet()) {
			List<RpcJmxRequestsStatsSet> listOfStats = classToGatheredStats.get(requestClass);
			AggregatedExceptionCounter aggregatedExceptionCounter = aggregateExceptionCounters(listOfStats);
			Throwable lastException = aggregatedExceptionCounter.getLastException();
			CompositeData compositeData = CompositeDataBuilder.builder(REQUEST_CLASS_COMPOSITE_DATA_NAME)
					.add(REQUEST_CLASS_KEY, SimpleType.STRING, requestClass.getName())
					.add(TOTAL_REQUESTS_KEY, SimpleType.STRING, aggregateTotalRequestsCounters(listOfStats).toString())
					.add(SUCCESSFUL_REQUESTS_KEY, SimpleType.STRING, aggregateSuccessfulRequestsCounters(listOfStats).toString())
					.add(FAILED_REQUESTS_KEY, SimpleType.STRING, aggregateFailedRequestsCounters(listOfStats).toString())
					.add(REJECTED_REQUESTS_KEY, SimpleType.STRING, aggregateRejectedRequestsCounters(listOfStats).toString())
					.add(EXPIRED_REQUESTS_KEY, SimpleType.STRING, aggregateExpiredRequestsCounters(listOfStats).toString())
					.add(RESPONSE_TIME_KEY, SimpleType.STRING, aggregatedResponseTimeCounters(listOfStats).toString())
					.add(LAST_SERVER_EXCEPTION_KEY, SimpleType.STRING,
							lastException != null ? lastException.toString() : "")
					.add(TOTAL_EXCEPTIONS_KEY, SimpleType.STRING,
							Integer.toString(aggregatedExceptionCounter.getTotalExceptions()))
					.build();
			compositeDataList.add(compositeData);
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);

	}

	@Override
	public long getTotalRequests() {
		return aggregateTotalRequestsCounters(getClientsGeneralRequestsStats()).getTotalEvents();
	}

	@Override
	public double getTotalRequestsRate() {
		return aggregateTotalRequestsCounters(getClientsGeneralRequestsStats()).getSmoothedRate();
	}

	@Override
	public String getTotalRequestsDetails() {
		return aggregateTotalRequestsCounters(getClientsGeneralRequestsStats()).toString();
	}

	@Override
	public long getSuccessfulRequests() {
		return aggregateSuccessfulRequestsCounters(getClientsGeneralRequestsStats()).getTotalEvents();
	}

	@Override
	public double getSuccessfulRequestsRate() {
		return aggregateSuccessfulRequestsCounters(getClientsGeneralRequestsStats()).getSmoothedRate();
	}

	@Override
	public String getSuccessfulRequestsDetails() {
		return aggregateSuccessfulRequestsCounters(getClientsGeneralRequestsStats()).toString();
	}

	@Override
	public long getFailedOnServerRequests() {
		return aggregateFailedRequestsCounters(getClientsGeneralRequestsStats()).getTotalEvents();
	}

	@Override
	public double getFailedOnServerRequestsRate() {
		return aggregateFailedRequestsCounters(getClientsGeneralRequestsStats()).getSmoothedRate();
	}

	@Override
	public String getFailedOnServerRequestsDetails() {
		return aggregateFailedRequestsCounters(getClientsGeneralRequestsStats()).toString();
	}

	@Override
	public long getRejectedRequests() {
		return aggregateRejectedRequestsCounters(getClientsGeneralRequestsStats()).getTotalEvents();
	}

	@Override
	public double getRejectedRequestsRate() {
		return aggregateRejectedRequestsCounters(getClientsGeneralRequestsStats()).getSmoothedRate();
	}

	@Override
	public String getRejectedRequestsDetails() {
		return aggregateRejectedRequestsCounters(getClientsGeneralRequestsStats()).toString();
	}

	@Override
	public long getExpiredRequests() {
		return aggregateExpiredRequestsCounters(getClientsGeneralRequestsStats()).getTotalEvents();
	}

	@Override
	public double getExpiredRequestsRate() {
		return aggregateExpiredRequestsCounters(getClientsGeneralRequestsStats()).getSmoothedRate();
	}

	@Override
	public String getExpiredRequestsDetails() {
		return aggregateExpiredRequestsCounters(getClientsGeneralRequestsStats()).toString();
	}

	@Override
	public int getSuccessfulConnects() {
		return (int) aggregateSuccessfulConnectsCounters(fetchAllConnectsStatsSets()).getTotalEvents();
	}

	@Override
	public String getSuccessfulConnectsDetails() {
		return aggregateSuccessfulConnectsCounters(fetchAllConnectsStatsSets()).toString();
	}

	@Override
	public int getFailedConnects() {
		return (int) aggregateFailedConnectsCounters(fetchAllConnectsStatsSets()).getTotalEvents();
	}

	@Override
	public String getFailedRequestsDetails() {
		return aggregateFailedConnectsCounters(fetchAllConnectsStatsSets()).toString();
	}

	@Override
	public int getClosedConnects() {
		return (int) aggregateClosedConnectsCounters(fetchAllConnectsStatsSets()).getTotalEvents();
	}

	@Override
	public String getClosedConnectsDetails() {
		return aggregateClosedConnectsCounters(fetchAllConnectsStatsSets()).toString();
	}

	@Override
	public double getAverageResponseTime() {
		return aggregatedResponseTimeCounters(getClientsGeneralRequestsStats()).getSmoothedAverage();
	}

	@Override
	public String getAverageResponseTimeDetails() {
		return aggregatedResponseTimeCounters(getClientsGeneralRequestsStats()).toString();
	}

	@Override
	public String getLastServerException() {
		Throwable lastException = aggregateExceptionCounters(getClientsGeneralRequestsStats()).getLastException();
		return lastException != null ? lastException.toString() : "";
	}

	@Override
	public int getExceptionsCount() {
		return (int) aggregateExceptionCounters(getClientsGeneralRequestsStats()).getTotalExceptions();
	}

	// methods to simplify fetching stats from rpcClients
	private List<RpcJmxRequestsStatsSet> getClientsGeneralRequestsStats() {
		List<RpcJmxRequestsStatsSet> clientsGeneralRequestsStats = new ArrayList<>();
		BlockingQueue<RpcJmxRequestsStatsSet> blockingQueue = new ArrayBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.fetchGeneralRequestsStats(blockingQueue);
		}
		for (int i = 0; i < rpcClients.size(); i++) {
			try {
				clientsGeneralRequestsStats.add(blockingQueue.take());
			} catch (InterruptedException e) {
				propagateInterruptedException(e);
			}
		}
		return clientsGeneralRequestsStats;
	}

	private List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> getClientsConnectsStatsPerAddress() {
		List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> clientsConnectsStats = new ArrayList<>();
		BlockingQueue<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> blockingQueue =
				new ArrayBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.fetchConnectsStatsPerAddress(blockingQueue);
		}
		for (int i = 0; i < rpcClients.size(); i++) {
			try {
				clientsConnectsStats.add(blockingQueue.take());
			} catch (InterruptedException e) {
				propagateInterruptedException(e);
			}
		}
		return clientsConnectsStats;
	}

	private List<Map<Class<?>, RpcJmxRequestsStatsSet>> getClientsRequestsStatsPerClass() {
		List<Map<Class<?>, RpcJmxRequestsStatsSet>> clientsStatsPerClass = new ArrayList<>();
		BlockingQueue<Map<Class<?>, RpcJmxRequestsStatsSet>> blockingQueue =
				new ArrayBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.fetchRequestsStatsPerClass(blockingQueue);
		}
		for (int i = 0; i < rpcClients.size(); i++) {
			try {
				clientsStatsPerClass.add(blockingQueue.take());
			} catch (InterruptedException e) {
				propagateInterruptedException(e);
			}
		}
		return clientsStatsPerClass;
	}

	private List<Map<InetSocketAddress, RpcJmxRequestsStatsSet>> getClientsRequestsStatsPerAddress() {
		List<Map<InetSocketAddress, RpcJmxRequestsStatsSet>> clientsAddressesStats = new ArrayList<>();
		BlockingQueue<Map<InetSocketAddress, RpcJmxRequestsStatsSet>> blockingQueue =
				new ArrayBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.fetchRequestStatsPerAddress(blockingQueue);
		}
		for (int i = 0; i < rpcClients.size(); i++) {
			try {
				clientsAddressesStats.add(blockingQueue.take());
			} catch (InterruptedException e) {
				propagateInterruptedException(e);
			}
		}
		return clientsAddressesStats;
	}

	private List<InetSocketAddress> getClientsAddresses() {
		List<InetSocketAddress> allClientsAddresses = new ArrayList<>();
		BlockingQueue<List<InetSocketAddress>> blockingQueue =
				new ArrayBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.fetchAddresses(blockingQueue);
		}
		for (int i = 0; i < rpcClients.size(); i++) {
			try {
				List<InetSocketAddress> clientAddresses = blockingQueue.take();
				for (InetSocketAddress address : clientAddresses) {
					if (!allClientsAddresses.contains(address)) {
						allClientsAddresses.add(address);
					}
				}
			} catch (InterruptedException e) {
				propagateInterruptedException(e);
			}
		}
		return allClientsAddresses;
	}

	private List<Integer> getConnectionsCountPerClient() {
		List<Integer> connectionCountPerClient = new ArrayList<>();
		BlockingQueue<Integer> blockingQueue =
				new ArrayBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
		for (RpcJmxClient rpcClient : rpcClients) {
			rpcClient.fetchActiveConnectionsCount(blockingQueue);
		}
		for (int i = 0; i < rpcClients.size(); i++) {
			try {
				int connectionCount = blockingQueue.take();
				connectionCountPerClient.add(connectionCount);
			} catch (InterruptedException e) {
				propagateInterruptedException(e);
			}
		}
		return getConnectionsCountPerClient();
	}

	// aggregating methods
	private AggregatedEventsCounter aggregateTotalRequestsCounters(List<RpcJmxRequestsStatsSet> stats) {
		List<EventsCounter> totalRequestsCounters = new ArrayList<>(stats.size());
		for (RpcJmxRequestsStatsSet stat : stats) {
			totalRequestsCounters.add(stat.getTotalRequests());
		}
		return AggregatedEventsCounter.aggregate(totalRequestsCounters);
	}

	private AggregatedEventsCounter aggregateSuccessfulRequestsCounters(List<RpcJmxRequestsStatsSet> stats) {
		List<EventsCounter> successfulRequestsCounters = new ArrayList<>(stats.size());
		for (RpcJmxRequestsStatsSet stat : stats) {
			successfulRequestsCounters.add(stat.getSuccessfulRequests());
		}
		return AggregatedEventsCounter.aggregate(successfulRequestsCounters);
	}

	private AggregatedEventsCounter aggregateFailedRequestsCounters(List<RpcJmxRequestsStatsSet> stats) {
		List<EventsCounter> failedRequestsCounters = new ArrayList<>(stats.size());
		for (RpcJmxRequestsStatsSet stat : stats) {
			failedRequestsCounters.add(stat.getFailedRequests());
		}
		return AggregatedEventsCounter.aggregate(failedRequestsCounters);
	}

	private AggregatedEventsCounter aggregateExpiredRequestsCounters(List<RpcJmxRequestsStatsSet> stats) {
		List<EventsCounter> expiredRequestsCounters = new ArrayList<>(stats.size());
		for (RpcJmxRequestsStatsSet stat : stats) {
			expiredRequestsCounters.add(stat.getExpiredRequests());
		}
		return AggregatedEventsCounter.aggregate(expiredRequestsCounters);
	}

	private AggregatedEventsCounter aggregateRejectedRequestsCounters(List<RpcJmxRequestsStatsSet> stats) {
		List<EventsCounter> rejectedRequestsCounters = new ArrayList<>(stats.size());
		for (RpcJmxRequestsStatsSet stat : stats) {
			rejectedRequestsCounters.add(stat.getRejectedRequests());
		}
		return AggregatedEventsCounter.aggregate(rejectedRequestsCounters);
	}

	private AggregatedStatsCounter aggregatedResponseTimeCounters(List<RpcJmxRequestsStatsSet> stats) {
		List<StatsCounter> responseTimeCounters = new ArrayList<>(stats.size());
		for (RpcJmxRequestsStatsSet stat : stats) {
			responseTimeCounters.add(stat.getResponseTimeStats());
		}
		return AggregatedStatsCounter.aggregate(responseTimeCounters);
	}

	private AggregatedExceptionCounter aggregateExceptionCounters(List<RpcJmxRequestsStatsSet> stats) {
		List<LastExceptionCounter> exceptionCounters = new ArrayList<>(stats.size());
		for (RpcJmxRequestsStatsSet stat : stats) {
			exceptionCounters.add(stat.getLastServerExceptionCounter());
		}
		return AggregatedExceptionCounter.aggregate(exceptionCounters);
	}

	private AggregatedEventsCounter aggregateSuccessfulConnectsCounters(List<RpcJmxConnectsStatsSet> stats) {
		List<EventsCounter> successfulConnectCounters = new ArrayList<>(stats.size());
		for (RpcJmxConnectsStatsSet stat : stats) {
			successfulConnectCounters.add(stat.getSuccessfulConnects());
		}
		return AggregatedEventsCounter.aggregate(successfulConnectCounters);
	}

	private AggregatedEventsCounter aggregateFailedConnectsCounters(List<RpcJmxConnectsStatsSet> stats) {
		List<EventsCounter> failedConnectCounters = new ArrayList<>(stats.size());
		for (RpcJmxConnectsStatsSet stat : stats) {
			failedConnectCounters.add(stat.getFailedConnects());
		}
		return AggregatedEventsCounter.aggregate(failedConnectCounters);
	}

	private AggregatedEventsCounter aggregateClosedConnectsCounters(List<RpcJmxConnectsStatsSet> stats) {
		List<EventsCounter> closedConnectCounters = new ArrayList<>(stats.size());
		for (RpcJmxConnectsStatsSet stat : stats) {
			closedConnectCounters.add(stat.getClosedConnects());
		}
		return AggregatedEventsCounter.aggregate(closedConnectCounters);
	}

	// methods for regrouping / reducing / gathering
	private Map<Class<?>, List<RpcJmxRequestsStatsSet>> getGatheredStatsPerClass() {
		List<Map<Class<?>, RpcJmxRequestsStatsSet>> allClientsStats = getClientsRequestsStatsPerClass();
		Map<Class<?>, List<RpcJmxRequestsStatsSet>> classToGatheredStatsList = new HashMap<>();
		for (Map<Class<?>, RpcJmxRequestsStatsSet> singleClientStatsPerClass : allClientsStats) {
			for (Class<?> requestClass : singleClientStatsPerClass.keySet()) {
				if (!classToGatheredStatsList.containsKey(requestClass)) {
					classToGatheredStatsList.put(requestClass, new ArrayList<RpcJmxRequestsStatsSet>());
				}
				List<RpcJmxRequestsStatsSet> listForRequestClass = classToGatheredStatsList.get(requestClass);
				RpcJmxRequestsStatsSet currentStats = singleClientStatsPerClass.get(requestClass);
				listForRequestClass.add(currentStats);
			}
		}
		return classToGatheredStatsList;
	}

	private Map<InetSocketAddress, List<RpcJmxRequestsStatsSet>> getGatheredRequestsStatsPerAddress() {
		List<Map<InetSocketAddress, RpcJmxRequestsStatsSet>> allClientsRequestsStats = getClientsRequestsStatsPerAddress();
		Map<InetSocketAddress, List<RpcJmxRequestsStatsSet>> addressToGatheredRequestsStatsList = new HashMap<>();
		for (Map<InetSocketAddress, RpcJmxRequestsStatsSet> singleClientRequestsStatsPerAddress : allClientsRequestsStats) {
			for (InetSocketAddress address : singleClientRequestsStatsPerAddress.keySet()) {
				if (!addressToGatheredRequestsStatsList.containsKey(address)) {
					addressToGatheredRequestsStatsList.put(address, new ArrayList<RpcJmxRequestsStatsSet>());
				}
				List<RpcJmxRequestsStatsSet> listForAddress = addressToGatheredRequestsStatsList.get(address);
				RpcJmxRequestsStatsSet currentRequestsStats = singleClientRequestsStatsPerAddress.get(address);
				listForAddress.add(currentRequestsStats);
			}
		}
		return addressToGatheredRequestsStatsList;
	}

	private Map<InetSocketAddress, List<RpcJmxConnectsStatsSet>> getGatheredConnectsStatsPerAddress() {
		List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> allClientsConnectsStats = getClientsConnectsStatsPerAddress();
		Map<InetSocketAddress, List<RpcJmxConnectsStatsSet>> addressToGatheredConnectsStatsList = new HashMap<>();
		for (Map<InetSocketAddress, RpcJmxConnectsStatsSet> singleClientConnectsStatsPerAddress : allClientsConnectsStats) {
			for (InetSocketAddress address : singleClientConnectsStatsPerAddress.keySet()) {
				if (!addressToGatheredConnectsStatsList.containsKey(address)) {
					addressToGatheredConnectsStatsList.put(address, new ArrayList<RpcJmxConnectsStatsSet>());
				}
				List<RpcJmxConnectsStatsSet> listForAddress = addressToGatheredConnectsStatsList.get(address);
				RpcJmxConnectsStatsSet currentRequestsStats = singleClientConnectsStatsPerAddress.get(address);
				listForAddress.add(currentRequestsStats);
			}
		}
		return addressToGatheredConnectsStatsList;
	}

	// other helpers
	private List<RpcJmxConnectsStatsSet> fetchAllConnectsStatsSets() {
		List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> clientsStats = getClientsConnectsStatsPerAddress();
		List<RpcJmxConnectsStatsSet> connectsStatsSets = new ArrayList<>();
		for (Map<InetSocketAddress, RpcJmxConnectsStatsSet> clientConnectsStats : clientsStats) {
			for (RpcJmxConnectsStatsSet statsSet : clientConnectsStats.values()) {
				connectsStatsSets.add(statsSet);
			}
		}
		return connectsStatsSets;
	}

	private static void propagateInterruptedException(InterruptedException e) {
		throw new RuntimeException(e);
	}

	// classes for aggregation
	private static class AggregatedStatsCounter {

		private double smoothedAverage;
		private double smoothedStandardDeviation;
		private int min;
		private int max;
		// smoothed min is chosen from client, which has minimum value of smoothed min (it is not properly aggregated)
		private double smoothedMin;
		// smoothed max is chosen from client, which has maximim value of smoothed max (it is not properly aggregated)
		private double smoothedMax;
		// actually it may not be last value of all clients, but last value of specific client
		private int last;

		private AggregatedStatsCounter(double smoothedAverage, double smoothedStandardDeviation, int min, int max,
		                               double smoothedMin, double smoothedMax, int last) {
			this.smoothedAverage = smoothedAverage;
			this.smoothedStandardDeviation = smoothedStandardDeviation;
			this.min = min;
			this.max = max;
			this.smoothedMin = smoothedMin;
			this.smoothedMax = smoothedMax;
			this.last = last;
		}

		public static AggregatedStatsCounter aggregate(List<StatsCounter> counters) {
			double totalSmoothedAverage = 0;
			double totalStdDeviation = 0;
			int min = Integer.MAX_VALUE;
			int max = Integer.MIN_VALUE;
			double smoothedMin = min;
			double smoothedMax = max;

			for (StatsCounter counter : counters) {
				totalSmoothedAverage += counter.getSmoothedAverage();
				totalStdDeviation += counter.getSmoothedStandardDeviation();
				int counterMin = counter.getMinValue();
				if (counterMin < min) {
					min = counterMin;
				}
				int counterMax = counter.getMaxValue();
				if (counterMax > max) {
					max = counterMax;
				}
				double counterSmoothedMin = counter.getSmoothedMin();
				if (counterSmoothedMin < smoothedMin) {
					smoothedMin = counterSmoothedMin;
				}
				double counterSmoothedMax = counter.getSmoothedMax();
				if (counterSmoothedMax > smoothedMax) {
					smoothedMax = counterSmoothedMax;
				}
			}

			double smoothedAverage = 0;
			double smoothedStdDeviation = 0;
			int last = 0;
			if (counters.size() > 0) {
				smoothedAverage = totalSmoothedAverage / counters.size();
				smoothedStdDeviation = totalStdDeviation / counters.size();
				last = counters.get(counters.size() - 1).getLastValue();
			}

			return new AggregatedStatsCounter(smoothedAverage, smoothedStdDeviation, min, max,
					smoothedMin, smoothedMax, last);
		}

		public double getSmoothedAverage() {
			return smoothedAverage;
		}

		public double getSmoothedStandardDeviation() {
			return smoothedStandardDeviation;
		}

		public int getMin() {
			return min;
		}

		public int getMax() {
			return max;
		}

		public double getSmoothedMin() {
			return smoothedMin;
		}

		public double getSmoothedMax() {
			return smoothedMax;
		}

		public int getLast() {
			return last;
		}

		@Override
		public String toString() {
			return String.format("%.2f±%.3f   min: %d   max: %d   last: %d   smoothedMin: %.2f   smoothedMax: %.2f",
					getSmoothedAverage(), getSmoothedStandardDeviation(), getMin(), getMax(), getLast(),
					getSmoothedMin(), getSmoothedMax());
		}
	}

	private static class AggregatedEventsCounter {

		private long totalEvents;
		private double smoothedRate;
		// smoothed min is chosen from client, which has minimum value of smoothed min (it is not properly aggregated)
		private double smoothedMinRate;
		// smoothed max is chosen from client, which has maximim value of smoothed max (it is not properly aggregated)
		private double smoothedMaxRate;

		public AggregatedEventsCounter(long totalEvents, double smoothedRate, double smoothedMinRate, double smoothedMaxRate) {
			this.totalEvents = totalEvents;
			this.smoothedRate = smoothedRate;
			this.smoothedMinRate = smoothedMinRate;
			this.smoothedMaxRate = smoothedMaxRate;
		}

		public static AggregatedEventsCounter aggregate(List<EventsCounter> counters) {
			long totalEvents = 0;
			double aggregatedSmoothedRate = 0;
			double smoothedMin = Integer.MAX_VALUE;
			double smoothedMax = Integer.MIN_VALUE;

			for (EventsCounter counter : counters) {
				totalEvents += counter.getEventsCount();
				aggregatedSmoothedRate += counter.getSmoothedRate();
				double counterSmoothedMin = counter.getSmoothedMinRate();
				if (counterSmoothedMin < smoothedMin) {
					smoothedMin = counterSmoothedMin;
				}
				double counterSmoothedMax = counter.getSmoothedMaxRate();
				if (counterSmoothedMax > smoothedMax) {
					smoothedMax = counterSmoothedMax;
				}

			}
			return new AggregatedEventsCounter(totalEvents, aggregatedSmoothedRate, smoothedMin, smoothedMax);
		}

		public long getTotalEvents() {
			return totalEvents;
		}

		public double getSmoothedRate() {
			return smoothedRate;
		}

		public double getSmoothedMinRate() {
			return smoothedMinRate;
		}

		public double getSmoothedMaxRate() {
			return smoothedMaxRate;
		}

		@Override
		public String toString() {
			return String.format("total: %d   smoothedRate: %.4f   smoothedMinRate: %.4f   smoothedMaxRate: %.4f",
					getTotalEvents(), getSmoothedRate(), getSmoothedMinRate(), getSmoothedMaxRate());
		}
	}

	private static class AggregatedExceptionCounter {

		private int totalExceptions;
		// actually it may not be last exception of all clients, but last exception of specific client
		private Throwable lastException;

		public AggregatedExceptionCounter(int totalExceptions, Throwable lastException) {
			this.totalExceptions = totalExceptions;
			this.lastException = lastException;
		}

		public static AggregatedExceptionCounter aggregate(List<LastExceptionCounter> counters) {
			int totalExceptions = 0;

			for (LastExceptionCounter counter : counters) {
				totalExceptions += counter.getTotal();
			}
			Throwable lastException = null;
			if (counters.size() > 0) {
				lastException = counters.get(counters.size() - 1).getLastException();
			}
			return new AggregatedExceptionCounter(totalExceptions, lastException);
		}

		public int getTotalExceptions() {
			return totalExceptions;
		}

		public Throwable getLastException() {
			return lastException;
		}
	}
}