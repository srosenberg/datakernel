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
		int totalConnectionsCount = 0;
		for (RpcJmxClient rpcClient : rpcClients) {
			totalConnectionsCount += rpcClient.getActiveConnectionsCount();
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
				LastExceptionCounter.Accumulator exeptionCounterAccumulator = aggregateExceptionCounters(requestsStatsSets);
				Throwable lastException = exeptionCounterAccumulator.getLastException();
				builder = builder.add(TOTAL_REQUESTS_KEY, SimpleType.STRING, aggregateTotalRequestsCounters(requestsStatsSets).toString())
						.add(SUCCESSFUL_REQUESTS_KEY, SimpleType.STRING, aggregateSuccessfulRequestsCounters(requestsStatsSets).toString())
						.add(FAILED_REQUESTS_KEY, SimpleType.STRING, aggregateFailedRequestsCounters(requestsStatsSets).toString())
						.add(REJECTED_REQUESTS_KEY, SimpleType.STRING, aggregateRejectedRequestsCounters(requestsStatsSets).toString())
						.add(EXPIRED_REQUESTS_KEY, SimpleType.STRING, aggregateExpiredRequestsCounters(requestsStatsSets).toString())
						.add(RESPONSE_TIME_KEY, SimpleType.STRING, aggregatedResponseTimeCounters(requestsStatsSets).toString())
						.add(LAST_SERVER_EXCEPTION_KEY, SimpleType.STRING,
								lastException != null ? lastException.toString() : "")
						.add(TOTAL_EXCEPTIONS_KEY, SimpleType.STRING,
								Integer.toString(exeptionCounterAccumulator.getTotalExceptions()));
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
			LastExceptionCounter.Accumulator lastExceptionAccumulator = aggregateExceptionCounters(listOfStats);
			Throwable lastException = lastExceptionAccumulator.getLastException();
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
							Integer.toString(lastExceptionAccumulator.getTotalExceptions()))
					.build();
			compositeDataList.add(compositeData);
		}
		return compositeDataList.toArray(new CompositeData[compositeDataList.size()]);

	}

	@Override
	public long getTotalRequests() {
		return aggregateTotalRequestsCounters(collectGeneralRequestsStatsFromAllClients()).getTotalEvents();
	}

	@Override
	public double getTotalRequestsRate() {
		return aggregateTotalRequestsCounters(collectGeneralRequestsStatsFromAllClients()).getSmoothedRate();
	}

	@Override
	public String getTotalRequestsDetails() {
		return aggregateTotalRequestsCounters(collectGeneralRequestsStatsFromAllClients()).toString();
	}

	@Override
	public long getSuccessfulRequests() {
		return aggregateSuccessfulRequestsCounters(collectGeneralRequestsStatsFromAllClients()).getTotalEvents();
	}

	@Override
	public double getSuccessfulRequestsRate() {
		return aggregateSuccessfulRequestsCounters(collectGeneralRequestsStatsFromAllClients()).getSmoothedRate();
	}

	@Override
	public String getSuccessfulRequestsDetails() {
		return aggregateSuccessfulRequestsCounters(collectGeneralRequestsStatsFromAllClients()).toString();
	}

	@Override
	public long getFailedOnServerRequests() {
		return aggregateFailedRequestsCounters(collectGeneralRequestsStatsFromAllClients()).getTotalEvents();
	}

	@Override
	public double getFailedOnServerRequestsRate() {
		return aggregateFailedRequestsCounters(collectGeneralRequestsStatsFromAllClients()).getSmoothedRate();
	}

	@Override
	public String getFailedOnServerRequestsDetails() {
		return aggregateFailedRequestsCounters(collectGeneralRequestsStatsFromAllClients()).toString();
	}

	@Override
	public long getRejectedRequests() {
		return aggregateRejectedRequestsCounters(collectGeneralRequestsStatsFromAllClients()).getTotalEvents();
	}

	@Override
	public double getRejectedRequestsRate() {
		return aggregateRejectedRequestsCounters(collectGeneralRequestsStatsFromAllClients()).getSmoothedRate();
	}

	@Override
	public String getRejectedRequestsDetails() {
		return aggregateRejectedRequestsCounters(collectGeneralRequestsStatsFromAllClients()).toString();
	}

	@Override
	public long getExpiredRequests() {
		return aggregateExpiredRequestsCounters(collectGeneralRequestsStatsFromAllClients()).getTotalEvents();
	}

	@Override
	public double getExpiredRequestsRate() {
		return aggregateExpiredRequestsCounters(collectGeneralRequestsStatsFromAllClients()).getSmoothedRate();
	}

	@Override
	public String getExpiredRequestsDetails() {
		return aggregateExpiredRequestsCounters(collectGeneralRequestsStatsFromAllClients()).toString();
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
		return aggregatedResponseTimeCounters(collectGeneralRequestsStatsFromAllClients()).getSmoothedAverage();
	}

	@Override
	public String getAverageResponseTimeDetails() {
		return aggregatedResponseTimeCounters(collectGeneralRequestsStatsFromAllClients()).toString();
	}

	@Override
	public String getLastServerException() {
		Throwable lastException = aggregateExceptionCounters(collectGeneralRequestsStatsFromAllClients()).getLastException();
		return lastException != null ? lastException.toString() : "";
	}

	@Override
	public int getExceptionsCount() {
		return (int) aggregateExceptionCounters(collectGeneralRequestsStatsFromAllClients()).getTotalExceptions();
	}

	// methods to simplify collecting stats from rpcClients
	private List<RpcJmxRequestsStatsSet> collectGeneralRequestsStatsFromAllClients() {
		List<RpcJmxRequestsStatsSet> clientsGeneralRequestsStats = new ArrayList<>();
		for (RpcJmxClient rpcClient : rpcClients) {
			clientsGeneralRequestsStats.add(rpcClient.getGeneralRequestsStats());
		}
		return clientsGeneralRequestsStats;
	}

	private List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> collectConnectsStatsPerAddressFromAllClients() {
		List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> clientsConnectsStats = new ArrayList<>();
		for (RpcJmxClient rpcClient : rpcClients) {
			clientsConnectsStats.add(rpcClient.getConnectsStatsPerAddress());
		}
		return clientsConnectsStats;
	}

	private List<Map<Class<?>, RpcJmxRequestsStatsSet>> getClientsRequestsStatsPerClass() {
		List<Map<Class<?>, RpcJmxRequestsStatsSet>> clientsStatsPerClass = new ArrayList<>();
		for (RpcJmxClient rpcClient : rpcClients) {
			clientsStatsPerClass.add(rpcClient.getRequestsStatsPerClass());
		}
		return clientsStatsPerClass;
	}

	private List<Map<InetSocketAddress, RpcJmxRequestsStatsSet>> getClientsRequestsStatsPerAddress() {
		List<Map<InetSocketAddress, RpcJmxRequestsStatsSet>> clientsAddressesStats = new ArrayList<>();
		for (RpcJmxClient rpcClient : rpcClients) {
			clientsAddressesStats.add(rpcClient.getRequestStatsPerAddress());
		}
		return clientsAddressesStats;
	}

	private List<InetSocketAddress> getClientsAddresses() {
		List<InetSocketAddress> allClientsAddresses = new ArrayList<>();
		for (RpcJmxClient rpcClient : rpcClients) {
			for (InetSocketAddress address : rpcClient.getAddresses()) {
				if (!allClientsAddresses.contains(address)) {
					allClientsAddresses.add(address);
				}
			}
		}
		return allClientsAddresses;
	}

	// aggregating methods
	private EventsCounter.Accumulator aggregateTotalRequestsCounters(List<RpcJmxRequestsStatsSet> stats) {
		EventsCounter.Accumulator accumulator = EventsCounter.accumulator();
		for (RpcJmxRequestsStatsSet stat : stats) {
			accumulator.add(stat.getTotalRequests());
		}
		return accumulator;
	}

	private EventsCounter.Accumulator aggregateSuccessfulRequestsCounters(List<RpcJmxRequestsStatsSet> stats) {
		EventsCounter.Accumulator accumulator = EventsCounter.accumulator();
		for (RpcJmxRequestsStatsSet stat : stats) {
			accumulator.add(stat.getSuccessfulRequests());
		}
		return accumulator;
	}

	private EventsCounter.Accumulator aggregateFailedRequestsCounters(List<RpcJmxRequestsStatsSet> stats) {
		EventsCounter.Accumulator accumulator = EventsCounter.accumulator();
		for (RpcJmxRequestsStatsSet stat : stats) {
			accumulator.add(stat.getFailedRequests());
		}
		return accumulator;
	}

	private EventsCounter.Accumulator aggregateExpiredRequestsCounters(List<RpcJmxRequestsStatsSet> stats) {
		EventsCounter.Accumulator accumulator = EventsCounter.accumulator();
		for (RpcJmxRequestsStatsSet stat : stats) {
			accumulator.add(stat.getExpiredRequests());
		}
		return accumulator;
	}

	private EventsCounter.Accumulator aggregateRejectedRequestsCounters(List<RpcJmxRequestsStatsSet> stats) {
		EventsCounter.Accumulator accumulator = EventsCounter.accumulator();
		for (RpcJmxRequestsStatsSet stat : stats) {
			accumulator.add(stat.getRejectedRequests());
		}
		return accumulator;
	}

	private StatsCounter.Accumulator aggregatedResponseTimeCounters(List<RpcJmxRequestsStatsSet> stats) {
		StatsCounter.Accumulator accumulator = StatsCounter.accumulator();
		for (RpcJmxRequestsStatsSet stat : stats) {
			accumulator.add(stat.getResponseTimeStats());
		}
		return accumulator;
	}

	private LastExceptionCounter.Accumulator aggregateExceptionCounters(List<RpcJmxRequestsStatsSet> stats) {
		LastExceptionCounter.Accumulator accumulator = LastExceptionCounter.accumulator();
		for (RpcJmxRequestsStatsSet stat : stats) {
			accumulator.add(stat.getLastServerExceptionCounter());
		}
		return accumulator;
	}

	private EventsCounter.Accumulator aggregateSuccessfulConnectsCounters(List<RpcJmxConnectsStatsSet> stats) {
		EventsCounter.Accumulator accumulator = EventsCounter.accumulator();
		for (RpcJmxConnectsStatsSet stat : stats) {
			accumulator.add(stat.getSuccessfulConnects());
		}
		return accumulator;
	}

	private EventsCounter.Accumulator aggregateFailedConnectsCounters(List<RpcJmxConnectsStatsSet> stats) {
		List<EventsCounter> failedConnectCounters = new ArrayList<>(stats.size());
		EventsCounter.Accumulator accumulator = EventsCounter.accumulator();
		for (RpcJmxConnectsStatsSet stat : stats) {
			accumulator.add(stat.getFailedConnects());
		}
		return accumulator;
	}

	private EventsCounter.Accumulator aggregateClosedConnectsCounters(List<RpcJmxConnectsStatsSet> stats) {
		EventsCounter.Accumulator accumulator = EventsCounter.accumulator();
		for (RpcJmxConnectsStatsSet stat : stats) {
			accumulator.add(stat.getClosedConnects());
		}
		return accumulator;
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
		List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> allClientsConnectsStats =
				collectConnectsStatsPerAddressFromAllClients();
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
		List<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> clientsStats =
				collectConnectsStatsPerAddressFromAllClients();
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
}