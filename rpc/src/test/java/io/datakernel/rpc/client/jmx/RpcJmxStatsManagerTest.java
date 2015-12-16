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

import io.datakernel.rpc.util.Predicate;
import org.junit.Test;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RpcJmxStatsManagerTest {

	public static final double SMOOTHING_WINDOW = 10.0;
	public static final double SMOOTHING_PRECISION = 0.1;
	public static final ManualTimeProvider MANUAL_TIME_PROVIDER = new ManualTimeProvider(0L);
	public static final Random RANDOM = new Random();

	@Test
	public void itShouldEnableAndDisableMonitoring() {
		List<RpcClientJmx> clients = asList((RpcClientJmx)new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);

		assertFalse(statsManager.isMonitoring());
		statsManager.startMonitoring();
		assertTrue(statsManager.isMonitoring());
		statsManager.stopMonitoring();
		assertFalse(statsManager.isMonitoring());
	}

	@Test
	public void itShouldCallStartMonitoringInjectOneselfToAllClientsWhenMonitoringIsEnabled() {
		List<RpcClientJmx> clients =
				Arrays.<RpcClientJmx>asList(new RpcClientJmxStub(), new RpcClientJmxStub(), new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);

		statsManager.startMonitoring();

		int amountOfClientsThatRecievedStartMonitoringCallWithProperParameter = 0;
		for (RpcClientJmx client : clients) {
			RpcClientJmxStub clientStub = ((RpcClientJmxStub) client);
			if (clientStub.wasStartMonitoringCalled() && clientStub.getStatsManager() == statsManager) {
				++amountOfClientsThatRecievedStartMonitoringCallWithProperParameter;
			}
		}
		assertEquals(3, amountOfClientsThatRecievedStartMonitoringCallWithProperParameter);
	}

	@Test
	public void itShouldCallStopMonitoringOnAllClientsWhenMonitoringIsDisabled() {
		List<RpcClientJmx> clients =
				Arrays.<RpcClientJmx>asList(new RpcClientJmxStub(), new RpcClientJmxStub(), new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);

		statsManager.startMonitoring();
		statsManager.stopMonitoring();

		int amountOfClientsThatReceivedStopMonitoringCall = 0;
		for (RpcClientJmx client : clients) {
			if (((RpcClientJmxStub) client).wasStopMonitoringCalled()) {
				++amountOfClientsThatReceivedStopMonitoringCall;
			}
		}
		assertEquals(3, amountOfClientsThatReceivedStopMonitoringCall);
	}

	@Test
	public void itShouldRecordEventsAndCountThemProperly() {
		List<RpcClientJmx> clients = asList((RpcClientJmx) new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);
		Class<?> requestClass = Object.class;
		Exception exception = new Exception();
		Object causedObject = null;
		int responseTime = 100;
		InetSocketAddress address = InetSocketAddress.createUnresolved("1.1.1.1", 10000);

		int successfulConnects = 5;
		int failedConnects = 3;
		int closedConnects = 2;

		int pendingRequests = 3;
		int successfulRequests = 5;
		int failedRequests = 8;
		int rejectedRequests = 10;
		int expiredRequests = 15;
		int totalRequests = pendingRequests + successfulRequests + failedRequests + rejectedRequests + expiredRequests;

		// record connects info
		for (int i = 0; i < successfulConnects; i++) {
			statsManager.recordSuccessfulConnect(address);
		}
		for (int i = 0; i < failedConnects; i++) {
			statsManager.recordFailedConnect(address);
		}
		for (int i = 0; i < closedConnects; i++) {
			statsManager.recordClosedConnect(address);
		}

		// record requests info
		for (int i = 0; i < pendingRequests; i++) {
			statsManager.recordNewRequest(requestClass);
		}
		for (int i = 0; i < successfulRequests; i++) {
			statsManager.recordNewRequest(requestClass);
			statsManager.recordSuccessfulRequest(requestClass, responseTime);
		}
		for (int i = 0; i < failedRequests; i++) {
			statsManager.recordNewRequest(requestClass);
			statsManager.recordFailedRequest(requestClass, exception, causedObject, responseTime);
		}
		for (int i = 0; i < rejectedRequests; i++) {
			statsManager.recordNewRequest(requestClass);
			statsManager.recordRejectedRequest(requestClass);
		}
		for (int i = 0; i < expiredRequests; i++) {
			statsManager.recordNewRequest(requestClass);
			statsManager.recordExpiredRequest(requestClass);
		}

		// check requests stats
		assertEquals(totalRequests, extractTotalEvents(statsManager.getTotalRequestsStats()));
		assertEquals(pendingRequests, extractLastValue(statsManager.getPendingRequestsStats()));
		assertEquals(successfulRequests, extractTotalEvents(statsManager.getSuccessfulRequestsStats()));
		assertEquals(failedRequests, extractTotalEvents(statsManager.getFailedRequestsStats()));
		assertEquals(rejectedRequests, extractTotalEvents(statsManager.getRejectedRequestsStats()));
		assertEquals(expiredRequests, extractTotalEvents(statsManager.getExpiredRequestsStats()));

		// check connects stats
		assertEquals(successfulConnects, extractTotalEvents(statsManager.getSuccessfulConnectsStats()));
		assertEquals(failedConnects, extractTotalEvents(statsManager.getFailedConnectsStats()));
		assertEquals(closedConnects, extractTotalEvents(statsManager.getClosedConnectsStats()));
	}

	@Test
	public void itShouldResetStatsAfterResetMethodIsCalled() {
		List<RpcClientJmx> clients = asList((RpcClientJmx) new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);
		Class<?> requestClass = Object.class;
		Exception exception = new Exception();
		Object causedObject = null;
		int responseTime = 100;
		InetSocketAddress address = InetSocketAddress.createUnresolved("1.1.1.1", 10000);

		int successfulConnects = 5;
		int failedConnects = 3;
		int closedConnects = 2;

		int pendingRequests = 3;
		int successfulRequests = 5;
		int failedRequests = 8;
		int rejectedRequests = 10;
		int expiredRequests = 15;
		int totalRequests = pendingRequests + successfulRequests + failedRequests + rejectedRequests + expiredRequests;

		// record connects info
		for (int i = 0; i < successfulConnects; i++) {
			statsManager.recordSuccessfulConnect(address);
		}
		for (int i = 0; i < failedConnects; i++) {
			statsManager.recordFailedConnect(address);
		}
		for (int i = 0; i < closedConnects; i++) {
			statsManager.recordClosedConnect(address);
		}

		// record requests info
		for (int i = 0; i < pendingRequests; i++) {
			statsManager.recordNewRequest(requestClass);
		}
		for (int i = 0; i < successfulRequests; i++) {
			statsManager.recordNewRequest(requestClass);
			statsManager.recordSuccessfulRequest(requestClass, responseTime);
		}
		for (int i = 0; i < failedRequests; i++) {
			statsManager.recordNewRequest(requestClass);
			statsManager.recordFailedRequest(requestClass, exception, causedObject, responseTime);
		}
		for (int i = 0; i < rejectedRequests; i++) {
			statsManager.recordNewRequest(requestClass);
			statsManager.recordRejectedRequest(requestClass);
		}
		for (int i = 0; i < expiredRequests; i++) {
			statsManager.recordNewRequest(requestClass);
			statsManager.recordExpiredRequest(requestClass);
		}

		statsManager.resetStats();

		// check requests stats
		assertEquals(0, extractTotalEvents(statsManager.getTotalRequestsStats()));
		assertEquals(0, extractLastValue(statsManager.getPendingRequestsStats()));
		assertEquals(0, extractTotalEvents(statsManager.getSuccessfulRequestsStats()));
		assertEquals(0, extractTotalEvents(statsManager.getFailedRequestsStats()));
		assertEquals(0, extractTotalEvents(statsManager.getRejectedRequestsStats()));
		assertEquals(0, extractTotalEvents(statsManager.getExpiredRequestsStats()));

		// check connects stats
		assertEquals(0, extractTotalEvents(statsManager.getSuccessfulConnectsStats()));
		assertEquals(0, extractTotalEvents(statsManager.getFailedConnectsStats()));
		assertEquals(0, extractTotalEvents(statsManager.getClosedConnectsStats()));

		// TODO(vmykhalko): check also addresses / request classes stats
	}

	@Test
	public void itShouldCountActiveConnections() {
		List<RpcClientJmx> clients = asList((RpcClientJmx) new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);
		InetSocketAddress address_1 = InetSocketAddress.createUnresolved("1.1.1.1", 10000);
		InetSocketAddress address_2 = InetSocketAddress.createUnresolved("2.2.2.2", 10000);
		InetSocketAddress address_3 = InetSocketAddress.createUnresolved("3.3.3.3", 10000);

		statsManager.recordSuccessfulConnect(address_1); // connection for address_1 is active
		statsManager.recordSuccessfulConnect(address_2); // connection for address_2 is active
		statsManager.recordFailedConnect(address_3); // connection for address_3 is not active

		assertEquals(2, statsManager.getActiveConnectionsCount());

		statsManager.recordClosedConnect(address_2);  // connection for address_2 is not active

		assertEquals(1, statsManager.getActiveConnectionsCount());
	}

	@Test
	public void itShouldCalculateProperAverageResponseTime() {
		List<RpcClientJmx> clients = asList((RpcClientJmx) new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);
		Class<?> requestClass = Object.class;
		Exception exception = new Exception();
		Object causedObject = null;
		int successfulRequest_ResponseTime = 150;
		int failedRequest_ResponseTime = 200;
		double successfulRequestProbability = 0.8;
		int requestsAmount = 10000;
		int timeIntervalBetweenRequests = (int)(SMOOTHING_WINDOW * 1000) / 500;

		for (int i = 0; i < requestsAmount; i++) {
			boolean isThisRequestSuccessful = RANDOM.nextDouble() < successfulRequestProbability;
			if (isThisRequestSuccessful) {
				statsManager.recordSuccessfulRequest(requestClass, successfulRequest_ResponseTime);
			} else {
				statsManager.recordFailedRequest(requestClass, exception, causedObject, failedRequest_ResponseTime);
			}
			MANUAL_TIME_PROVIDER.upgradeTime(timeIntervalBetweenRequests);
		}

		double expectedAverage = successfulRequest_ResponseTime * successfulRequestProbability +
				failedRequest_ResponseTime * (1.0 - successfulRequestProbability);
		double acceptableError = 1.0;
		assertEquals(expectedAverage, extractSmoothedAverage(statsManager.getAverageResponseTimeStats()), acceptableError);
	}

	@Test
	public void itShouldProperlyProcessExceptionStats() {
		List<RpcClientJmx> clients = asList((RpcClientJmx) new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);
		Class<?> requestClass = Object.class;
		CustomException exception_1 = new CustomException();
		CustomException exception_2 = new CustomException();
		CustomException exception_3 = new CustomException();
		Object causedObject = null;
		int responseTime = 100;

		statsManager.recordFailedRequest(requestClass, exception_1, causedObject, responseTime);
		statsManager.recordFailedRequest(requestClass, exception_2, causedObject, responseTime);
		statsManager.recordFailedRequest(requestClass, exception_3, causedObject, responseTime);

		assertEquals(3, statsManager.getExceptionsCount());
		assertEquals("CustomException", statsManager.getLastServerException().get("ExceptionType"));
	}

	@Test
	public void itShouldProperlyCalculateStatsPerRequestClass() throws OpenDataException {
		List<RpcClientJmx> clients = asList((RpcClientJmx) new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);
		final Class<?> requestClass_1 = Integer.class;
		final Class<?> requestClass_2 = String.class;
		int requestClass_1_requests = 7;
		int requestClass_2_requests = 11;
		Exception exception = new Exception();
		Object causedObject = null;
		int responseTime = 100;

		// for each request class amount of successful, failed, expired and rejected requests is 1,
		// but amount of total and pending requests is different

		for (int i = 0; i < requestClass_1_requests; i++) {
			statsManager.recordNewRequest(requestClass_1);
		}
		statsManager.recordSuccessfulRequest(requestClass_1, responseTime);
		statsManager.recordFailedRequest(requestClass_1, exception, causedObject, responseTime);
		statsManager.recordRejectedRequest(requestClass_1);
		statsManager.recordExpiredRequest(requestClass_1);

		for (int i = 0; i < requestClass_2_requests; i++) {
			statsManager.recordNewRequest(requestClass_2);
		}
		statsManager.recordSuccessfulRequest(requestClass_2, responseTime);
		statsManager.recordFailedRequest(requestClass_2, exception, causedObject, responseTime);
		statsManager.recordRejectedRequest(requestClass_2);
		statsManager.recordExpiredRequest(requestClass_2);

		final CompositeData[] compositeDataArray = statsManager.getRequestClassesStats();

		// predicates to find specific CompositeData in array
		Predicate<CompositeData> requestClass_1_Predicate = new Predicate<CompositeData>() {
			@Override
			public boolean check(CompositeData compositeData) {
				return compositeData.get("Request class").equals(requestClass_1.getName());
			}
		};
		Predicate<CompositeData> requestClass_2_Predicate = new Predicate<CompositeData>() {
			@Override
			public boolean check(CompositeData compositeData) {
				return compositeData.get("Request class").equals(requestClass_2.getName());
			}
		};

		CompositeData requestClass_1_compositeData = findInArray(compositeDataArray, requestClass_1_Predicate);
		CompositeData requestClass_2_compositeData = findInArray(compositeDataArray, requestClass_2_Predicate);

		int amountOfRequestsThatAreNotPending = 4; // 1 successful, 1 failed, 1 rejected and 1 expired

		// check stats for requestClass_1
		assertEquals(requestClass_1_requests,
				extractTotalEvents((String)requestClass_1_compositeData.get(RpcJmxStatsManager.TOTAL_REQUESTS_KEY)));
		assertEquals(requestClass_1_requests - amountOfRequestsThatAreNotPending,
				extractLastValue((String)requestClass_1_compositeData.get(RpcJmxStatsManager.PENDING_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_1_compositeData.get(RpcJmxStatsManager.SUCCESSFUL_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_1_compositeData.get(RpcJmxStatsManager.FAILED_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_1_compositeData.get(RpcJmxStatsManager.REJECTED_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String) requestClass_1_compositeData.get(RpcJmxStatsManager.EXPIRED_REQUESTS_KEY)));

		// check stats for requestClass_2
		assertEquals(requestClass_2_requests,
				extractTotalEvents((String)requestClass_2_compositeData.get(RpcJmxStatsManager.TOTAL_REQUESTS_KEY)));
		assertEquals(requestClass_2_requests - amountOfRequestsThatAreNotPending,
				extractLastValue((String)requestClass_2_compositeData.get(RpcJmxStatsManager.PENDING_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_2_compositeData.get(RpcJmxStatsManager.SUCCESSFUL_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_2_compositeData.get(RpcJmxStatsManager.FAILED_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_2_compositeData.get(RpcJmxStatsManager.REJECTED_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String) requestClass_2_compositeData.get(RpcJmxStatsManager.EXPIRED_REQUESTS_KEY)));
	}

	@Test
	public void itShouldProperlyCalculateStatsPerAddress() {
		List<RpcClientJmx> clients = asList((RpcClientJmx) new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);
		final Class<?> requestClass = Object.class;
		InetSocketAddress address_1 = InetSocketAddress.createUnresolved("1.1.1.1", 10000);
		InetSocketAddress address_2 = InetSocketAddress.createUnresolved("2.2.2.2", 10000);
		int address_1_requests = 7;
		int address_2_requests = 11;
		Exception exception = new Exception();
		Object causedObject = null;
		int responseTime = 100;

		RpcJmxStatsManager.RpcAddressStatsManager address_1_StatsManager = statsManager.getAddressStatsManager()

		// for each address amount of successful, failed, expired and rejected requests is 1,
		// but amount of total and pending requests is different

		for (int i = 0; i < address_1_requests; i++) {
			statsManager.recordNewRequest(address_1);
		}
		statsManager.recordSuccessfulRequest(requestClass_1, responseTime);
		statsManager.recordFailedRequest(requestClass_1, exception, causedObject, responseTime);
		statsManager.recordRejectedRequest(requestClass_1);
		statsManager.recordExpiredRequest(requestClass_1);

		for (int i = 0; i < requestClass_2_requests; i++) {
			statsManager.recordNewRequest(requestClass_2);
		}
		statsManager.recordSuccessfulRequest(requestClass_2, responseTime);
		statsManager.recordFailedRequest(requestClass_2, exception, causedObject, responseTime);
		statsManager.recordRejectedRequest(requestClass_2);
		statsManager.recordExpiredRequest(requestClass_2);

		final CompositeData[] compositeDataArray = statsManager.getRequestClassesStats();

		// predicates to find specific CompositeData in array
		Predicate<CompositeData> requestClass_1_Predicate = new Predicate<CompositeData>() {
			@Override
			public boolean check(CompositeData compositeData) {
				return compositeData.get("Request class").equals(requestClass_1.getName());
			}
		};
		Predicate<CompositeData> requestClass_2_Predicate = new Predicate<CompositeData>() {
			@Override
			public boolean check(CompositeData compositeData) {
				return compositeData.get("Request class").equals(requestClass_2.getName());
			}
		};

		CompositeData requestClass_1_compositeData = findInArray(compositeDataArray, requestClass_1_Predicate);
		CompositeData requestClass_2_compositeData = findInArray(compositeDataArray, requestClass_2_Predicate);

		int amountOfRequestsThatAreNotPending = 4; // 1 successful, 1 failed, 1 rejected and 1 expired

		// check stats for requestClass_1
		assertEquals(requestClass_1_requests,
				extractTotalEvents((String)requestClass_1_compositeData.get(RpcJmxStatsManager.TOTAL_REQUESTS_KEY)));
		assertEquals(requestClass_1_requests - amountOfRequestsThatAreNotPending,
				extractLastValue((String)requestClass_1_compositeData.get(RpcJmxStatsManager.PENDING_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_1_compositeData.get(RpcJmxStatsManager.SUCCESSFUL_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_1_compositeData.get(RpcJmxStatsManager.FAILED_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_1_compositeData.get(RpcJmxStatsManager.REJECTED_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String) requestClass_1_compositeData.get(RpcJmxStatsManager.EXPIRED_REQUESTS_KEY)));

		// check stats for requestClass_2
		assertEquals(requestClass_2_requests,
				extractTotalEvents((String)requestClass_2_compositeData.get(RpcJmxStatsManager.TOTAL_REQUESTS_KEY)));
		assertEquals(requestClass_2_requests - amountOfRequestsThatAreNotPending,
				extractLastValue((String)requestClass_2_compositeData.get(RpcJmxStatsManager.PENDING_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_2_compositeData.get(RpcJmxStatsManager.SUCCESSFUL_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_2_compositeData.get(RpcJmxStatsManager.FAILED_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String)requestClass_2_compositeData.get(RpcJmxStatsManager.REJECTED_REQUESTS_KEY)));
		assertEquals(1, extractTotalEvents(
				(String) requestClass_2_compositeData.get(RpcJmxStatsManager.EXPIRED_REQUESTS_KEY)));
	}

//	@Test
//	public void test() {
//		CustomException customException = new CustomException("bingo");
//		LastExceptionCounter
//		System.out.println(Arrays.toString(MBeanFormat.formatException(customException)));
//	}

	// helpers

	public static int extractLastValue(String input) {
		String regex = ".*last:\\s+(-?\\d+)\\s.*";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(input);
		if (matcher.matches()) {
			return Integer.parseInt(matcher.group(1));
		} else {
			throw new RuntimeException("cannot parse input");
		}
	}

	public static double extractSmoothedAverage(String input) {
		String regex = "(\\d+.\\d+)±.*";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(input);
		if (matcher.matches()) {
			return Double.parseDouble(matcher.group(1));
		} else {
			throw new RuntimeException("cannot parse input");
		}
	}

	public static int extractTotalEvents(String input) {
		String regex = "total:\\s+(\\d+)\\s+.*";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(input);
		if (matcher.matches()) {
			return Integer.parseInt(matcher.group(1));
		} else {
			throw new RuntimeException("cannot parse input");
		}
	}

	public static<T> T findInArray(T[] array, Predicate<T> predicate) {
		for (T item : array) {
			if (predicate.check(item)) {
				return item;
			}
		}
		throw new IllegalArgumentException("Cannot find array element which satisfies condition");
	}



	public static class CustomException extends Exception {
		public CustomException(String message) {
			super(message);
		}

		public CustomException() {
			super();
		}
	}

	public static class RpcClientJmxStub implements RpcClientJmx {

		private RpcJmxStatsManager statsManager = null;
		private boolean stopMonitoringWasCalled;
		private boolean startMonitoringWasCalled;

		@Override
		public void startMonitoring(RpcJmxStatsManager jmxStatsManager) {
			this.statsManager = jmxStatsManager;
			this.startMonitoringWasCalled = true;
		}

		@Override
		public void stopMonitoring() {
			stopMonitoringWasCalled = true;
		}

		public RpcJmxStatsManager getStatsManager() {
			return statsManager;
		}

		public boolean wasStopMonitoringCalled() {
			return stopMonitoringWasCalled;
		}

		public boolean wasStartMonitoringCalled() {
			return startMonitoringWasCalled;
		}
	}
}
