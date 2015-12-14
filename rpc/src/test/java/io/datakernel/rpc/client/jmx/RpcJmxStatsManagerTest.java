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

import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.time.CurrentTimeProvider;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RpcJmxStatsManagerTest {

	public static final double SMOOTHING_WINDOW = 10.0;
	public static final double SMOOTHING_PRECISION = 0.1;
	public static final ManualTimeProvider MANUAL_TIME_PROVIDER = new ManualTimeProvider(0L);

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
	public void itShouldInjectOneselfToAllClientsWhenMonitoringIsEnabled() {
		List<RpcClientJmx> clients =
				Arrays.<RpcClientJmx>asList(new RpcClientJmxStub(), new RpcClientJmxStub(), new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);

		statsManager.startMonitoring();

		int amountOfClientsThatReceviedStatsManager = 0;
		for (RpcClientJmx client : clients) {
			if (((RpcClientJmxStub) client).getStatsManager() == statsManager) {
				++amountOfClientsThatReceviedStatsManager;
			}
		}
		assertEquals(3, amountOfClientsThatReceviedStatsManager);
	}

	@Test
	public void itShouldInjectNullToAllClientsWhenMonitoringIsDisabled() {
		List<RpcClientJmx> clients =
				Arrays.<RpcClientJmx>asList(new RpcClientJmxStub(), new RpcClientJmxStub(), new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);

		statsManager.startMonitoring();
		statsManager.stopMonitoring();

		int amountOfClientsThatReceviedNullReference = 0;
		for (RpcClientJmx client : clients) {
			if (((RpcClientJmxStub) client).getStatsManager() == null) {
				++amountOfClientsThatReceviedNullReference;
			}
		}
		assertEquals(3, amountOfClientsThatReceviedNullReference);
	}

	@Test
	public void itShouldNotRecordEventsIfMonitoringIsNotEnabled() {
		List<RpcClientJmx> clients = asList((RpcClientJmx)new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);
		Class<?> requestClass = Object.class;
		Exception exception = new Exception();
		Object causedObject = null;
		int responseTime = 100;

		int pendingRequests = 3;
		int successfulRequests = 4;
		int failedRequests = 8;
		int rejectedRequests = 10;
		int expiredRequests = 15;

		// monitoring is disabled
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

		assertEquals(0, statsManager.getTotalPendingRequests());
		assertEquals(0, statsManager.getTotalSuccessfulRequests());
		assertEquals(0, statsManager.getTotalFailedRequests());
		assertEquals(0, statsManager.getTotalRejectedRequests());
		assertEquals(0, statsManager.getTotalExpiredRequests());
	}

	@Test
	public void itShouldRecordEventsAndCountThemProperlyIfMonitoringIsEnabled() {
		List<RpcClientJmx> clients = asList((RpcClientJmx)new RpcClientJmxStub());
		RpcJmxStatsManager statsManager =
				new RpcJmxStatsManager(clients, SMOOTHING_WINDOW, SMOOTHING_PRECISION, MANUAL_TIME_PROVIDER);
		Class<?> requestClass = Object.class;
		Exception exception = new Exception();
		Object causedObject = null;
		int responseTime = 100;

		int pendingRequests = 3;
		int successfulRequests = 4;
		int failedRequests = 8;
		int rejectedRequests = 10;
		int expiredRequests = 15;

		// monitoring is disabled
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

		assertEquals(pendingRequests, statsManager.getTotalPendingRequests());
		assertEquals(successfulRequests, statsManager.getTotalSuccessfulRequests());
		assertEquals(failedRequests, statsManager.getTotalFailedRequests());
		assertEquals(rejectedRequests, statsManager.getTotalRejectedRequests());
		assertEquals(expiredRequests, statsManager.getTotalExpiredRequests());
	}






















	public static class RpcClientJmxStub implements RpcClientJmx {

		private RpcJmxStatsManager statsManager = null;

		@Override
		public void setRpcJmxStatsManager(RpcJmxStatsManager jmxStatsManager) {
			this.statsManager = jmxStatsManager;
		}

		public RpcJmxStatsManager getStatsManager() {
			return statsManager;
		}
	}
}
