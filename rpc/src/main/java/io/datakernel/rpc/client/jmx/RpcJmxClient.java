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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Implementations are supposed to be thread-safe
 */
public interface RpcJmxClient {

	void startMonitoring();

	void stopMonitoring();

	void reset();

	void reset(double smoothingWindow, double smoothingPrecision);

	/**
	 * Stats will be placed in {@code container}
	 *
	 * @param container container for stats
	 */
	void fetchGeneralRequestsStats(BlockingQueue<RpcJmxRequestsStatsSet> container);

	/**
	 * Stats will be placed in {@code container}
	 *
	 * @param container container for stats
	 */
	void fetchRequestsStatsPerClass(BlockingQueue<Map<Class<?>, RpcJmxRequestsStatsSet>> container);

	/**
	 * Stats will be placed in {@code container}
	 *
	 * @param container container for stats
	 */
	void fetchConnectsStatsPerAddress(BlockingQueue<Map<InetSocketAddress, RpcJmxConnectsStatsSet>> container);

	/**
	 * Stats will be placed in {@code container}
	 *
	 * @param container container for stats
	 */
	void fetchRequestStatsPerAddress(BlockingQueue<Map<InetSocketAddress, RpcJmxRequestsStatsSet>> container);

	/**
	 * Stats will be placed in {@code container}
	 *
	 * @param container container for stats
	 */
	void fetchActiveConnectionsCount(BlockingQueue<Integer> container);

	/**
	 * Stats will be placed in {@code container}
	 *
	 * @param container container for stats
	 */
	void fetchAddresses(BlockingQueue<List<InetSocketAddress>> container);
}
