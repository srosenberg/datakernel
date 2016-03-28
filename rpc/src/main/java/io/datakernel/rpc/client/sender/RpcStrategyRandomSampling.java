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

package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.util.XorShiftRandom;

import java.net.InetSocketAddress;
import java.util.*;

import static io.datakernel.util.Preconditions.checkArgument;

public class RpcStrategyRandomSampling implements RpcStrategy {
	private final Map<RpcStrategy, Integer> strategyToWeight = new HashMap<>();

	// TODO(vmykhalko): implement
	// public RpcStrategyRandomSampling add(RpcStrategy strategy, String strategyName, int weight)

	public RpcStrategyRandomSampling add(int weight, RpcStrategy strategy) {
		checkArgument(weight >= 0, "weight cannot be negative");
		checkArgument(!strategyToWeight.containsKey(strategy), "strategy is already added");

		strategyToWeight.put(strategy, weight);

		return this;
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		HashSet<InetSocketAddress> result = new HashSet<>();
		for (RpcStrategy strategy : strategyToWeight.keySet()) {
			result.addAll(strategy.getAddresses());
		}
		return result;
	}

	@Override
	public RpcSender createSender(RpcClientConnectionPool pool) {
		Map<RpcSender, Integer> senderToWeight = new HashMap<>();
		for (RpcStrategy rpcStrategy : strategyToWeight.keySet()) {
			RpcSender sender = rpcStrategy.createSender(pool);
			if (sender != null) {
				senderToWeight.put(sender, strategyToWeight.get(rpcStrategy));
			}
		}
		return new RandomSamplingSender(senderToWeight);
	}

	private static final class RandomSamplingSender implements RpcSender {
		private final XorShiftRandom random = new XorShiftRandom();
		private final List<RpcSender> senders;
		private final int[] cumulativeWeights;
		private final int totalWeight;

		public RandomSamplingSender(Map<RpcSender, Integer> senderToWeight) {
			checkArgument(!senderToWeight.containsKey(null), "sender cannot be null");

			senders = new ArrayList<>(senderToWeight.size());
			cumulativeWeights = new int[senderToWeight.size()];
			int currentCumulativeWeight = 0;
			int currentSender = 0;
			for (RpcSender rpcSender : senderToWeight.keySet()) {
				currentCumulativeWeight += senderToWeight.get(rpcSender);
				senders.add(rpcSender);
				cumulativeWeights[currentSender++] = currentCumulativeWeight;
			}
			totalWeight = currentCumulativeWeight;
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
			chooseSender().sendRequest(request, timeout, callback);
		}

		private RpcSender chooseSender() {
			int currentRandomValue = (int) ((random.nextLong() & Long.MAX_VALUE) % totalWeight);
			int senderIndex = binarySearch(currentRandomValue, 0, cumulativeWeights.length);
			return senders.get(senderIndex);
		}

		private int binarySearch(int value, int lowerIndex, int upperIndex) {
			if (lowerIndex == upperIndex) {
				return lowerIndex;
			}

			int middle = (lowerIndex + upperIndex) / 2;
			if (value >= cumulativeWeights[middle]) {
				return binarySearch(value, middle + 1, upperIndex);
			} else {
				return binarySearch(value, lowerIndex, middle);
			}
		}
	}

}
