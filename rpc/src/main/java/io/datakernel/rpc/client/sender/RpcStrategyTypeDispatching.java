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

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

public final class RpcStrategyTypeDispatching implements RpcStrategy {
	private Map<Class<?>, RpcStrategy> dataTypeToStrategy = new HashMap<>();
	private RpcStrategy defaultStrategy;

	private RpcStrategyTypeDispatching() {}

	public static RpcStrategyTypeDispatching create() {return new RpcStrategyTypeDispatching();}

	public RpcStrategyTypeDispatching on(Class<?> dataType,
	                                     RpcStrategy strategy) {
		checkNotNull(dataType);
		checkNotNull(strategy);
		checkState(!dataTypeToStrategy.containsKey(dataType),
				"Strategy for type " + dataType.toString() + " is already set");
		dataTypeToStrategy.put(dataType, strategy);
		return this;
	}

	public RpcStrategyTypeDispatching onDefault(RpcStrategy strategy) {
		checkState(defaultStrategy == null, "Default Strategy is already set");
		defaultStrategy = strategy;
		return this;
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		HashSet<InetSocketAddress> result = new HashSet<>();
		for (RpcStrategy strategy : dataTypeToStrategy.values()) {
			result.addAll(strategy.getAddresses());
		}
		result.addAll(defaultStrategy.getAddresses());
		return result;
	}

	@Override
	public RpcSender createSender(RpcClientConnectionPool pool) {
		HashMap<Class<?>, RpcSender> typeToSender = new HashMap<>();
		for (Class<?> dataType : dataTypeToStrategy.keySet()) {
			RpcStrategy strategy = dataTypeToStrategy.get(dataType);
			RpcSender sender = strategy.createSender(pool);
			if (sender == null)
				return null;
			typeToSender.put(dataType, sender);
		}
		RpcSender defaultSender = null;
		if (defaultStrategy != null) {
			defaultSender = defaultStrategy.createSender(pool);
			if (typeToSender.isEmpty())
				return defaultSender;
			if (defaultSender == null)
				return null;
		}
		return new Sender(typeToSender, defaultSender);
	}

	static final class Sender implements RpcSender {
		@SuppressWarnings("ThrowableInstanceNeverThrown")
		private static final RpcNoSenderException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderException("No active senders available");

		private final HashMap<Class<?>, RpcSender> typeToSender;
		private final RpcSender defaultSender;

		public Sender(HashMap<Class<?>, RpcSender> typeToSender,
		              RpcSender defaultSender) {
			checkNotNull(typeToSender);

			this.typeToSender = typeToSender;
			this.defaultSender = defaultSender;
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
			RpcSender sender = typeToSender.get(request.getClass());
			if (sender == null) {
				sender = defaultSender;
			}
			if (sender != null) {
				sender.sendRequest(request, timeout, callback);
			} else {
				callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
			}
		}
	}
}
