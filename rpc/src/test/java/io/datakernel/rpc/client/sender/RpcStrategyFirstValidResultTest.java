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
import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStubWithKey;
import io.datakernel.rpc.util.Predicate;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class RpcStrategyFirstValidResultTest {

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);

	@Test
	public void itShouldSendRequestToAllAvailableSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy allAvailableStrategy =
				new RpcStrategyFirstValidResult(asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();
		int callsAmountIterationOne = 10;
		int callsAmountIterationTwo = 25;
		RpcRequestSender senderToAll;

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		senderToAll = allAvailableStrategy.create(pool).getSender();
		for (int i = 0; i < callsAmountIterationOne; i++) {
			senderToAll.sendRequest(data, timeout, callback);
		}
		pool.remove(ADDRESS_1);
		// we should recreate sender after changing in pool
		senderToAll = allAvailableStrategy.create(pool).getSender();
		for (int i = 0; i < callsAmountIterationTwo; i++) {
			senderToAll.sendRequest(data, timeout, callback);
		}

		assertEquals(callsAmountIterationOne, connection1.getCallsAmount());
		assertEquals(callsAmountIterationOne + callsAmountIterationTwo, connection2.getCallsAmount());
		assertEquals(callsAmountIterationOne + callsAmountIterationTwo, connection3.getCallsAmount());
	}

	@Test
	public void itShouldCallOnResultWithNullIfAllSendersReturnedNullAndValidatorAndExceptionAreNotSpecified() {
		final AtomicInteger onResultWithNullWasCalledTimes = new AtomicInteger(0);
		RpcRequestSendingStrategy returningNullStrategy1 = new RequestSenderOnResultWithNullStrategy();
		RpcRequestSendingStrategy returningNullStrategy2 = new RequestSenderOnResultWithNullStrategy();
		RpcRequestSendingStrategy returningNullStrategy3 = new RequestSenderOnResultWithNullStrategy();
		RpcStrategyFirstValidResult allAvailableStrategy = new RpcStrategyFirstValidResult(
				asList(returningNullStrategy1, returningNullStrategy2, returningNullStrategy3)
		);
		RpcRequestSender sender = allAvailableStrategy.create(
				new RpcClientConnectionPool(Arrays.<InetSocketAddress>asList())).getSender();
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		ResultCallback<RpcMessageDataStub> callback = new ResultCallback<RpcMessageDataStub>() {
			@Override
			public void onException(Exception exception) {
				throw new IllegalStateException(exception);
			}

			@Override
			public void onResult(RpcMessageDataStub result) {
				assert result == null;
				onResultWithNullWasCalledTimes.incrementAndGet();
			}
		};

		sender.sendRequest(data, timeout, callback);

		// despite there are several sender, onResult should be called only once after all senders returned null
		assertEquals(1, onResultWithNullWasCalledTimes.get());
	}

	@Test
	public void itShouldCallOnExceptionIfAllSendersReturnsNullAndValidatorIsDefaultButExceptionIsSpecified() {
		// default validator should check whether result is not null
		String exceptionMessage = "exception-message";
		final AtomicReference<Exception> passedException = new AtomicReference<>(null);
		RpcRequestSendingStrategy returningNullStrategy1 = new RequestSenderOnResultWithNullStrategy();
		RpcRequestSendingStrategy returningNullStrategy2 = new RequestSenderOnResultWithNullStrategy();
		RpcRequestSendingStrategy returningNullStrategy3 = new RequestSenderOnResultWithNullStrategy();
		RpcStrategyFirstValidResult allAvailableStrategy = new RpcStrategyFirstValidResult(
				asList(returningNullStrategy1, returningNullStrategy2, returningNullStrategy3)
		).withNoValidResultException(new Exception(exceptionMessage));
		RpcRequestSender sender = allAvailableStrategy.create(
				new RpcClientConnectionPool(Arrays.<InetSocketAddress>asList())).getSender();
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		ResultCallback<RpcMessageDataStub> callback = new ResultCallback<RpcMessageDataStub>() {
			@Override
			public void onException(Exception exception) {
				assert passedException.get() == null; // ensure onException is called only once
				passedException.set(exception);
			}

			@Override
			public void onResult(RpcMessageDataStub result) {
				throw new IllegalStateException();
			}
		};

		sender.sendRequest(data, timeout, callback);

		assertEquals(exceptionMessage, passedException.get().getMessage());
	}

	@Test
	public void itShouldUseCustomValidatorIfItIsSpecified() {
		final int invalidKey = 1;
		final int validKey = 2;
		final String exceptionMessage = "exception-message";
		final AtomicReference<RpcMessageDataStubWithKey> passedResult = new AtomicReference<>(null);
		final RpcRequestSendingStrategy returningValueStrategy1
				= new RequestSenderOnResultWithValueStrategy(new RpcMessageDataStubWithKey(invalidKey));
		RpcRequestSendingStrategy returningValueStrategy2
				= new RequestSenderOnResultWithValueStrategy(new RpcMessageDataStubWithKey(validKey));
		RpcRequestSendingStrategy returningValueStrategy3
				= new RequestSenderOnResultWithValueStrategy(new RpcMessageDataStubWithKey(invalidKey));
		Predicate<RpcMessageDataStubWithKey> validator = new Predicate<RpcMessageDataStubWithKey>() {
			@Override
			public boolean check(RpcMessageDataStubWithKey input) {
				return input.getKey() == validKey;
			}
		};
		RpcStrategyFirstValidResult allAvailableStrategy
				= new RpcStrategyFirstValidResult(
				asList(returningValueStrategy1, returningValueStrategy2, returningValueStrategy3)
		)
				.withResultValidator(validator)
				.withNoValidResultException(new Exception(exceptionMessage));
		RpcRequestSender sender = allAvailableStrategy.create(
				new RpcClientConnectionPool(Arrays.<InetSocketAddress>asList())).getSender();
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		ResultCallback<RpcMessageDataStubWithKey> callback = new ResultCallback<RpcMessageDataStubWithKey>() {
			@Override
			public void onException(Exception exception) {
				throw new IllegalStateException(exception);
			}

			@Override
			public void onResult(RpcMessageDataStubWithKey result) {
				assert passedResult.get() == null;  // ensure that onResult was called only once
				passedResult.set(result);
			}
		};

		sender.sendRequest(data, timeout, callback);

		assertEquals(validKey, passedResult.get().getKey());
	}

	@Test
	public void itShouldCallOnExceptionIfNoSenderReturnsValidResultButExceptionWasSpecified() {
		final int invalidKey = 1;
		final int validKey = 2;
		final String exceptionMessage = "exception-message";
		final AtomicReference<Exception> passedException = new AtomicReference<>(null);
		final RpcRequestSendingStrategy returningValueStrategy1
				= new RequestSenderOnResultWithValueStrategy(new RpcMessageDataStubWithKey(invalidKey));
		RpcRequestSendingStrategy returningValueStrategy2
				= new RequestSenderOnResultWithValueStrategy(new RpcMessageDataStubWithKey(invalidKey));
		RpcRequestSendingStrategy returningValueStrategy3
				= new RequestSenderOnResultWithValueStrategy(new RpcMessageDataStubWithKey(invalidKey));
		Predicate<RpcMessageDataStubWithKey> validator = new Predicate<RpcMessageDataStubWithKey>() {
			@Override
			public boolean check(RpcMessageDataStubWithKey input) {
				return input.getKey() == validKey;
			}
		};
		RpcStrategyFirstValidResult allAvailableStrategy
				= new RpcStrategyFirstValidResult(
				asList(returningValueStrategy1, returningValueStrategy2, returningValueStrategy3)
		)
				.withResultValidator(validator)
				.withNoValidResultException(new Exception(exceptionMessage));
		RpcRequestSender sender = allAvailableStrategy.create(
				new RpcClientConnectionPool(Arrays.<InetSocketAddress>asList())).getSender();
		int timeout = 50;
		Object data = new RpcMessageDataStub();
		ResultCallback<RpcMessageDataStubWithKey> callback = new ResultCallback<RpcMessageDataStubWithKey>() {
			@Override
			public void onException(Exception exception) {
				assert passedException.get() == null; // ensure onException is called only once
				passedException.set(exception);
			}

			@Override
			public void onResult(RpcMessageDataStubWithKey result) {
				throw new IllegalStateException();
			}
		};

		sender.sendRequest(data, timeout, callback);

		assertEquals(exceptionMessage, passedException.get().getMessage());
	}

	@Test
	public void itShouldBeCreatedWhenThereIsAtLeastOneActiveSubSender() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection = new RpcClientConnectionStub();
		// one connection is added
		pool.add(ADDRESS_2, connection);
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy firstValideResult =
				new RpcStrategyFirstValidResult(asList(singleServerStrategy1, singleServerStrategy2));

		assertFalse(singleServerStrategy1.create(pool).isSenderPresent());
		assertTrue(singleServerStrategy2.create(pool).isSenderPresent());
		assertTrue(firstValideResult.create(pool).isSenderPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNoActiveSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		// no connections were added to pool
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy firstValidResult =
				new RpcStrategyFirstValidResult(asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));

		assertFalse(singleServerStrategy1.create(pool).isSenderPresent());
		assertFalse(singleServerStrategy2.create(pool).isSenderPresent());
		assertFalse(singleServerStrategy3.create(pool).isSenderPresent());
		assertFalse(firstValidResult.create(pool).isSenderPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy firstValidResult =
				new RpcStrategyFirstValidResult(
						asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3)
				).withMinActiveSubStrategies(4);

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);

		assertTrue(singleServerStrategy1.create(pool).isSenderPresent());
		assertTrue(singleServerStrategy2.create(pool).isSenderPresent());
		assertTrue(singleServerStrategy3.create(pool).isSenderPresent());
		assertFalse(firstValidResult.create(pool).isSenderPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereAreNotEnoughActiveSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcRequestSendingStrategy singleServerStrategy1 = new RpcStrategySingleServer(ADDRESS_1);
		RpcRequestSendingStrategy singleServerStrategy2 = new RpcStrategySingleServer(ADDRESS_2);
		RpcRequestSendingStrategy singleServerStrategy3 = new RpcStrategySingleServer(ADDRESS_3);
		RpcRequestSendingStrategy firstAvailableStrategy =
				new RpcStrategyFirstValidResult(
						asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3)
				).withMinActiveSubStrategies(3);

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		// we don't add connection3

		assertTrue(singleServerStrategy1.create(pool).isSenderPresent());
		assertTrue(singleServerStrategy2.create(pool).isSenderPresent());
		assertFalse(singleServerStrategy3.create(pool).isSenderPresent());
		assertFalse(firstAvailableStrategy.create(pool).isSenderPresent());
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenSubStrategiesListIsNull() {
		RpcRequestSendingStrategy strategy = new RpcStrategyFirstValidResult(null);
	}

	static final class RequestSenderOnResultWithNullCaller implements RpcRequestSender {

		@Override
		public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
			callback.onResult(null);
		}
	}

	static final class RequestSenderOnResultWithValueCaller implements RpcRequestSender {

		private final Object data;

		public RequestSenderOnResultWithValueCaller(Object data) {
			this.data = data;
		}

		@Override
		public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
			callback.onResult((T) data);
		}
	}

	static final class RequestSenderOnResultWithNullStrategy implements RpcRequestSendingStrategy {

		@Override
		public RpcRequestSenderHolder create(RpcClientConnectionPool pool) {
			return RpcRequestSenderHolder.of(new RequestSenderOnResultWithNullCaller());
		}

		@Override
		public List<RpcRequestSenderHolder> createAsList(RpcClientConnectionPool pool) {
			return asList(create(pool));
		}
	}

	static final class RequestSenderOnResultWithValueStrategy implements RpcRequestSendingStrategy {

		private final Object data;

		public RequestSenderOnResultWithValueStrategy(Object data) {
			this.data = data;
		}

		@Override
		public RpcRequestSenderHolder create(RpcClientConnectionPool pool) {
			return RpcRequestSenderHolder.of(new RequestSenderOnResultWithValueCaller(data));
		}

		@Override
		public List<RpcRequestSenderHolder> createAsList(RpcClientConnectionPool pool) {
			return asList(create(pool));
		}
	}
}