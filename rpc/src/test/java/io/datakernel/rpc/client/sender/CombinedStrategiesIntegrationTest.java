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

import com.google.common.net.InetAddresses;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.hash.Sharder;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static io.datakernel.async.AsyncCallbacks.startFuture;
import static io.datakernel.async.AsyncCallbacks.stopFuture;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.eventloop.NioThreadFactory.defaultNioThreadFactory;
import static io.datakernel.rpc.client.sender.RpcRequestSendingStrategies.*;
import static io.datakernel.rpc.protocol.RpcSerializer.serializerFor;
import static org.junit.Assert.assertEquals;

public class CombinedStrategiesIntegrationTest {

	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final int TIMEOUT = 1500;
	private NioEventloop eventloop;
	private RpcServer serverOne;
	private RpcServer serverTwo;
	private RpcServer serverThree;

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		eventloop = new NioEventloop();

		serverOne = RpcServer.create(eventloop, serializerFor(HelloRequest.class, HelloResponse.class))
				.on(HelloRequest.class, helloServiceRequestHandler(new HelloServiceImplOne()))
				.setListenPort(PORT_1);
		serverOne.listen();

		serverTwo = RpcServer.create(eventloop, serializerFor(HelloRequest.class, HelloResponse.class))
				.on(HelloRequest.class, helloServiceRequestHandler(new HelloServiceImplTwo()))
				.setListenPort(PORT_2);
		serverTwo.listen();

		serverThree = RpcServer.create(eventloop, serializerFor(HelloRequest.class, HelloResponse.class))
				.on(HelloRequest.class, helloServiceRequestHandler(new HelloServiceImplThree()))
				.setListenPort(PORT_3);
		serverThree.listen();

		defaultNioThreadFactory().newThread(eventloop).start();
	}

	@Test
	public void testBlockingCall() throws Exception {
		try (BlockingHelloClient client = new BlockingHelloClient(eventloop)) {

			String currentName = "John";
			String currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello, " + currentName + "!", currentResponse);

			currentName = "Winston";
			currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello Hello, " + currentName + "!", currentResponse);

			currentName = "Ann";
			currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello, " + currentName + "!", currentResponse);

			currentName = "Emma";
			currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello Hello, " + currentName + "!", currentResponse);

			currentName = "Lukas";
			currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello, " + currentName + "!", currentResponse);

			currentName = "Sophia"; // name starts with "s", so hash code is different from previous examples
			currentResponse = client.hello(currentName);
			System.out.println("Request with name \"" + currentName + "\": " + currentResponse);
			assertEquals("Hello Hello Hello, " + currentName + "!", currentResponse);

		} finally {
			serverOne.closeFuture().await();
			serverTwo.closeFuture().await();
			serverThree.closeFuture().await();

		}
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private interface HelloService {
		String hello(String name) throws Exception;
	}

	private static class HelloServiceImplOne implements HelloService {
		@Override
		public String hello(String name) throws Exception {
			if (name.equals("--")) {
				throw new Exception("Illegal name");
			}
			return "Hello, " + name + "!";
		}
	}

	private static class HelloServiceImplTwo implements HelloService {
		@Override
		public String hello(String name) throws Exception {
			if (name.equals("--")) {
				throw new Exception("Illegal name");
			}
			return "Hello Hello, " + name + "!";
		}
	}

	private static class HelloServiceImplThree implements HelloService {
		@Override
		public String hello(String name) throws Exception {
			if (name.equals("--")) {
				throw new Exception("Illegal name");
			}
			return "Hello Hello Hello, " + name + "!";
		}
	}

	protected static class HelloRequest {
		@Serialize(order = 0)
		public String name;

		public HelloRequest(@Deserialize("name") String name) {
			this.name = name;
		}
	}

	protected static class HelloResponse {
		@Serialize(order = 0)
		public String message;

		public HelloResponse(@Deserialize("message") String message) {
			this.message = message;
		}
	}

	private static RpcRequestHandler<HelloRequest> helloServiceRequestHandler(final HelloService helloService) {
		return new RpcRequestHandler<HelloRequest>() {
			@Override
			public void run(HelloRequest request, ResultCallback<Object> callback) {
				String result;
				try {
					result = helloService.hello(request.name);
				} catch (Exception e) {
					callback.onException(e);
					return;
				}
				callback.onResult(new HelloResponse(result));
			}
		};
	}

	private static class BlockingHelloClient implements HelloService, AutoCloseable {
		private final RpcClient client;

		public BlockingHelloClient(NioEventloop eventloop) throws Exception {

			InetSocketAddress address1 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), PORT_1);
			InetSocketAddress address2 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), PORT_2);
			InetSocketAddress address3 = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), PORT_3);

			Sharder<HelloRequest> sharder = new Sharder<HelloRequest>() {
				@Override
				public int getShard(HelloRequest item) {
					int shard = 0;
					if (item.name.startsWith("S")) {
						shard = 1;
					}
					return shard;
				}
			};

			this.client = RpcClient.create(eventloop, serializerFor(HelloRequest.class, HelloResponse.class))
					.strategy(
							roundRobin(
									server(address1),
									sharding(sharder,
											server(address2),
											server(address3))));

			startFuture(client).await();
		}

		@Override
		public String hello(final String name) throws Exception {
			try {
				ResultCallbackFuture<HelloResponse> future = client.sendRequestFuture(new HelloRequest(name), TIMEOUT);
				return future.get().message;
			} catch (ExecutionException e) {
				throw (Exception) e.getCause();
			}
		}

		@Override
		public void close() throws Exception {
			stopFuture(client).await();
		}
	}
}
