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

package io.datakernel.stream.net;

import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import com.google.gson.Gson;
import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.*;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.net.Messaging.ReadCallback;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.serializer.asm.BufferSerializers.longSerializer;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class MessagingConnectionTest {
	private static final int LISTEN_PORT = 4821;
	private static final InetSocketAddress address = new InetSocketAddress(InetAddresses.forString("127.0.0.1"), LISTEN_PORT);

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void testPing() throws Exception {
		final Eventloop eventloop = new Eventloop();

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				MessagingConnection<Integer, Integer> messaging = new MessagingConnection<>(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), Integer.class, new Gson(), Integer.class));
				pong(messaging);
				return messaging;
			}

			void pong(final Messaging<Integer, Integer> messaging) {
				messaging.read(new ReadCallback<Integer>() {
					@Override
					public void onRead(Integer msg) {
						messaging.write(msg, ignoreCompletionCallback());
						pong(messaging);
					}

					@Override
					public void onReadEndOfStream() {
						messaging.close();
					}

					@Override
					public void onException(Exception e) {
						messaging.close();
					}
				});
			}

		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					void ping(int n, final Messaging<Integer, Integer> messaging) {
						messaging.write(n, ignoreCompletionCallback());

						messaging.read(new ReadCallback<Integer>() {
							@Override
							public void onRead(Integer msg) {
								if (msg > 0) {
									ping(msg - 1, messaging);
								} else {
									messaging.close();
								}
							}

							@Override
							public void onReadEndOfStream() {
								// empty
							}

							@Override
							public void onException(Exception e) {
								messaging.close();
							}
						});
					}

					@Override
					public AsyncTcpSocket.EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
						MessagingConnection<Integer, Integer> messaging = new MessagingConnection<>(eventloop, asyncTcpSocket,
								MessagingSerializers.ofGson(new Gson(), Integer.class, new Gson(), Integer.class));
						ping(3, messaging);
						return messaging;
					}

					@Override
					public void onException(Exception exception) {
						fail("Test Exception: " + exception);
					}
				}
		);

		eventloop.run();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testMessagingDownload() throws Exception {
		final List<Long> source = Lists.newArrayList();
//		for (long i = 0; i < 100; i++) {
//			source.add(i);
//		}

		final Eventloop eventloop = new Eventloop();

		List<Long> l = new ArrayList<>();
		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop, l);

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				final MessagingConnection<String, String> messaging = new MessagingConnection<>(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.read(new ReadCallback<String>() {
					@Override
					public void onRead(String msg) {
						assertEquals("start", msg);
						StreamBinarySerializer<Long> streamSerializer = new StreamBinarySerializer<>(eventloop, longSerializer(), 1, 10, 0, false);
						StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
						messaging.writeStream(streamSerializer.getOutput(), ignoreCompletionCallback());
					}

					@Override
					public void onReadEndOfStream() {

					}

					@Override
					public void onException(Exception e) {
					}
				});

				return messaging;
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public AsyncTcpSocket.EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
						MessagingConnection<String, String> messaging = new MessagingConnection<>(eventloop, asyncTcpSocket,
								MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

						messaging.write("start", ignoreCompletionCallback());
						messaging.writeEndOfStream(ignoreCompletionCallback());

						StreamBinaryDeserializer<Long> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, longSerializer(), 10);
						messaging.readStream(streamDeserializer.getInput(), ignoreCompletionCallback());
						streamDeserializer.getOutput().streamTo(consumerToList);

						return messaging;
					}

					@Override
					public void onException(Exception e) {
						fail("Test Exception: " + e);
					}
				}
		);

		eventloop.run();
		assertEquals(source, consumerToList.getList());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testBinaryMessagingUpload() throws Exception {
		final List<Long> source = Lists.newArrayList();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final Eventloop eventloop = new Eventloop();

		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop);

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				final MessagingConnection<String, String> messaging = new MessagingConnection<>(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.read(new ReadCallback<String>() {
					@Override
					public void onRead(String message) {
						assertEquals("start", message);

						StreamBinaryDeserializer<Long> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, longSerializer(), 10);
						messaging.readStream(streamDeserializer.getInput(), ignoreCompletionCallback());
						streamDeserializer.getOutput().streamTo(consumerToList);

						messaging.writeEndOfStream(ignoreCompletionCallback());
					}

					@Override
					public void onReadEndOfStream() {

					}

					@Override
					public void onException(Exception e) {
					}
				});

				return messaging;
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public AsyncTcpSocket.EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
						MessagingConnection<String, String> messaging = new MessagingConnection<>(eventloop, asyncTcpSocket,
								MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

						messaging.write("start", ignoreCompletionCallback());

						StreamBinarySerializer<Long> streamSerializer = new StreamBinarySerializer<>(eventloop, longSerializer(), 1, 10, 0, false);
						StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
						messaging.writeStream(streamSerializer.getOutput(), ignoreCompletionCallback());

						return messaging;
					}

					@Override
					public void onException(Exception e) {
						fail("Test Exception: " + e);
					}
				}
		);

		eventloop.run();

		assertEquals(source, consumerToList.getList());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testBinaryMessagingUploadAck() throws Exception {
		final List<Long> source = Lists.newArrayList();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final boolean[] ack = new boolean[]{false};

		final Eventloop eventloop = new Eventloop();

		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop);

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				final MessagingConnection<String, String> messaging = new MessagingConnection<>(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.read(new ReadCallback<String>() {
					@Override
					public void onRead(String msg) {
						assertEquals("start", msg);

						StreamBinaryDeserializer<Long> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, longSerializer(), 10);
						streamDeserializer.getOutput().streamTo(consumerToList);
						messaging.readStream(streamDeserializer.getInput(), new CompletionCallback() {
							@Override
							public void onComplete() {
								messaging.write("ack", ignoreCompletionCallback());
								messaging.writeEndOfStream(ignoreCompletionCallback());
							}

							@Override
							public void onException(Exception exception) {
							}
						});
					}

					@Override
					public void onReadEndOfStream() {

					}

					@Override
					public void onException(Exception exception) {
					}
				});

				return messaging;
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public AsyncTcpSocket.EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
						final MessagingConnection<String, String> messaging = new MessagingConnection<>(eventloop, asyncTcpSocket,
								MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

						messaging.write("start", ignoreCompletionCallback());

						StreamBinarySerializer<Long> streamSerializer = new StreamBinarySerializer<>(eventloop, longSerializer(), 1, 10, 0, false);
						StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
						messaging.writeStream(streamSerializer.getOutput(), ignoreCompletionCallback());

						messaging.read(new ReadCallback<String>() {
							@Override
							public void onRead(String msg) {
								assertEquals("ack", msg);
								messaging.close();
								ack[0] = true;
							}

							@Override
							public void onReadEndOfStream() {

							}

							@Override
							public void onException(Exception exception) {

							}
						});

						return messaging;
					}

					@Override
					public void onException(Exception e) {
						fail("Test Exception: " + e);
					}
				}
		);

		eventloop.run();

		assertEquals(source, consumerToList.getList());
		assertTrue(ack[0]);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testGsonMessagingUpload() throws Exception {
		final List<Long> source = Lists.newArrayList();
		for (long i = 0; i < 100; i++) {
			source.add(i);
		}

		final Eventloop eventloop = new Eventloop();

		final StreamConsumers.ToList<Long> consumerToList = StreamConsumers.toList(eventloop);

		AbstractServer server = new AbstractServer(eventloop) {
			@Override
			protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {
				final MessagingConnection<String, String> messaging = new MessagingConnection<>(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

				messaging.read(new ReadCallback<String>() {
					@Override
					public void onRead(String msg) {
						assertEquals("start", msg);

						messaging.writeEndOfStream(ignoreCompletionCallback());

						StreamBinaryDeserializer<Long> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, longSerializer(), 10);
						messaging.readStream(streamDeserializer.getInput(), ignoreCompletionCallback());

						streamDeserializer.getOutput().streamTo(consumerToList);
					}

					@Override
					public void onReadEndOfStream() {

					}

					@Override
					public void onException(Exception exception) {

					}
				});

				return messaging;
			}
		};
		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public AsyncTcpSocket.EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
						MessagingConnection<String, String> messaging = new MessagingConnection<>(eventloop, asyncTcpSocket,
								MessagingSerializers.ofGson(new Gson(), String.class, new Gson(), String.class));

						messaging.write("start", ignoreCompletionCallback());

						StreamBinarySerializer<Long> streamSerializer = new StreamBinarySerializer<>(eventloop, longSerializer(), 1, 10, 0, false);
						StreamProducers.ofIterable(eventloop, source).streamTo(streamSerializer.getInput());
						messaging.writeStream(streamSerializer.getOutput(), ignoreCompletionCallback());

						return messaging;
					}

					@Override
					public void onException(Exception e) {
						fail("Test Exception: " + e);
					}
				}
		);

		eventloop.run();

		assertEquals(source, consumerToList.getList());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

}