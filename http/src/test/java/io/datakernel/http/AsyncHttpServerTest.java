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

package io.datakernel.http;

import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.SocketSettings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Random;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.AsyncTcpSocketImpl.wrapChannel;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.TestUtils.readFully;
import static io.datakernel.http.TestUtils.toByteArray;
import static java.lang.Math.min;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AsyncHttpServerTest {
	private final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	private static final int PORT = 44000;

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void testKeepAlive() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);

		doTestKeepAlive(eventloop, blockingHttpServer(eventloop, port), new InetSocketAddress(port));
		doTestKeepAlive(eventloop, asyncHttpServer(eventloop, port), new InetSocketAddress(port));
		doTestKeepAlive(eventloop, delayedHttpServer(eventloop, port), new InetSocketAddress(port));

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testClosed() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		AsyncHttpServer server = blockingHttpServer(eventloop, port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress(port));
		writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\n");
		socket.close();

		server.closeFuture().get();
		thread.join();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testNoKeepAlive() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
		AsyncHttpServer server = blockingHttpServer(eventloop, port);
		server.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();

		socket.connect(new InetSocketAddress(port));
		writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n/abc");
		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		socket.close();

		server.closeFuture().get();
		thread.join();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testPipelining() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		int port = (int) (System.currentTimeMillis() % 1000 + 40000);
//		doTestPipelining(eventloop, blockingHttpServer(eventloop));
//		doTestPipelining(eventloop, asyncHttpServer(eventloop));
		doTestPipelining(eventloop, delayedHttpServer(eventloop, port), port);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private void doTestPipelining(Eventloop eventloop, AsyncHttpServer server, int port) throws Exception {
		server.withListenPort(port);
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.connect(new InetSocketAddress(port));

		for (int i = 0; i < 100; i++) {
			writeByRandomParts(socket, "GET /abc HTTP1.1\r\nConnection: Keep-Alive\r\nHost: localhost\r\n\r\n"
					+ "GET /123456 HTTP1.1\r\nConnection: Keep-Alive\r\nHost: localhost\r\n\r\n");
		}

		for (int i = 0; i < 100; i++) {
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 7\r\n\r\n/123456");
		}

		server.closeFuture().get();
		thread.join();
	}

	@Test
	public void testBigHttpMessage() throws Exception {
		// Arrange
		final AsyncServlet servlet = new AsyncServlet() {
			@Override
			public void serve(final HttpRequest request, final ResultCallback<HttpResponse> callback) {
				final HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				callback.setResult(content);
			}
		};

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withMaxHttpMessageSize(25)
				.withListenPort(PORT);

		final ByteBuf buf = HttpRequest.post("http://127.0.0.1:" + PORT)
				.withBody(ByteBuf.wrapForReading(encodeAscii("(\"Test big HTTP message body")))
				.toByteBuf();

		// Act
		server.listen();
		sendByteBufTo(new InetSocketAddress(PORT), buf, eventloop, new SimpleCompletionCallback() {
			@Override
			protected void onCompleteOrException() {
				server.close(IgnoreCompletionCallback.create());
			}
		});
		eventloop.run();

		// Assert
		assertEquals(1, fetchTotalIOErrors(eventloop));
		assertEquals("Too big HttpMessage", fetchLastIOErrorMessage(eventloop));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	static String fetchLastIOErrorMessage(Eventloop eventloop) {
		return eventloop.getStats().getErrorStats().getIoErrors().getLastException().getMessage();
	}

	static int fetchTotalIOErrors(Eventloop eventloop) {
		return eventloop.getStats().getErrorStats().getIoErrors().getTotal();
	}

	static AsyncHttpServer blockingHttpServer(Eventloop primaryEventloop, int port) {
		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
				HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				callback.setResult(content);
			}
		};

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenPort(port);
	}

	static AsyncHttpServer asyncHttpServer(final Eventloop primaryEventloop, int port) {
		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public void serve(final HttpRequest request, final ResultCallback<HttpResponse> callback) {
				final HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				primaryEventloop.post(new Runnable() {
					@Override
					public void run() {
						callback.setResult(content);
					}
				});
			}
		};

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenPort(port);
	}

	static AsyncHttpServer delayedHttpServer(final Eventloop primaryEventloop, int port) {
		final Random random = new Random();
		AsyncServlet servlet = new AsyncServlet() {
			@Override
			public void serve(final HttpRequest request, final ResultCallback<HttpResponse> callback) {
				final HttpResponse content = HttpResponse.ok200().withBody(encodeAscii(request.getUrl().getPathAndQuery()));
				primaryEventloop.schedule(primaryEventloop.currentTimeMillis() + random.nextInt(3), new Runnable() {
					@Override
					public void run() {
						callback.setResult(content);
					}
				});
			}
		};

		return AsyncHttpServer.create(primaryEventloop, servlet).withListenPort(port);
	}

	static void writeByRandomParts(Socket socket, String string) throws IOException {
		ByteBuf buf = ByteBuf.wrapForReading(encodeAscii(string));
		Random random = new Random();
		while (buf.canRead()) {
			int count = min(1 + random.nextInt(5), buf.readRemaining());
			socket.getOutputStream().write(buf.array(), buf.readPosition(), count);
			buf.moveReadPosition(count);
		}
	}

	static void readAndAssert(InputStream is, String expected) throws IOException {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		Assert.assertEquals(expected, decodeAscii(bytes));
	}

	static void sendByteBufTo(InetSocketAddress addr,
							  final ByteBuf buf, final Eventloop eventloop,
							  final CompletionCallback callback) {
		eventloop.connect(addr, 0, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				final AsyncTcpSocketImpl socket = wrapChannel(eventloop, socketChannel, SocketSettings.create());
				final EventHandler handler = new EventHandler() {
					@Override
					public void onRegistered() {
						socket.write(buf);
					}

					@Override
					public void onRead(ByteBuf buf) {

					}

					@Override
					public void onReadEndOfStream() {

					}

					@Override
					public void onWrite() {
						socket.close();
						callback.setComplete();
					}

					@Override
					public void onClosedWithError(Exception e) {
						fail();
					}
				};
				socket.setEventHandler(handler);
				socket.register();
			}

			@Override
			public void onException(Exception e) {
				fail();
			}
		});

	}

	private void doTestKeepAlive(Eventloop eventloop, AsyncHttpServer server, InetSocketAddress address) throws Exception {
		server.listen();
		Thread thread = new Thread(eventloop);
		thread.start();

		Socket socket = new Socket();
		socket.setTcpNoDelay(true);
		socket.connect(address);

		for (int i = 0; i < 100; i++) {
			writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");

			writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n");
			readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 4\r\n\r\n/abc");
		}

		writeByRandomParts(socket, "GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: close\n\r\n");
		readAndAssert(socket.getInputStream(), "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n/abc"); // ?

		assertTrue(toByteArray(socket.getInputStream()).length == 0);
		assertTrue(socket.isClosed());
		socket.close();

		server.closeFuture().get();
		thread.join();
	}

	public static void main(String[] args) throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		AsyncHttpServer server = blockingHttpServer(eventloop, 8888);
		server.listen();
		eventloop.run();
	}
}
