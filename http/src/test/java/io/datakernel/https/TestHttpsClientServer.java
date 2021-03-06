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

package io.datakernel.https;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.async.*;
import io.datakernel.dns.AsyncDnsClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.File;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncRunnables.runInParallel;
import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.HttpRequest.post;
import static io.datakernel.http.HttpResponse.ok200;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.https.SslUtils.*;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static junit.framework.TestCase.assertEquals;

public class TestHttpsClientServer {
	private static final int PORT = 5590;
	private static final int SSL_PORT = 5591;

	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
		//System.setProperty("javax.net.debug", "all");
	}

	private KeyManager[] keyManagers = createKeyManagers(new File("./src/test/resources/keystore.jks"), "testtest", "testtest");
	private TrustManager[] trustManagers = createTrustManagers(new File("./src/test/resources/truststore.jks"), "testtest");
	private AsyncServlet bobServlet = new AsyncServlet() {
		@Override
		public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
			callback.setResult(ok200().withBody(wrapAscii("Hello, I am Bob!")));
		}
	};
	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	private ExecutorService executor = newCachedThreadPool();
	private SSLContext context = createSslContext("TLSv1.2", keyManagers, trustManagers, new SecureRandom());

	public TestHttpsClientServer() throws Exception {
	}

	public static final InetAddress GOOGLE_PUBLIC_DNS = inetAddress("8.8.8.8");

	@Test
	public void testClientServerInteraction() throws Exception {
		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, bobServlet)
				.withSslListenPort(createSslContext("TLSv1.2", keyManagers, trustManagers, new SecureRandom()), executor, SSL_PORT);

		final AsyncDnsClient dnsClient = AsyncDnsClient.create(eventloop)
				.withTimeout(500)
				.withDnsServerAddress(GOOGLE_PUBLIC_DNS);
		final AsyncHttpClient client = AsyncHttpClient.create(eventloop)
				.withDnsClient(dnsClient)
				.withSslEnabled(createSslContext("TLSv1.2", keyManagers, trustManagers, new SecureRandom()), executor);

		HttpRequest request = post("https://127.0.0.1:" + SSL_PORT).withBody(wrapAscii("Hello, I am Alice!"));
		final ResultCallbackFuture<String> callback = ResultCallbackFuture.create();

		server.listen();

		client.send(request, 500, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				callback.setResult(decodeAscii(result.getBody()));
				server.close(IgnoreCompletionCallback.create());
				client.close();
			}

			@Override
			public void onException(Exception e) {
				callback.setException(e);
				server.close(IgnoreCompletionCallback.create());
				client.close();
			}
		});
		eventloop.run();
		executor.shutdown();

		assertEquals("Hello, I am Bob!", callback.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testServesTwoPortsSimultaneously() throws Exception {
		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, bobServlet)
				.withSslListenPort(context, executor, SSL_PORT)
				.withListenPort(PORT);

		final AsyncDnsClient dnsClient = AsyncDnsClient.create(eventloop)
				.withTimeout(500)
				.withDnsServerAddress(GOOGLE_PUBLIC_DNS);

		final AsyncHttpClient client = AsyncHttpClient.create(eventloop)
				.withDnsClient(dnsClient)
				.withSslEnabled(context, executor);

		final HttpRequest httpsRequest = post("https://127.0.0.1:" + SSL_PORT).withBody(wrapAscii("Hello, I am Alice!"));
		final HttpRequest httpRequest = post("http://127.0.0.1:" + PORT).withBody(wrapAscii("Hello, I am Alice!"));

		final ResultCallbackFuture<String> callbackHttps = ResultCallbackFuture.create();
		final ResultCallbackFuture<String> callbackHttp = ResultCallbackFuture.create();

		server.listen();

		runInParallel(eventloop, asList(
				new AsyncRunnable() {
					@Override
					public void run(final CompletionCallback callback) {
						client.send(httpsRequest, 500, new AssertingResultCallback<HttpResponse>() {
							@Override
							public void onResult(HttpResponse result) {
								callbackHttps.setResult(decodeAscii(result.getBody()));
								callback.setComplete();
							}
						});
					}
				},
				new AsyncRunnable() {
					@Override
					public void run(final CompletionCallback callback) {
						client.send(httpRequest, 500, new AssertingResultCallback<HttpResponse>() {
							@Override
							public void onResult(HttpResponse result) {
								callbackHttp.setResult(decodeAscii(result.getBody()));
								callback.setComplete();
							}
						});
					}
				}
		)).run(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server.close(IgnoreCompletionCallback.create());
				client.close();
			}
		});

		eventloop.run();
		executor.shutdown();

		assertEquals(callbackHttp.get(), callbackHttps.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}