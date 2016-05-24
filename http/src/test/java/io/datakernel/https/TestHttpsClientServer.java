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
import io.datakernel.async.ParseException;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.io.File;
import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.http.HttpRequest.post;
import static io.datakernel.http.HttpResponse.create;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.https.SslUtils.*;
import static io.datakernel.net.DatagramSocketSettings.defaultDatagramSocketSettings;
import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.wrapAscii;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static junit.framework.TestCase.assertEquals;

public class TestHttpsClientServer {
	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
		//System.setProperty("javax.net.debug", "all");
	}

	@Test
	public void testClientServerInteraction() throws Exception {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();

		AsyncHttpServlet bobServlet = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) throws ParseException {
				callback.onResult(create().body(wrapAscii("Hello, I am Bob!")));
			}
		};

		KeyManager[] keyManagers = createKeyManagers(new File("./src/test/resources/keystore.jks"), "testtest", "testtest");
		TrustManager[] trustManagers = createTrustManagers(new File("./src/test/resources/truststore.jks"), "testtest");

		final AsyncHttpServer server = new AsyncHttpServer(eventloop, bobServlet)
				.enableSsl(createSslContext("TLSv1.2", keyManagers, trustManagers, new SecureRandom()), executor)
				.setListenPort(5590);

		final AsyncHttpClient client = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, defaultDatagramSocketSettings(), 500, inetAddress("8.8.8.8")))
				.enableSsl(createSslContext("TLSv1.2", keyManagers, trustManagers, new SecureRandom()), executor);

		HttpRequest request = post("https://127.0.0.1:" + 5590).body(wrapAscii("Hello, I am Alice!"));
		final ResultCallbackFuture<String> callback = new ResultCallbackFuture<>();

		server.listen();

		client.execute(request, 500, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				callback.onResult(decodeAscii(result.getBody()));
				server.close();
				client.close();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
				server.close();
				client.close();
			}
		});
		eventloop.run();
		executor.shutdown();

		assertEquals("Hello, I am Bob!", callback.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}