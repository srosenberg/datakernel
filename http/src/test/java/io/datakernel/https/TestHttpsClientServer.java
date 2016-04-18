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
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.SslUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.http.SslUtils.createSslContext;
import static io.datakernel.net.DatagramSocketSettings.defaultDatagramSocketSettings;
import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.wrapAscii;
import static junit.framework.TestCase.assertEquals;

public class TestHttpsClientServer {
	private static final int PORT = 5568;

	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
	}

	@Ignore
	@Test
	public void test() throws Exception {
		Eventloop eventloop = new Eventloop();

//		final AsyncHttpServer server = new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
//			@Override
//			public void serveAsync(HttpRequest request, Callback callback) throws ParseException {
//				callback.onResult(HttpResponse.create().body(wrapAscii("Hello, I am Bob!")));
//			}
//		});
//
//		server.enableSsl(createSslContext("TLSv1.2",
//				SslUtils.createKeyManagers("./src/test/resources/keystore.jks", "testtest", "testtest"),
//				SslUtils.createTrustManagers("./src/test/resources/truststore.jks", "testtest"),
//				new SecureRandom()));
//
//		server.setListenPort(PORT);
//		server.listen();

		final AsyncHttpClient client = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, defaultDatagramSocketSettings(), 500, inetAddress("8.8.8.8")));

		client.enableSsl(createSslContext("TLSv1.2",
				SslUtils.createKeyManagers("./src/test/resources/keystore.jks", "testtest", "testtest"),
				SslUtils.createTrustManagers("./src/test/resources/truststore.jks", "testtest"),
				new SecureRandom()), Executors.newCachedThreadPool());

		HttpRequest request = HttpRequest.post("https://127.0.0.1:" + PORT).body(wrapAscii("Hello, I am Alice!"));

		final ResultCallbackFuture<String> callback = new ResultCallbackFuture<>();

		client.execute(request, 500000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				callback.onResult(decodeAscii(result.getBody()));
//				server.close();
				client.close();
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
//				server.close();
				client.close();
			}
		});
		eventloop.run();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertEquals("Hello, I am Bob!", callback.get());
	}
}
