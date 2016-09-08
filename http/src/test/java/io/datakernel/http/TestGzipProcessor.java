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

import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.http.GzipProcessor.fromGzip;
import static io.datakernel.http.GzipProcessor.toGzip;
import static io.datakernel.http.HttpHeaders.ACCEPT_ENCODING;
import static io.datakernel.http.HttpResponse.ok200;
import static io.datakernel.http.HttpUtils.inetAddress;
import static io.datakernel.net.DatagramSocketSettings.defaultDatagramSocketSettings;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertThat;

public class TestGzipProcessor {
	private static final int PORT = 5595;
	private static final int TIMEOUT = 500;
	private static final String TEST_PHRASE = "I grant! I've never seen a goddess go. My mistress, when she walks, treads on the ground";

	@Test
	public void testEncodeDecode() throws ParseException {
		ByteBuf actual = fromGzip(toGzip(wrapAscii(TEST_PHRASE)));
		assertEquals(TEST_PHRASE, decodeAscii(actual));
		actual.recycle();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testGzippedCommunicationBetweenClientServer() throws IOException, ParseException, ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create();
		AsyncHttpServlet servlet = new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) throws ParseException {
				String receivedData = ByteBufStrings.decodeAscii(request.getBody());
				assertEquals(TEST_PHRASE, receivedData);
				callback.onResult(ok200().withBody(ByteBufStrings.wrapAscii(receivedData)));
			}
		};

		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, servlet)
				.withListenPort(PORT);

		final AsyncHttpClient client = AsyncHttpClient.create(eventloop,
				NativeDnsResolver.create(eventloop, defaultDatagramSocketSettings(), 500, inetAddress("8.8.8.8")));

		final ResultCallbackFuture<String> callback = ResultCallbackFuture.create();

		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT)
				.withHeader(ACCEPT_ENCODING, "gzip")
				.withBody(wrapAscii(TEST_PHRASE))
				.withGzipCompression();

		server.listen();
		client.send(request, TIMEOUT, new ResultCallback<HttpResponse>() {
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
		assertEquals(TEST_PHRASE, callback.get());
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}
}
