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
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.ByteBufStrings;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static org.junit.Assert.assertEquals;

public class HttpServerConnectionTest {
	private static class Tuple {
		public final int code;
		public final String msg;

		public Tuple(int code, String msg) {
			this.code = code;
			this.msg = msg;
		}
	}

	/*
	 *  type: http, runtime, common or null
	 *  code: any int > 0
	 *  msg: nullable string
	 */
	private static class AsyncHttpMockServlet implements AsyncHttpServlet {
		@Override
		public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
			int code = Integer.parseInt(request.getParameter("code"));
			String type = request.getParameter("type");
			String msg = request.getParameter("msg");

			if (type == null) {
				callback.onResult(HttpResponse.create());
			} else if (type.equals("http")) {
				callback.onException(new HttpException(code, msg));
			} else if (type.equals("runtime")) {
				callback.onException(new RuntimeException(msg));
			} else if (type.equals("common")) {
				callback.onException(new Exception(msg));
			} else {
				throw new RuntimeException("unknown type");
			}

		}
	}

	public static final int PORT = 5566;

	@Test
	public void testHttpExceptionWithCustomMsg() throws Exception {
		Tuple t = makeRequest(400, "http", "CustomNotFoundMessage");
		assertEquals(400, t.code);
		assertEquals("CustomNotFoundMessage", t.msg);
	}

	@Test
	public void testHttpExceptionStandardMsg() throws Exception {
		Tuple t = makeRequest(400, "http", null);
		assertEquals(400, t.code);
		assertEquals("Bad Request", t.msg);
	}

	@Test
	public void testExceptionStandardMsg() throws Exception {
		Tuple t = makeRequest(0, "common", null);
		assertEquals(500, t.code);
		assertEquals("Failed to process request", t.msg);
	}

	@Test(expected = ExecutionException.class)
	public void testRuntimeExceptionBehavior() throws Exception {
		makeRequest(0, "", null);
	}

	private Tuple makeRequest(int code, String type, String msg) throws Exception {
		Eventloop eventloop = new Eventloop();
		AsyncHttpMockServlet mockServlet = new AsyncHttpMockServlet();
		final AsyncHttpServer server = new AsyncHttpServer(eventloop, mockServlet);

		server.setListenPort(PORT);
		server.listen();

		final AsyncHttpClient client = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS,
						3_000L, HttpUtils.inetAddress("8.8.8.8")));

		final ResultCallbackFuture<Tuple> resultObserver = new ResultCallbackFuture<>();

		HttpRequest request = HttpRequest.get(
				"http://127.0.0.1:" + PORT
						+ "/?code=" + code
						+ (type != null ? "&type=" + type : "")
						+ (msg != null ? "&msg=" + msg : ""));

		client.execute(request, 3000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				resultObserver.onResult(new Tuple(result.getCode(), ByteBufStrings.decodeAscii(result.getBody())));
				server.close();
				client.close();
			}

			@Override
			public void onException(Exception e) {
				resultObserver.onException(e);
				server.close();
				client.close();
			}
		});

		eventloop.run();

		return resultObserver.get();
	}
}
