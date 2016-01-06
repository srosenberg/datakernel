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

package io.datakernel;

import com.google.common.net.InetAddresses;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.http.*;
import io.datakernel.http.server.AsyncHttpServlet;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static org.junit.Assert.assertEquals;

public class HttpApiTest {
	public static final int PORT = 5568;

	private NioEventloop eventloop;
	private AsyncHttpServer server;
	private AsyncHttpClient client;

	// request
	private List<AcceptContentType> requestAcceptContentTypes = new ArrayList<>();
	private List<AcceptCharset> requestAcceptCharsets = new ArrayList<>();
	private Date requestDate = new Date();
	private Date dateIMS = new Date();
	private Date dateIUMS = new Date();
	private MediaType requestMime = MediaTypes.ANY_TEXT;
	private ContentType requestContentType = ContentType.of(requestMime);
	private List<HttpCookie> requestCookies = new ArrayList<>();

	// response
	private Date responseDate = new Date();
	private Date expiresDate = new Date();
	private Date lastModified = new Date();
	private MediaType responseMime = MediaType.of("font/woff2");
	private Charset responseCharset = StandardCharsets.UTF_16LE;
	private ContentType responseContentType = ContentType.of(responseMime, responseCharset);
	private List<HttpCookie> responseCookies = new ArrayList<>();
	private int age = 10_000;

	final ResultCallbackFuture<HttpResponse> resultObserver = new ResultCallbackFuture<>();

	@Before
	public void setUp() {
		eventloop = new NioEventloop();
		server = new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, ResultCallback<HttpResponse> callback) {
				testRequest(request);
				HttpResponse response = createResponse();
				callback.onResult(response);
			}
		}).setListenPort(PORT);

		client = new AsyncHttpClient(eventloop, new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS,
				3_000L, InetAddresses.forString("8.8.8.8")));

		// setup request and response data
		requestAcceptContentTypes.add(AcceptContentType.of(MediaTypes.ANY_AUDIO, 90));
		requestAcceptContentTypes.add(AcceptContentType.of(MediaTypes.ANY));
		requestAcceptContentTypes.add(AcceptContentType.of(MediaTypes.ATOM));
		requestAcceptContentTypes.add(AcceptContentType.of(MediaType.of("hello/world")));

		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("UTF-8")));
		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("ISO-8859-5"), 10));
		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("ISO-8859-2"), 10));
		requestAcceptCharsets.add(AcceptCharset.of(Charset.forName("ISO-8859-3"), 10));

		HttpCookie cookie2 = new HttpCookie("name1", "value1");
		requestCookies.add(cookie2);
		HttpCookie cookie3 = new HttpCookie("name3");
		requestCookies.add(cookie3);

		HttpCookie cookie1 = new HttpCookie("name2", "value2");
		cookie1.setMaxAge(123);
		cookie1.setExpirationDate(new Date());
		responseCookies.add(cookie1);
	}

	@Test
	public void test() throws IOException, ExecutionException, InterruptedException {
		server.listen();
		HttpRequest request = createRequest();
		client.execute(request, 3000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				resultObserver.onResult(result);
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

		testResponse(resultObserver.get());
	}

	private HttpResponse createResponse() {
		HttpResponse response = HttpResponse.create();
		response.setDate(responseDate);
		response.setExpires(expiresDate);
		response.setContentType(responseContentType);
		response.setServerCookie(responseCookies);
		response.setAge(age);
		return response;
	}

	private HttpRequest createRequest() {
		HttpRequest request = HttpRequest.get("http://127.0.0.1:" + PORT);
		request.setAccept(requestAcceptContentTypes);
		request.setAcceptCharsets(requestAcceptCharsets);
		request.setDate(requestDate);
		request.setContentType(requestContentType);
		request.setIfModifiedSince(dateIMS);
		request.setIfUnModifiedSince(dateIUMS);
		request.setClientCookies(requestCookies);
		return request;
	}

	private void testResponse(HttpResponse response) {
		assertEquals(responseContentType, response.getContentType());
		assertEquals(responseCookies, response.getCookies());
		assertEquals(responseDate, response.getDate());
		assertEquals(age, response.getAge());
		assertEquals(expiresDate, response.getExpires());
		assertEquals(lastModified, response.getLastModified());
	}

	private void testRequest(HttpRequest request) {
		assertEquals(requestAcceptContentTypes, request.getAccept());
		assertEquals(requestAcceptCharsets, request.getAcceptCharsets());
		assertEquals(requestDate, request.getDate());
		assertEquals(dateIMS, request.getIfModifiedSince());
		assertEquals(dateIUMS, request.getIfUnModifiedSince());
		assertEquals(requestContentType, request.getContentType());
		assertEquals(requestCookies, request.getCookies());
	}
}
