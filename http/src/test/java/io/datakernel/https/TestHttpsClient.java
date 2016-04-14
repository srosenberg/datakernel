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
import io.datakernel.dns.DnsClient;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.net.DatagramSocketSettings;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import static io.datakernel.http.MediaTypes.*;
import static io.datakernel.util.ByteBufStrings.decodeUTF8;

public class TestHttpsClient {
	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
	}

	public static void main(String[] args) throws Exception {
		/*
			This property could be used to trace handshake process
			System.setProperty("javax.net.debug", "all");
		 */

		Eventloop eventloop = new Eventloop();
		DnsClient dns = new NativeDnsResolver(eventloop, DatagramSocketSettings.defaultDatagramSocketSettings(),
				500, HttpUtils.inetAddress("8.8.8.8"));

		final AsyncHttpClient client = new AsyncHttpClient(eventloop, dns);

		client.enableSsl(SSLContext.getDefault());

//		String url = "https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLEngine.html";
		String url = "https://github.com";
		client.execute(get(url), 1000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				System.out.println(decodeUTF8(result.detachBody()));
				client.close();
			}

			@Override
			public void onException(Exception ignore) {
				client.close();
			}
		});

		eventloop.run();
	}

	private static HttpRequest get(String url) {
		return HttpRequest.get(url)
				.header(HttpHeaders.CONNECTION, "keep-alive")
				.header(HttpHeaders.CACHE_CONTROL, "max-age=0")
				.header(HttpHeaders.ACCEPT_ENCODING, "gzip, deflate, sdch")
				.header(HttpHeaders.ACCEPT_LANGUAGE, "en-US,en;q=0.8")
				.header(HttpHeaders.USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36")
				.accept(AcceptMediaType.of(HTML),
						AcceptMediaType.of(XHTML_APP),
						AcceptMediaType.of(XML_APP, 90),
						AcceptMediaType.of(WEBP),
						AcceptMediaType.of(ANY, 80));
	}
}
