package io.datakernel.http;

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.net.DatagramSocketSettings;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class AbstractHttpConnectionTest {
	private static final int PORT = 5050;

	@Test
	public void testMultiLineHeader() throws IOException {
		final Map<String, String> data = new HashMap<>();

		Eventloop eventloop = Eventloop.create();
		final AsyncHttpServer server = AsyncHttpServer.create(eventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) throws ParseException {
				callback.onResult(createMultiLineHeaderWithInitialBodySpacesResponse());
			}
		});
		server.withListenPort(PORT);
		server.listen();

		final AsyncHttpClient client = AsyncHttpClient.create(eventloop,
				NativeDnsResolver.create(eventloop)
						.withDatagramSocketSetting(DatagramSocketSettings.create())
						.withTimeout(300).withDnsServerAddress(HttpUtils.inetAddress("8.8.8.8"))
		);

		client.send(HttpRequest.get("http://127.0.0.1:" + PORT), 50000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				data.put("body", decodeAscii(result.getBody()));
				data.put("header", result.getHeader(HttpHeaders.CONTENT_TYPE));
				client.close();
				server.close();
			}

			@Override
			public void onException(Exception e) {
				e.printStackTrace();
				client.close();
				server.close();
			}
		});

		eventloop.run();
		assertEquals("text/           html", data.get("header"));
		assertEquals("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>", data.get("body"));
		assertThat(eventloop, doesntHaveFatals());
	}

	private HttpResponse createMultiLineHeaderWithInitialBodySpacesResponse() {
		return HttpResponse.ok200()
				.withHeader(HttpHeaders.DATE, "Mon, 27 Jul 2009 12:28:53 GMT")
				.withHeader(HttpHeaders.CONTENT_TYPE, "text/\n          html")
				.withBody(ByteBufStrings.wrapAscii("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>"));
	}
}