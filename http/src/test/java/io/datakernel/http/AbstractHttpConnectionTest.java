package io.datakernel.http;

import io.datakernel.async.ParseException;
import io.datakernel.async.ResultCallback;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.util.ByteBufStrings;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AbstractHttpConnectionTest {

	public static final int PORT = 5050;

	@Test
	public void testMultiLineHeader() throws IOException {
		final Map<String, String> data = new HashMap<>();

		Eventloop eventloop = new Eventloop();
		final AsyncHttpServer server = new AsyncHttpServer(eventloop, new AsyncHttpServlet() {
			@Override
			public void serveAsync(HttpRequest request, Callback callback) throws ParseException {
				callback.onResult(createMultiLineHeaderWithInitialBodySpacesResponse());
			}
		});
		server.setListenPort(PORT);
		server.listen();

		final AsyncHttpClient client = new AsyncHttpClient(eventloop,
				new NativeDnsResolver(eventloop, new DatagramSocketSettings(), 300, HttpUtils.inetAddress("8.8.8.8")));

		client.execute(HttpRequest.get("http://127.0.0.1:" + PORT), 50000, new ResultCallback<HttpResponse>() {
			@Override
			public void onResult(HttpResponse result) {
				data.put("body", ByteBufStrings.decodeAscii(result.getBody()));
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
		assertEquals(data.get("header"), "text/           html");
		assertEquals(data.get("body"), "  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>");
	}

	private HttpResponse createMultiLineHeaderWithInitialBodySpacesResponse() {
		return HttpResponse.create()
				.header(HttpHeaders.DATE, "Mon, 27 Jul 2009 12:28:53 GMT")
				.header(HttpHeaders.CONTENT_TYPE, "text/\n          html")
				.body(ByteBufStrings.wrapAscii("  <html>\n<body>\n<h1>Hello, World!</h1>\n</body>\n</html>"));
	}
}