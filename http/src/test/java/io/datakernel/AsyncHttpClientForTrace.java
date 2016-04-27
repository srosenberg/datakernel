package io.datakernel;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.async.ResultCallback;
import io.datakernel.dns.NativeDnsResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.dns.NativeDnsResolver.DEFAULT_DATAGRAM_SOCKET_SETTINGS;
import static io.datakernel.http.HttpUtils.inetAddress;

public class AsyncHttpClientForTrace {

	static {
		Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		logger.setLevel(Level.TRACE);
	}

	public static void main(String[] args) {
		final Eventloop eventloop = new Eventloop();
		NativeDnsResolver dns = new NativeDnsResolver(eventloop, DEFAULT_DATAGRAM_SOCKET_SETTINGS, 3_000L, inetAddress("8.8.8.8"));
		AsyncHttpClient client = new AsyncHttpClient(eventloop, dns);

		client.setMaxConnectionsPerIp(100);

		final ExecutorService executor = Executors.newCachedThreadPool();

		final int[] connects = {0, 0};

		executor.submit(new Runnable() {
			@Override
			public void run() {
				eventloop.keepAlive(true);
				eventloop.run();
				System.out.println("finished: " + connects[0] + ", failed: " + connects[1]);
				executor.shutdownNow();
			}
		});

		int numberOfRequests = 100_000;
		System.out.println("sending " + numberOfRequests + " requests!");
		for (int i = 0; i < numberOfRequests; i++) {

			try {
				Thread.sleep(1);
			} catch (InterruptedException ignored) {
			}

			client.execute(HttpRequest.get("http://127.0.0.1:5588"), 10000, new ResultCallback<HttpResponse>() {
				@Override
				public void onResult(HttpResponse result) {
					System.out.println("executed");
					connects[0]++;
				}

				@Override
				public void onException(Exception e) {
					if(e.getMessage() == null) {
						System.err.println(e);
					} else {
						System.err.println(e.getMessage());
					}
					connects[1]++;
				}
			});
		}

		eventloop.keepAlive(false);
	}
}
