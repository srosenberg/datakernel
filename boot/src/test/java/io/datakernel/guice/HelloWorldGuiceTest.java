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

package io.datakernel.guice;

import com.google.common.io.Closeables;
import com.google.inject.*;
import com.google.inject.name.Named;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import io.datakernel.jmx.JmxModule;
import io.datakernel.jmx.JmxRegistrator;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;
import io.datakernel.worker.WorkerPool;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import static com.google.common.io.ByteStreams.readFully;
import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.http.HttpResponse.ok200;
import static org.junit.Assert.assertEquals;

public class HelloWorldGuiceTest {
	public static final int PORT = 7583;
	public static final int WORKERS = 4;

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	public static class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			install(ServiceGraphModule.defaultInstance());
		}

		@Provides
		@Singleton
		WorkerPool workerPools() {
			return new WorkerPool(WORKERS);
		}

		@Provides
		@Singleton
		@Named("PrimaryEventloop")
		Eventloop primaryEventloop() {
//			return Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
			return Eventloop.create();
		}

		@Provides
		@Singleton
		PrimaryServer primaryServer(@Named("PrimaryEventloop") Eventloop primaryEventloop, WorkerPool workerPool) {
			List<AsyncHttpServer> workerHttpServers = workerPool.getInstances(AsyncHttpServer.class);
			return PrimaryServer.create(primaryEventloop, workerHttpServers).withListenPort(PORT);
		}

		@Provides
		@Worker
		Eventloop workerEventloop() {
//			return Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
			return Eventloop.create();
		}

		@Provides
		@Worker
		AsyncHttpServer workerHttpServer(Eventloop eventloop, AsyncServlet servlet) {
			return AsyncHttpServer.create(eventloop, servlet);
		}

		@Provides
		@Worker
		AsyncServlet servlet(@WorkerId final int workerId) {
			return new AsyncServlet() {
				@Override
				public void serve(HttpRequest request, ResultCallback<HttpResponse> callback) {
					byte[] body = ByteBufStrings.encodeAscii("Hello world: worker server #" + workerId);
					callback.setResult(ok200().withBody(ByteBuf.wrapForReading(body)));
				}
			};
		}
	}

	@Test
	public void test() throws Exception {
		Injector injector = Guice.createInjector(Stage.PRODUCTION, new TestModule());
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		Socket socket0 = new Socket(), socket1 = new Socket();
		try {
			serviceGraph.startFuture().get();

			socket0.connect(new InetSocketAddress(PORT));
			socket1.connect(new InetSocketAddress(PORT));

			for (int i = 0; i < 10; i++) {
				socket0.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket0.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 29\r\n\r\nHello world: worker server #0");

				socket0.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket0.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 29\r\n\r\nHello world: worker server #0");

				socket1.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 29\r\n\r\nHello world: worker server #1");

				socket1.getOutputStream().write(encodeAscii("GET /abc HTTP1.1\r\nHost: localhost\r\nConnection: keep-alive\n\r\n"));
				readAndAssert(socket1.getInputStream(), "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 29\r\n\r\nHello world: worker server #1");
			}
		} finally {
			serviceGraph.stopFuture().get();
			Closeables.close(socket0, true);
			Closeables.close(socket1, true);
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	public static void readAndAssert(InputStream is, String expected) throws IOException {
		byte[] bytes = new byte[expected.length()];
		readFully(is, bytes);
		assertEquals(expected, decodeAscii(bytes));
	}

	public static void main(String[] args) throws Exception {
		ByteBuf buf = ByteBufPool.allocate(10_000);

		Thread.sleep(1000);
		ByteBuf buf2 = ByteBufPool.allocate(1_000);
		buf2.write(new byte[]{'a', 'b', 'c'});

		Thread.sleep(1000);
		ByteBuf buf3 = ByteBufPool.allocate(10_000);
		buf3.write(new byte[]{'a', 'b', 'c'});
		buf3.readByte();


		Injector injector = Guice.createInjector(Stage.PRODUCTION, new TestModule(), JmxModule.create());

		// jmx
		JmxRegistrator jmxRegistrator = injector.getInstance(JmxRegistrator.class);
		jmxRegistrator.registerJmxMBeans();

		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		try {
			serviceGraph.startFuture().get();

			System.out.println("Server started, press enter to stop it.");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			br.readLine();
		} finally {
			serviceGraph.stopFuture().get();
		}


		System.out.println(buf);
		System.out.println(buf2);
		System.out.println(buf3);
	}

}

