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

package io.datakernel.rpc.temp;

import com.google.common.base.Strings;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;

import java.io.IOException;

public class RpcBackPressureTestServer {
	private static final int SERVICE_PORT = 34765;

	public static void main(String[] args) throws IOException, InterruptedException {
		final Eventloop serverEventloop = Eventloop.create().withThreadName("server-eventloop");
		final RpcServer server = RpcServer.create(serverEventloop)
				.withMessageTypes(String.class)
				.withHandler(String.class, String.class, new RpcRequestHandler<String, String>() {
					@Override
					public void run(String request, ResultCallback<String> callback) {
						String result = request + Strings.repeat("Hello", (int) (Math.random() * 20000));
						callback.setResult(result);
						System.out.println("handled. result size: " + result.length());
					}
				})
				.withListenPort(SERVICE_PORT);

		server.listen();

		Thread serverThread = new Thread(serverEventloop);
		serverThread.start();

		serverThread.join();
	}
}