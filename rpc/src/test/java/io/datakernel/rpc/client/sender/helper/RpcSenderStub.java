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

package io.datakernel.rpc.client.sender.helper;

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.protocol.RpcMessage;

public final class RpcSenderStub implements RpcClientConnection {
	private int sends;

	public int getSendsNumber() {
		return sends;
	}

	@Override
	public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
		sends++;
	}

	@Override
	public void close() {
		// do nothing
	}

	@Override
	public SocketConnection getSocketConnection() {
		// return nothing
		return null;
	}

	@Override
	public void onReceiveMessage(RpcMessage message) {
		// do nothing
	}

	@Override
	public void ready() {
		// do nothing
	}

	@Override
	public void onClosed() {
		// do nothing
	}

	@Override
	public NioEventloop getEventloop() {
		// return nothing
		return null;
	}
}
