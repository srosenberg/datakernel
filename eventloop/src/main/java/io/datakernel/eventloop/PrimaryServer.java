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

package io.datakernel.eventloop;

import java.nio.channels.SocketChannel;
import java.util.Collection;

/**
 * PrimaryServer is used for load-balancing.
 * It just forwards "accept" events to worker servers using round-robin algorithm
 */
public final class PrimaryServer extends AbstractServer<PrimaryServer> {
	private EventloopServer[] workerServers;

	private int currentAcceptor = 0;

	private PrimaryServer(Eventloop primaryEventloop) {
		super(primaryEventloop);
	}

	public static PrimaryServer create(Eventloop primaryEventloop) {
		return new PrimaryServer(primaryEventloop);
	}

	/**
	 * Set worker servers
	 *
	 * @param workerServers list of worker servers
	 * @return this PrimaryNioServer
	 */
	@SuppressWarnings("unchecked")
	public PrimaryServer workerServers(Collection<? extends EventloopServer> workerServers) {
		this.workerServers = workerServers.toArray(new EventloopServer[workerServers.size()]);
		return this;
	}

	/**
	 * Adds the NioServers from argument which will handle accepting to this server
	 *
	 * @param workerServers list of workers NioServers
	 * @return this PrimaryNioServer
	 */
	@SuppressWarnings("unchecked")
	public PrimaryServer workerServers(EventloopServer... workerServers) {
		this.workerServers = workerServers;
		return this;
	}

	/**
	 * Instance of this class can not create a connection, it only forwards it. That is why this method
	 * throws Exception
	 *
	 * @param socketChannel the socketChannel for creating connection.
	 */
	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Forwards "accept" events to worker servers using round-robin algorithm
	 *
	 * @param socketChannel the incoming socketChannel.
	 */
	@Override
	public void onAccept(final SocketChannel socketChannel) {
		assert eventloop.inEventloopThread();

		// jmx
		getTotalAccepts().recordEvent();

		final EventloopServer server = workerServers[currentAcceptor];
		currentAcceptor = (currentAcceptor + 1) % workerServers.length;
		Eventloop eventloop = server.getEventloop();
		if (eventloop == this.eventloop) {
			server.onAccept(socketChannel);
		} else {
			eventloop.execute(new Runnable() {
				@Override
				public void run() {
					server.onAccept(socketChannel);
				}
			});
		}

		if (acceptOnce) {
			close();
		}

	}
}
