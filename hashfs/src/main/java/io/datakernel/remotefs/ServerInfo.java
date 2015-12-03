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

package io.datakernel.remotefs;

import java.net.InetSocketAddress;

public final class ServerInfo {
	private final InetSocketAddress address;
	private final double weight;
	private final int serverId;
	private long lastHeartBeatReceived;

	public ServerInfo(int serverId, InetSocketAddress address, double weight) {
		this.serverId = serverId;
		this.address = address;
		this.weight = weight;
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	public double getWeight() {
		return weight;
	}

	public int getServerId() {
		return serverId;
	}

	public void updateState(long heartBeat) {
		lastHeartBeatReceived = heartBeat;
	}

	public boolean isAlive(long expectedDieTime) {
		return lastHeartBeatReceived > expectedDieTime;
	}

	@Override
	public int hashCode() {
		return serverId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ServerInfo that = (ServerInfo) o;
		return serverId == that.serverId;
	}

	@Override
	public String toString() {
		return "FileServer id: " + serverId;
	}
}