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

package io.datakernel.hashfs2.protocol;

import io.datakernel.hashfs2.ServerInfo;

import java.util.Collections;
import java.util.Set;

class HashFsResponseListServers extends HashFsResponse {
	public final Set<ServerInfo> servers;

	public HashFsResponseListServers(Set<ServerInfo> servers) {
		this.servers = Collections.unmodifiableSet(servers);
	}

	@Override
	public String toString() {
		return "Listed{" + servers.size() + "}";
	}
}