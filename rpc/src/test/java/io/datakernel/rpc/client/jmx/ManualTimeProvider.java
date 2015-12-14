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

package io.datakernel.rpc.client.jmx;

import io.datakernel.time.CurrentTimeProvider;

public final class ManualTimeProvider implements CurrentTimeProvider {

	private long currentTime;

	public ManualTimeProvider(long currentTime) {
		this.currentTime = currentTime;
	}

	public void upgradeTime(int millis) {
		currentTime += millis;
	}

	@Override
	public long currentTimeMillis() {
		return currentTime;
	}
}
