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

package io.datakernel.util;

import java.util.Random;

public final class XorShiftRandom {
	private long state0;
	private long state1;

	public XorShiftRandom(long seed0, long seed1) {
		this.state0 = seed0;
		this.state1 = seed1;
	}

	public XorShiftRandom() {
		this(nonZeroRandomLong(), nonZeroRandomLong());
	}

	private static long nonZeroRandomLong() {
		Random random = new Random();
		long randomLong = random.nextLong();
		return randomLong != 0L ? randomLong : 2347230858016798896L;
	}

	public long nextLong() {
		long x = state0;
		long y = state1;
		state0 = y;
		x ^= x << 23;
		state1 = (x ^ y ^ (x >>> 17) ^ (y >>> 26));
		return state1 + y;
	}
}
