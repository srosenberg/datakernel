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

package io.datakernel.jmx;

import io.datakernel.time.CurrentTimeProvider;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import static java.lang.Math.abs;
import static java.lang.Math.pow;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DynamicRateCounterTest {

	private static final ManualTimeProvider MANUAL_TIME_PROVIDER = new ManualTimeProvider(0);

	@Test
	public void counterShouldUpdatesRateDependingOnPrecision() throws InterruptedException {
		double precision_1_inSeconds = 1.0;
		double precision_2_inSeconds = 2.0;
		int oneSecondInMillis = 1000;
		DynamicRateCounter dynamicRateCounter_1 = new DynamicRateCounter(0.1, precision_1_inSeconds, MANUAL_TIME_PROVIDER);
		DynamicRateCounter dynamicRateCounter_2 = new DynamicRateCounter(0.1, precision_2_inSeconds, MANUAL_TIME_PROVIDER);

		double counter_1_initRate = dynamicRateCounter_1.getRate();
		double counter_2_initRate = dynamicRateCounter_2.getRate();
		MANUAL_TIME_PROVIDER.upgradeTime(oneSecondInMillis);
		dynamicRateCounter_1.recordEvent();
		dynamicRateCounter_2.recordEvent();
		double counter_1_rateAfterUpgrade_1 = dynamicRateCounter_1.getRate();
		double counter_2_rateAfterUpgrade_1 = dynamicRateCounter_2.getRate();
		MANUAL_TIME_PROVIDER.upgradeTime(oneSecondInMillis);
		dynamicRateCounter_1.recordEvent();
		dynamicRateCounter_2.recordEvent();
		double counter_1_rateAfterUpgrade_2 = dynamicRateCounter_1.getRate();
		double counter_2_rateAfterUpgrade_2 = dynamicRateCounter_2.getRate();

		double acceptableError = 1E-5;
		assertNotEquals(counter_1_initRate, counter_1_rateAfterUpgrade_1, acceptableError);
		assertEquals(counter_2_initRate, counter_2_rateAfterUpgrade_1, acceptableError); // not enough time passed to be updated
		assertNotEquals(counter_1_rateAfterUpgrade_1, counter_1_rateAfterUpgrade_2, acceptableError);
		assertNotEquals(counter_2_rateAfterUpgrade_1, counter_2_rateAfterUpgrade_2, acceptableError);
	}

	@Test
	public void ifRateIsConstantCounterShouldApproximateThatRateAfterEnoughTimePassed() throws InterruptedException {
		DynamicRateCounter dynamicRateCounter = new DynamicRateCounter(0.1, 0.01, MANUAL_TIME_PROVIDER);
		int events = 100;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int)(period * 1000);

		for (int i = 0; i < events; i++) {
			dynamicRateCounter.recordEvent();
			MANUAL_TIME_PROVIDER.upgradeTime(periodInMillis);
//			System.out.println(i + ": " + dynamicRateCounter.getRate());
		}

		double acceptableError = 1E-5;
		assertEquals(rate, dynamicRateCounter.getRate(), acceptableError);
	}

	@Test
	public void counterShouldResetRateAfterResetMethodCall() {
		DynamicRateCounter dynamicRateCounter = new DynamicRateCounter(0.1, 0.01, MANUAL_TIME_PROVIDER);
		int events = 100;
		double rate = 20.0;
		double period = 1.0 / rate;
		int periodInMillis = (int)(period * 1000);

		for (int i = 0; i < events; i++) {
			dynamicRateCounter.recordEvent();
			MANUAL_TIME_PROVIDER.upgradeTime(periodInMillis);
//			System.out.println(i + ": " + dynamicRateCounter.getRate());
		}
		double rateBeforeReset = dynamicRateCounter.getRate();
		double initRateOfReset = 0.0;
		dynamicRateCounter.reset();
		double rateAfterReset = dynamicRateCounter.getRate();

		double acceptableError = 1E-5;
		assertEquals(rate, rateBeforeReset, acceptableError);
		assertEquals(initRateOfReset, rateAfterReset, acceptableError);
	}

	public static final class ManualTimeProvider implements CurrentTimeProvider {

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

}
