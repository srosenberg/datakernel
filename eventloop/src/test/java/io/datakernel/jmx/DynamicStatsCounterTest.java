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

import java.util.Random;

import static java.lang.Math.*;
import static org.junit.Assert.assertEquals;

public class DynamicStatsCounterTest {

	private static final ManualTimeProvider MANUAL_TIME_PROVIDER = new ManualTimeProvider(0);
	private static final int ONE_SECOND_IN_MILLIS = 1000;
	private static final Random RANDOM = new Random();


	@Test
	public void dynamicAverageAtLimitShouldBeSameAsInputInCaseOfConstantData() {
		double window = 10.0;
		double precision = 0.1;
		DynamicStatsCounter dynamicStatsCounter = new DynamicStatsCounter(window, precision, MANUAL_TIME_PROVIDER);
		double inputValue = 5.0;
		int iterations = 1000;

		for (int i = 0; i < iterations; i++) {
			MANUAL_TIME_PROVIDER.upgradeTime(ONE_SECOND_IN_MILLIS);
			dynamicStatsCounter.add(inputValue);
		}

		double acceptableError = 10E-5;
		assertEquals(inputValue, dynamicStatsCounter.getDynamicAvg(), acceptableError);
	}

	@Test
	public void itShouldReturnProperStandardDeviationAtLimit() {
		double window = 100.0;
		double precision = 0.1;
		DynamicStatsCounter counter = new DynamicStatsCounter(window, precision, MANUAL_TIME_PROVIDER);
		int iterations = 10000;
		double minValue = 0.0;
		double maxValue = 10.0;

		for (int i = 0; i < iterations; i++) {
			MANUAL_TIME_PROVIDER.upgradeTime(100);
			double currentValue = uniformRandom(minValue, maxValue);
			counter.add(currentValue);
		};

		double expectedStandardDeviation = (maxValue - minValue) / sqrt(12);  // standard deviation of uniform distribution
		double acceptableError = 0.1;
		assertEquals(expectedStandardDeviation, counter.getDynamicStdDeviation(), acceptableError);
	}

	@Test
	public void itShouldResetStatsAfterResetMethodCall() {
		double window = 10.0;
		double precision = 0.1;
		DynamicStatsCounter dynamicStatsCounter = new DynamicStatsCounter(window, precision, MANUAL_TIME_PROVIDER);
		double inputValue = 5.0;
		int iterations = 1000;

		for (int i = 0; i < iterations; i++) {
			MANUAL_TIME_PROVIDER.upgradeTime(ONE_SECOND_IN_MILLIS);
			dynamicStatsCounter.add(inputValue);
		}

		double avgBeforeReset = dynamicStatsCounter.getDynamicAvg();
		dynamicStatsCounter.reset();
		double avgAfterReset = dynamicStatsCounter.getDynamicAvg();

		double acceptableError = 10E-5;
		assertEquals(inputValue, avgBeforeReset, acceptableError);
		assertEquals(0.0, avgAfterReset, acceptableError);
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

	public static double uniformRandom(double min, double max) {
		return min + (RANDOM.nextDouble() * (max - min));
	}
}
