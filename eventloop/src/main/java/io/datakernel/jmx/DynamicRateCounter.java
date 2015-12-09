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

import static java.lang.Math.exp;
import static java.lang.Math.log;

/**
 * Computes dynamic rate using exponential smoothing algorithm
 * <p/>
 * Class is supposed to work in single thread
 */
public final class DynamicRateCounter {
	private static final double ONE_SECOND_IN_MILLIS = 1000.0;
	private static final double DEFAULT_INITIAL_PERIOD = 0.0;

	private final CurrentTimeProvider timeProvider;
	private double windowE;
	private double precision;
	private long lastTimestampMillis;
	private int eventPerLastTimePeriod;
	private double dynamicPeriodMillis;
	private boolean calculationsStarted;

	/**
	 * Creates {@link DynamicRateCounter} with specified parameters and rate = 0
	 *
	 * @param window       time in seconds at which weight of appropriate rate is 0.5
	 * @param precision    time in seconds to update dynamic rate
	 * @param timeProvider provider of current time
	 */
	public DynamicRateCounter(double window, double precision, CurrentTimeProvider timeProvider) {
		this.timeProvider = timeProvider;
		resetValues(transformWindow(window), secondsToMillis(precision));
	}

	/**
	 * Resets rate to zero and sets new parameters
	 *
	 * @param window    time in seconds at which weight of appropriate rate is 0.5
	 * @param precision time in seconds to update dynamic rate
	 */
	public void reset(double window, double precision) {
		resetValues(transformWindow(window), secondsToMillis(precision));
	}

	/**
	 * Resets rate to zero
	 */
	public void reset() {
		resetValues(windowE, precision);
	}

	private void resetValues(double windowE, double precisionInMillis) {
		this.windowE = windowE;
		this.precision = precisionInMillis;
		this.lastTimestampMillis = timeProvider.currentTimeMillis();
		this.eventPerLastTimePeriod = 0;
		this.dynamicPeriodMillis = DEFAULT_INITIAL_PERIOD;
		this.calculationsStarted = false;
	}

	/**
	 * Records event and updates rate
	 */
	public void recordEvent() {
		int timeElapsedMillis = (int) (timeProvider.currentTimeMillis() - lastTimestampMillis);
		++eventPerLastTimePeriod;
		if (timeElapsedMillis >= precision) {
			if (eventPerLastTimePeriod > 0) {
				double lastPeriodAvg = (double) (timeElapsedMillis) / eventPerLastTimePeriod;
				double weight = 1 - exp(-timeElapsedMillis / windowE);
				dynamicPeriodMillis += (lastPeriodAvg - dynamicPeriodMillis) * weight;
				lastTimestampMillis += timeElapsedMillis;
				eventPerLastTimePeriod = 0;
				calculationsStarted = true;
			} else {
				// we don't update lastTimestamp
			}
		}
	}

	/**
	 * Returns dynamic value of rate in events per second.
	 * <p/>
	 * Value may be delayed. Last update was performed during {@code recordEvent()} method invocation
	 *
	 * @return dynamic value of rate in events per second
	 */
	public double getRate() {
		if (calculationsStarted) {
			return ONE_SECOND_IN_MILLIS / dynamicPeriodMillis;
		} else {
			return 0.0; // before any calculations were performed default period is 0, thus rate would be infinity, which is bad
		}
	}

	@Override
	public String toString() {
		return "Dynamic Rate: " + String.valueOf(getRate());
	}

	private static double secondsToMillis(double seconds) {
		return seconds * ONE_SECOND_IN_MILLIS;
	}

	private static double transformWindow(double windowBase2InSeconds) {
		return windowBase2InSeconds * ONE_SECOND_IN_MILLIS / log(2);
	}
}
