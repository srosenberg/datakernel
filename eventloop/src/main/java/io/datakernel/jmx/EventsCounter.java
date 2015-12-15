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
 * Computes total amount of events and dynamic rate using exponential smoothing algorithm
 * <p/>
 * Class is supposed to work in single thread
 */
public final class EventsCounter {
	private static final double ONE_SECOND_IN_MILLIS = 1000.0;
	private static final double DEFAULT_INITIAL_PERIOD_IN_MILLIS = 1E6;

	private final CurrentTimeProvider timeProvider;
	private double windowE;
	private double precision;
	private long lastTimestampMillis;
	private int eventPerLastTimePeriod;
	private double smoothedPeriod;
	private long totalEvents;
	private double smoothedMinRate;
	private double smoothedMaxRate;
	private boolean calculationsStarted;

	/**
	 * Creates {@link EventsCounter} with specified parameters and rate = 0
	 *
	 * @param window       time in seconds at which weight of appropriate rate is 0.5
	 * @param precision    time in seconds to update dynamic rate
	 * @param timeProvider provider of current time
	 */
	public EventsCounter(double window, double precision, CurrentTimeProvider timeProvider) {
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
		this.smoothedPeriod = DEFAULT_INITIAL_PERIOD_IN_MILLIS;
		this.totalEvents = 0;
		this.smoothedMinRate = Double.MAX_VALUE;
		this.smoothedMaxRate = Double.MIN_VALUE;
		this.calculationsStarted = false;
	}

	/**
	 * Records event and updates rate
	 */
	public void recordEvent() {
		int timeElapsedMillis = (int) (timeProvider.currentTimeMillis() - lastTimestampMillis);
		++eventPerLastTimePeriod;
		++totalEvents;
		if (timeElapsedMillis >= precision) {
			if (eventPerLastTimePeriod > 0) {
				double lastPeriodAvg = (double) (timeElapsedMillis) / eventPerLastTimePeriod;
				double weight = 1 - exp(-timeElapsedMillis / windowE);
				smoothedPeriod += (lastPeriodAvg - smoothedPeriod) * weight;

				double lastStepRate = (1.0 / lastPeriodAvg) * ONE_SECOND_IN_MILLIS;
				updateSmoothedMin(weight, lastStepRate);
				updateSmoothedMax(weight, lastStepRate);

				lastTimestampMillis += timeElapsedMillis;
				eventPerLastTimePeriod = 0;
				calculationsStarted = true;
			} else {
				// we don't update lastTimestamp
			}
		}
	}

	private void updateSmoothedMax(double weight, double lastStepRate) {
		if (lastStepRate > smoothedMaxRate) {
			smoothedMaxRate = lastStepRate;
		} else {
			smoothedMaxRate += (smoothedMinRate - smoothedMaxRate) * weight;
		}
	}

	private void updateSmoothedMin(double weight, double lastStepRate) {
		if (lastStepRate < smoothedMinRate) {
			smoothedMinRate = lastStepRate;
		} else {
			smoothedMinRate += (smoothedMaxRate - smoothedMinRate) * weight;
		}
	}

	/**
	 * Returns smoothed value of rate in events per second.
	 * <p/>
	 * Value may be delayed. Last update was performed during {@code recordEvent()} method invocation
	 *
	 * @return dynamic value of rate in events per second
	 */
	public double getSmoothedRate() {
		return calculationsStarted ? ONE_SECOND_IN_MILLIS / smoothedPeriod : 0.0;
	}

	/**
	 * Returns smoothed minimum rate in events per second.
	 * <p/>
	 * Value may be delayed. Last update was performed during {@code recordEvent()} method invocation
	 *
	 * @return smoothed minimum rate in events per second.
	 */
	public double getSmoothedMinRate() {
		return calculationsStarted ? smoothedMinRate : 0.0;
	}

	/**
	 * Returns smoothed maximum rate in events per second.
	 * <p/>
	 * Value may be delayed. Last update was performed during {@code recordEvent()} method invocation
	 *
	 * @return smoothed maximum rate in events per second.
	 */
	public double getSmoothedMaxRate() {
		return calculationsStarted ? smoothedMaxRate : 0.0;
	}

	/**
	 * Returns total amount of recorded events
	 *
	 * @return total amount of recorded events
	 */
	public long getEventsCount() {
		return totalEvents;
	}

	@Override
	public String toString() {
		return String.format("total: %d   smoothedRate: %.4f   smoothedMinRate: %.4f   smoothedMaxRate: %.4f",
				getEventsCount(), getSmoothedRate(), getSmoothedMinRate(), getSmoothedMaxRate());
	}

	private static double secondsToMillis(double seconds) {
		return seconds * ONE_SECOND_IN_MILLIS;
	}

	private static double transformWindow(double windowBase2InSeconds) {
		return windowBase2InSeconds * ONE_SECOND_IN_MILLIS / log(2);
	}
}
