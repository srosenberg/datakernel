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

import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.Math.*;

/**
 * Counts dynamic average of values using exponential smoothing algorithm
 *
 * Class is supposed to work in single thread
 */
public final class DynamicStatsCounter {

	private static final double ONE_SECOND_IN_MILLIS = 1000.0;
	private static final double DEFAULT_INITIAL_DYNAMIC_AVG = 0.0;
	private static final double DEFAULT_INITIAL_DYNAMIC_VARIANCE = 0.0;

	private final CurrentTimeProvider timeProvider;
	private double windowE;
	private double precision;
	private long lastTimestampMillis;
	private double lastValuesSum;
	private int lastValuesAmount;
	private double lastValue;
	private double maxValue;
	private double minValue;
	private double dynamicAvg;
	private double dynamicVariance;

	/**
	 * Creates {@link DynamicStatsCounter} with specified parameters
	 *
	 * @param window time in seconds at which weight of appropriate value is 0.5
	 * @param precision time in seconds to update dynamic average
	 * @param timeProvider provider of current time
	 */
	public DynamicStatsCounter(double window, double precision, CurrentTimeProvider timeProvider) {
		this.timeProvider = timeProvider;
		resetValues(transformWindow(window), secondsToMillis(precision));
	}

	/**
	 * Resets stats and sets new parameters
	 *
	 * @param window time in seconds at which weight of appropriate value is 0.5
	 * @param precision time in seconds to update dynamic average
	 */
	public void reset(double window, double precision) {
		resetValues(transformWindow(window), secondsToMillis(precision));
	}

	/**
	 * Resets stats and sets initial value to zero
	 *
	 */
	public void reset() {
		resetValues(windowE, precision);
	}

	private void resetValues(double windowE, double precisionInMillis) {
		this.windowE = windowE;
		this.precision = precisionInMillis;
		this.dynamicAvg = DEFAULT_INITIAL_DYNAMIC_AVG;
		this.dynamicVariance = DEFAULT_INITIAL_DYNAMIC_VARIANCE;
		this.lastTimestampMillis = timeProvider.currentTimeMillis();
		this.lastValuesSum = 0.0;
		this.lastValuesAmount = 0;
		this.maxValue = Double.MIN_VALUE;
		this.minValue = Double.MAX_VALUE;
	}

	/**
	 * Adds value
	 */
	public void add(double value) {
		lastValue = value;
		if (value > maxValue) {
			maxValue = value;
		}
		if (value < minValue) {
			minValue = value;
		}

		int timeElapsedMillis = (int)(timeProvider.currentTimeMillis() - lastTimestampMillis);
		lastValuesSum += value;
		++lastValuesAmount;
		if (timeElapsedMillis >= precision) {
			double lastValuesAvg = lastValuesSum / lastValuesAmount;
			double weight = 1 - exp(-timeElapsedMillis / windowE);

			dynamicAvg += (lastValuesAvg - dynamicAvg) * weight;

			double currentDeviationSquared = pow((dynamicAvg - lastValuesAvg), 2.0);
			dynamicVariance += (currentDeviationSquared - dynamicVariance) * weight;

			lastTimestampMillis += timeElapsedMillis;
			lastValuesSum = 0;
			lastValuesAmount = 0;
		}
	}

	/**
	 * Returns dynamic average of added values
	 *
	 * @return dynamic average of added values
	 */
	public double getDynamicAvg() {
		return dynamicAvg;
	}

	/**
	 * Returns dynamic standard deviation
	 *
	 * @return dynamic standard deviation
	 */
	public double getDynamicStdDeviation() {
		return sqrt(dynamicVariance);
	}

	/**
	 * Returns minimum of all added values
	 *
	 * @return minimum of all added values
	 */
	public double getMinValue() {
		return minValue;
	}

	/**
	 * Returns maximum of all added values
	 *
	 * @return maximum of all added values
	 */
	public double getMaxValue() {
		return maxValue;
	}

	/**
	 * Returns last added value
	 *
	 * @return last added value
	 */
	public double getLastValue() {
		return lastValue;
	}

	@Override
	public String toString() {
		return String.format("%.2f±%.3f min: %.2f max: .2f",
				getDynamicAvg(), getDynamicStdDeviation(), getMinValue(), getMaxValue());
	}

	private static double secondsToMillis(double precisionInSeconds) {
		return precisionInSeconds * ONE_SECOND_IN_MILLIS;
	}

	private static double transformWindow(double windowBase2InSeconds) {
		return windowBase2InSeconds * ONE_SECOND_IN_MILLIS / log(2);
	}
}
