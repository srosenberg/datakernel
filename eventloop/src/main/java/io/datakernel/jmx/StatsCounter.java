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

public final class StatsCounter {
	private double min = Double.MAX_VALUE;
	private double max = Double.MIN_VALUE;
	private double last;
	private int count;
	private double sum;
	private double sumSqr;

	public void reset() {
		min = Double.MAX_VALUE;
		max = Double.MIN_VALUE;
		count = 0;
		last = 0;
		sumSqr = 0;
		sum = 0;
	}

	public void add(double value) {
		setValue(value);
		sum += value;
		sumSqr += sqr(value);
		++count;
	}

	public void incCount() {
		++count;
	}

	public void update(int value, int prevValue) {
		if (value == prevValue)
			return;
		setValue(value);
		sum = sum - prevValue + value;
		sumSqr = sumSqr - sqr(prevValue) + sqr(value);
	}

	private void setValue(double value) {
		last = value;
		if (value < min)
			min = value;
		if (value > max)
			max = value;
	}

	public double getLast() {
		return last;
	}

	public double getMin() {
		return min;
	}

	public double getMax() {
		return max;
	}

	public double getAvg() {
		if (count == 0)
			return 0;
		return sum / count;
	}

	public double getTotal() {
		return sum;
	}

	public double getStd() {
		if (count <= 1)
			return 0;
		double avg = getAvg();
		return Math.sqrt( sumSqr / count - avg * avg);
	}

	private static double sqr(double v) {
		return v * v;
	}

	@Override
	public String toString() {
		if (count == 0)
			return "";
		return String.format("%.2f±%.3f min: %.3f max: .3f", getAvg(), getStd(), getMin(), getMax());
	}
}
