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

package io.datakernel.eventloop.jmx;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.*;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.ExceptionMarker;
import io.datakernel.util.Stopwatch;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public final class NioEventloopStatsSet {

	private static final class DurationRunnable {
		private Runnable runnable;
		private long duration;

		void reset() {
			duration = 0;
			runnable = null;
		}

		void update(Runnable runnable, long duration) {
			this.duration = duration;
			this.runnable = runnable;
		}

		long getDuration() {
			return duration;
		}

		@Override
		public String toString() {
			return (runnable == null) ? "" : runnable.getClass().getName() + ": " + duration;
		}
	}

	private static final class ExceptionMarkerImpl implements ExceptionMarker {
		private final Class<?> clazz;
		private final Marker marker;

		ExceptionMarkerImpl(Class<?> clazz, String name) {
			this.clazz = clazz;
			this.marker = MarkerFactory.getMarker(name);
		}

		@Override
		public Marker getMarker() {
			return marker;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			ExceptionMarkerImpl that = (ExceptionMarkerImpl) o;
			return equal(this.clazz, that.clazz) &&
					equal(this.marker, that.marker);
		}

		private static boolean equal(Object a, Object b) {
			return a == b || (a != null && a.equals(b));
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(new Object[]{clazz, marker});
		}

		@Override
		public String toString() {
			return clazz.getName() + "." + marker.getName();
		}
	}

	private final CurrentTimeProvider timeProvider;
	private double smoothingWindow;
	private double smoothingPrecision;

	private final ValuesCounter selectorSelectTimeStats;
	private final ValuesCounter businessLogicTimeStats;
	private final EventsCounter selectedKeys;
	private final EventsCounter invalidKeys;
	private final EventsCounter acceptKeys;
	private final EventsCounter connectKeys;
	private final EventsCounter readKeys;
	private final EventsCounter writeKeys;
	private final EventsCounter localTasks;
	private final EventsCounter concurrentTasks;
	private final EventsCounter scheduledTasks;

	private final ValuesCounter localTaskDuration;
	private final DurationRunnable lastLongestLocalRunnable;
	private final ValuesCounter concurrentTaskDuration;
	private final DurationRunnable lastLongestConcurrentRunnable;
	private final ValuesCounter scheduledTaskDuration;
	private final DurationRunnable lastLongestScheduledRunnable;

	private final ValuesCounter selectedKeysTimeStats;
	private final ValuesCounter localTasksTimeStats;
	private final ValuesCounter concurrentTasksTimeStats;
	private final ValuesCounter scheduledTasksTimeStats;

	private final Map<ExceptionMarker, LastExceptionCounter> exceptionCounters = new HashMap<>();

	public NioEventloopStatsSet(double smoothingWindow, double smoothingPrecision, CurrentTimeProvider timeProvider) {
		this.timeProvider = timeProvider;

		this.selectorSelectTimeStats = createValuesCounter();
		this.businessLogicTimeStats = createValuesCounter();
		this.selectedKeys = createEventsCounter();
		this.invalidKeys = createEventsCounter();
		this.acceptKeys = createEventsCounter();
		this.connectKeys = createEventsCounter();
		this.readKeys = createEventsCounter();
		this.writeKeys = createEventsCounter();
		this.localTasks = createEventsCounter();
		this.concurrentTasks = createEventsCounter();
		this.scheduledTasks = createEventsCounter();

		this.localTaskDuration = createValuesCounter();
		this.lastLongestLocalRunnable = new DurationRunnable();
		this.concurrentTaskDuration = createValuesCounter();
		this.lastLongestConcurrentRunnable = new DurationRunnable();
		this.scheduledTaskDuration = createValuesCounter();
		this.lastLongestScheduledRunnable = new DurationRunnable();

		this.selectedKeysTimeStats = createValuesCounter();
		this.localTasksTimeStats = createValuesCounter();
		this.concurrentTasksTimeStats = createValuesCounter();
		this.scheduledTasksTimeStats = createValuesCounter();
	}

	private ValuesCounter createValuesCounter() {
		return new ValuesCounter(smoothingWindow, smoothingPrecision, timeProvider);
	}

	private EventsCounter createEventsCounter() {
		return new EventsCounter(smoothingWindow, smoothingPrecision, timeProvider);
	}

	public void updateBusinessLogicTime(long timestamp, long businessLogicTime) {
		businessLogicTimeStats.recordValue((int) businessLogicTime);
	}

	public void updateSelectorSelectTime(long selectorSelectTime) {
		selectorSelectTimeStats.recordValue((int) selectorSelectTime);
	}

	public void updateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys, int connectKeys, int readKeys, int writeKeys) {
		this.selectedKeys.recordEvents(lastSelectedKeys);
		this.invalidKeys.recordEvents(invalidKeys);
		this.acceptKeys.recordEvents(acceptKeys);
		this.connectKeys.recordEvents(connectKeys);
		this.readKeys.recordEvents(readKeys);
		this.writeKeys.recordEvents(writeKeys);
	}

	public void updateSelectedKeysTimeStats(@Nullable Stopwatch sw) {
		if (sw != null)
			selectedKeysTimeStats.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
	}

	private void updateTaskDuration(ValuesCounter counter, DurationRunnable longestCounter, Runnable runnable, @Nullable Stopwatch sw) {
		if (sw != null) {
			int elapsed = (int) sw.elapsed(TimeUnit.MICROSECONDS);
			counter.recordValue(elapsed);
			if (elapsed > longestCounter.getDuration()) {
				longestCounter.update(runnable, elapsed);
			}
		}
	}

	public void updateLocalTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(localTaskDuration, lastLongestLocalRunnable, runnable, sw);
	}

	public void updateLocalTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			localTasksTimeStats.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
		localTasks.recordEvents(newTasks);
	}

	public void updateConcurrentTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(concurrentTaskDuration, lastLongestConcurrentRunnable, runnable, sw);
	}

	public void updateConcurrentTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			concurrentTasksTimeStats.recordValue((int) sw.elapsed(TimeUnit.MICROSECONDS));
		concurrentTasks.recordEvents(newTasks);
	}

	public void updateScheduledTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(scheduledTaskDuration, lastLongestScheduledRunnable, runnable, sw);
	}

	public void updateScheduledTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			scheduledTasksTimeStats.recordValue((int) sw.elapsed(TimeUnit.MILLISECONDS));
		scheduledTasks.recordEvents(newTasks);
	}

	// Exceptions stats

	public static ExceptionMarker exceptionMarker(Class<?> clazz, String name) {
		return new ExceptionMarkerImpl(clazz, name);
	}

	public LastExceptionCounter getExceptionCounter(ExceptionMarker marker) {
		return exceptionCounters.get(marker);
	}

	public LastExceptionCounter ensureExceptionCounter(ExceptionMarker marker) {
		if (!exceptionCounters.containsKey(marker))
			exceptionCounters.put(marker, new LastExceptionCounter(marker.getMarker()));
		return exceptionCounters.get(marker);
	}

	public void updateExceptionCounter(ExceptionMarker marker, Throwable e, Object o, long timestamp) {
		ensureExceptionCounter(marker).update(e, o, timestamp);
	}

	public void resetExceptionCounter(ExceptionMarker marker) {
		LastExceptionCounter counter = exceptionCounters.get(marker);
		if (counter != null)
			counter.reset();
	}

	public void resetStats() {
		resetStats(this.smoothingWindow, this.smoothingPrecision);
	}

	public void resetStats(double smoothingWindow, double smoothingPrecision) {
		this.smoothingWindow = smoothingWindow;
		this.smoothingPrecision = smoothingPrecision;

		selectorSelectTimeStats.reset(smoothingWindow, smoothingPrecision);
		businessLogicTimeStats.reset(smoothingWindow, smoothingPrecision);

		selectedKeys.reset(smoothingWindow, smoothingPrecision);
		invalidKeys.reset(smoothingWindow, smoothingPrecision);
		acceptKeys.reset(smoothingWindow, smoothingPrecision);
		connectKeys.reset(smoothingWindow, smoothingPrecision);
		readKeys.reset(smoothingWindow, smoothingPrecision);
		writeKeys.reset(smoothingWindow, smoothingPrecision);

		localTasks.reset(smoothingWindow, smoothingPrecision);
		concurrentTasks.reset(smoothingWindow, smoothingPrecision);
		scheduledTasks.reset(smoothingWindow, smoothingPrecision);

		localTaskDuration.reset(smoothingWindow, smoothingPrecision);
		concurrentTaskDuration.reset(smoothingWindow, smoothingPrecision);
		scheduledTaskDuration.reset(smoothingWindow, smoothingPrecision);

		selectedKeysTimeStats.reset(smoothingWindow, smoothingPrecision);
		localTasksTimeStats.reset(smoothingWindow, smoothingPrecision);
		concurrentTasksTimeStats.reset(smoothingWindow, smoothingPrecision);
		scheduledTasksTimeStats.reset(smoothingWindow, smoothingPrecision);

		for (LastExceptionCounter counter : exceptionCounters.values()) {
			counter.reset();
		}

		lastLongestLocalRunnable.reset();
		lastLongestConcurrentRunnable.reset();
		lastLongestScheduledRunnable.reset();
	}

	public ValuesCounter getSelectorSelectTimeStats() {
		return selectorSelectTimeStats;
	}

	public ValuesCounter getBusinessLogicTimeStats() {
		return businessLogicTimeStats;
	}

	public EventsCounter getSelectedKeys() {
		return selectedKeys;
	}

	public EventsCounter getInvalidKeys() {
		return invalidKeys;
	}

	public EventsCounter getAcceptKeys() {
		return acceptKeys;
	}

	public EventsCounter getConnectKeys() {
		return connectKeys;
	}

	public EventsCounter getReadKeys() {
		return readKeys;
	}

	public EventsCounter getWriteKeys() {
		return writeKeys;
	}

	public EventsCounter getLocalTasks() {
		return localTasks;
	}

	public EventsCounter getConcurrentTasks() {
		return concurrentTasks;
	}

	public EventsCounter getScheduledTasks() {
		return scheduledTasks;
	}

	public ValuesCounter getLocalTaskDuration() {
		return localTaskDuration;
	}

	public DurationRunnable getLastLongestLocalRunnable() {
		return lastLongestLocalRunnable;
	}

	public ValuesCounter getConcurrentTaskDuration() {
		return concurrentTaskDuration;
	}

	public DurationRunnable getLastLongestConcurrentRunnable() {
		return lastLongestConcurrentRunnable;
	}

	public ValuesCounter getScheduledTaskDuration() {
		return scheduledTaskDuration;
	}

	public DurationRunnable getLastLongestScheduledRunnable() {
		return lastLongestScheduledRunnable;
	}

	public ValuesCounter getSelectedKeysTimeStats() {
		return selectedKeysTimeStats;
	}

	public ValuesCounter getLocalTasksTimeStats() {
		return localTasksTimeStats;
	}

	public ValuesCounter getConcurrentTasksTimeStats() {
		return concurrentTasksTimeStats;
	}

	public ValuesCounter getScheduledTasksTimeStats() {
		return scheduledTasksTimeStats;
	}

	public Map<ExceptionMarker, LastExceptionCounter> getExceptionCounters() {
		return exceptionCounters;
	}

	public static Accumulator accumulator() {
		return new Accumulator();
	}

	public static final class Accumulator {
		private final ValuesCounter.Accumulator selectorSelectTimeStats;
		private final ValuesCounter.Accumulator businessLogicTimeStats;
		private final EventsCounter.Accumulator selectedKeys;
		private final EventsCounter.Accumulator invalidKeys;
		private final EventsCounter.Accumulator acceptKeys;
		private final EventsCounter.Accumulator connectKeys;
		private final EventsCounter.Accumulator readKeys;
		private final EventsCounter.Accumulator writeKeys;
		private final EventsCounter.Accumulator localTasks;
		private final EventsCounter.Accumulator concurrentTasks;
		private final EventsCounter.Accumulator scheduledTasks;

		private final ValuesCounter.Accumulator localTaskDuration;
		private final ValuesCounter.Accumulator concurrentTaskDuration;
		private final ValuesCounter.Accumulator scheduledTaskDuration;

		private final ValuesCounter.Accumulator selectedKeysTimeStats;
		private final ValuesCounter.Accumulator localTasksTimeStats;
		private final ValuesCounter.Accumulator concurrentTasksTimeStats;
		private final ValuesCounter.Accumulator scheduledTasksTimeStats;

		private final Map<ExceptionMarker, LastExceptionCounter.Accumulator> exceptionCounters = new HashMap<>();

		private Accumulator() {
			this.selectorSelectTimeStats = ValuesCounter.accumulator();
			this.businessLogicTimeStats = ValuesCounter.accumulator();
			this.selectedKeys = EventsCounter.accumulator();
			this.invalidKeys = EventsCounter.accumulator();
			this.acceptKeys = EventsCounter.accumulator();
			this.connectKeys = EventsCounter.accumulator();
			this.readKeys = EventsCounter.accumulator();
			this.writeKeys = EventsCounter.accumulator();
			this.localTasks = EventsCounter.accumulator();
			this.concurrentTasks = EventsCounter.accumulator();
			this.scheduledTasks = EventsCounter.accumulator();

			this.localTaskDuration = ValuesCounter.accumulator();
			this.concurrentTaskDuration = ValuesCounter.accumulator();
			this.scheduledTaskDuration = ValuesCounter.accumulator();

			this.selectedKeysTimeStats = ValuesCounter.accumulator();
			this.localTasksTimeStats = ValuesCounter.accumulator();
			this.concurrentTasksTimeStats = ValuesCounter.accumulator();
			this.scheduledTasksTimeStats = ValuesCounter.accumulator();
		}

		public void add(NioEventloopStatsSet statsSet) {
			selectorSelectTimeStats.add(statsSet.selectorSelectTimeStats);
			businessLogicTimeStats.add(statsSet.businessLogicTimeStats);
			selectedKeys.add(statsSet.selectedKeys);
			invalidKeys.add(statsSet.invalidKeys);
			acceptKeys.add(statsSet.acceptKeys);
			connectKeys.add(statsSet.connectKeys);
			readKeys.add(statsSet.readKeys);
			writeKeys.add(statsSet.writeKeys);
			localTasks.add(statsSet.localTasks);
			concurrentTasks.add(statsSet.concurrentTasks);
			scheduledTasks.add(statsSet.scheduledTasks);

			localTaskDuration.add(statsSet.localTaskDuration);
			concurrentTaskDuration.add(statsSet.concurrentTaskDuration);
			scheduledTaskDuration.add(statsSet.scheduledTaskDuration);

			selectedKeysTimeStats.add(statsSet.selectedKeysTimeStats);
			localTasksTimeStats.add(statsSet.localTasksTimeStats);
			concurrentTasksTimeStats.add(statsSet.concurrentTasksTimeStats);
			scheduledTasksTimeStats.add(statsSet.scheduledTasksTimeStats);

			for (ExceptionMarker marker : statsSet.exceptionCounters.keySet()) {
				if (!this.exceptionCounters.containsKey(marker)) {
					this.exceptionCounters.put(marker, LastExceptionCounter.accumulator());
				}
				LastExceptionCounter.Accumulator exceptionAccumulator = this.exceptionCounters.get(marker);
				exceptionAccumulator.add(statsSet.exceptionCounters.get(marker));
			}
		}

		public ValuesCounter.Accumulator getSelectorSelectTimeStats() {
			return selectorSelectTimeStats;
		}

		public ValuesCounter.Accumulator getBusinessLogicTimeStats() {
			return businessLogicTimeStats;
		}

		public EventsCounter.Accumulator getSelectedKeys() {
			return selectedKeys;
		}

		public EventsCounter.Accumulator getInvalidKeys() {
			return invalidKeys;
		}

		public EventsCounter.Accumulator getAcceptKeys() {
			return acceptKeys;
		}

		public EventsCounter.Accumulator getConnectKeys() {
			return connectKeys;
		}

		public EventsCounter.Accumulator getReadKeys() {
			return readKeys;
		}

		public EventsCounter.Accumulator getWriteKeys() {
			return writeKeys;
		}

		public EventsCounter.Accumulator getLocalTasks() {
			return localTasks;
		}

		public EventsCounter.Accumulator getConcurrentTasks() {
			return concurrentTasks;
		}

		public EventsCounter.Accumulator getScheduledTasks() {
			return scheduledTasks;
		}

		public ValuesCounter.Accumulator getLocalTaskDuration() {
			return localTaskDuration;
		}

		public ValuesCounter.Accumulator getConcurrentTaskDuration() {
			return concurrentTaskDuration;
		}

		public ValuesCounter.Accumulator getScheduledTaskDuration() {
			return scheduledTaskDuration;
		}

		public ValuesCounter.Accumulator getSelectedKeysTimeStats() {
			return selectedKeysTimeStats;
		}

		public ValuesCounter.Accumulator getLocalTasksTimeStats() {
			return localTasksTimeStats;
		}

		public ValuesCounter.Accumulator getConcurrentTasksTimeStats() {
			return concurrentTasksTimeStats;
		}

		public ValuesCounter.Accumulator getScheduledTasksTimeStats() {
			return scheduledTasksTimeStats;
		}

		public Map<ExceptionMarker, LastExceptionCounter.Accumulator> getExceptionCounters() {
			return exceptionCounters;
		}
	}
}
