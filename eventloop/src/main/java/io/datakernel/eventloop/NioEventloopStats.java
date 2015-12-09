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

package io.datakernel.eventloop;

import io.datakernel.annotation.Nullable;
import io.datakernel.jmx.*;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.util.ExceptionMarker;
import io.datakernel.util.Stopwatch;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class NioEventloopStats implements NioEventloopStatsMBean {
	private static final long DEFAULT_LONGLOOP_TIME = 500; // 500 ms
	private static final double DEFAULT_STATS_WINDOW = 10.0; // 10 seconds
	private static final double DEFAULT_STATS_PRECISION = 0.1; // 0.1 seconds

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

	private final DynamicStatsCounter selectorSelectTimeStats;
	private final DynamicStatsCounter businessLogicTimeStats;
	private final DynamicStatsCounter selectedKeysStats;
	private final DynamicStatsCounter invalidKeysStats;
	private final DynamicStatsCounter acceptKeysStats;
	private final DynamicStatsCounter connectKeysStats;
	private final DynamicStatsCounter readKeysStats;
	private final DynamicStatsCounter writeKeysStats;
	private final DynamicStatsCounter localTasksStats;
	private final DynamicStatsCounter concurrentTasksStats;
	private final DynamicStatsCounter scheduledTasksStats;

	private final DynamicStatsCounter localTaskDuration;
	private final DurationRunnable lastLongestLocalRunnable;
	private final DynamicStatsCounter concurrentTaskDuration;
	private final DurationRunnable lastLongestConcurrentRunnable;
	private final DynamicStatsCounter scheduledTaskDuration;
	private final DurationRunnable lastLongestScheduledRunnable;

	private final DynamicStatsCounter selectedKeysTimeStats;
	private final DynamicStatsCounter localTasksTimeStats;
	private final DynamicStatsCounter concurrentTasksTimeStats;
	private final DynamicStatsCounter scheduledTasksTimeStats;

	private boolean monitoring;
	private long monitoringTimestamp;
	private long monitoringLoop;

	// long loop monitoring
	private volatile long longLoopMillis = DEFAULT_LONGLOOP_TIME;
	private final DynamicRateCounter longLoopsRate;
	private final DynamicStatsCounter longLoopLocalTasksStats;
	private final DynamicStatsCounter longLoopConcurrentTasksStats;
	private final DynamicStatsCounter longLoopScheduledTasksStats;
	private String longLoopLongestLocalTask;
	private String longLoopLongestConcurrentTask;
	private String longLoopLongestScheduledTask;
	private StatsSnapshot lastLongLoopStats;

	public NioEventloopStats(CurrentTimeProvider timeProvider) {
		this.selectorSelectTimeStats = createDynamicStatsCounter(timeProvider);
		this.businessLogicTimeStats = createDynamicStatsCounter(timeProvider);
		this.selectedKeysStats = createDynamicStatsCounter(timeProvider);
		this.invalidKeysStats = createDynamicStatsCounter(timeProvider);
		this.acceptKeysStats = createDynamicStatsCounter(timeProvider);
		this.connectKeysStats = createDynamicStatsCounter(timeProvider);
		this.readKeysStats = createDynamicStatsCounter(timeProvider);
		this.writeKeysStats = createDynamicStatsCounter(timeProvider);
		this.localTasksStats = createDynamicStatsCounter(timeProvider);
		this.concurrentTasksStats = createDynamicStatsCounter(timeProvider);
		this.scheduledTasksStats = createDynamicStatsCounter(timeProvider);

		this.localTaskDuration = createDynamicStatsCounter(timeProvider);
		this.lastLongestLocalRunnable = new DurationRunnable();
		this.concurrentTaskDuration = createDynamicStatsCounter(timeProvider);
		this.lastLongestConcurrentRunnable = new DurationRunnable();
		this.scheduledTaskDuration = createDynamicStatsCounter(timeProvider);
		this.lastLongestScheduledRunnable = new DurationRunnable();

		this.selectedKeysTimeStats = createDynamicStatsCounter(timeProvider);
		this.localTasksTimeStats = createDynamicStatsCounter(timeProvider);
		this.concurrentTasksTimeStats = createDynamicStatsCounter(timeProvider);
		this.scheduledTasksTimeStats = createDynamicStatsCounter(timeProvider);

		this.longLoopsRate = new DynamicRateCounter(DEFAULT_STATS_WINDOW, DEFAULT_STATS_PRECISION, timeProvider);
		this.longLoopLocalTasksStats = createDynamicStatsCounter(timeProvider);
		this.longLoopConcurrentTasksStats = createDynamicStatsCounter(timeProvider);
		this.longLoopScheduledTasksStats = createDynamicStatsCounter(timeProvider);
	}

	private static DynamicStatsCounter createDynamicStatsCounter(CurrentTimeProvider timeProvider) {
		return new DynamicStatsCounter(DEFAULT_STATS_WINDOW, DEFAULT_STATS_PRECISION, timeProvider);
	}

	void incMonitoringLoop() {
		if (isMonitoring()) {
			monitoringLoop++;
		}
	}

	void updateBusinessLogicTime(long timestamp, long businessLogicTime) {
		businessLogicTimeStats.add((int) businessLogicTime);

		if (!isMonitoring())
			return;
		if (businessLogicTime > longLoopMillis) {
			longLoopsRate.recordEvent();
			longLoopLocalTasksStats.add(localTasksStats.getLastValue());
			longLoopConcurrentTasksStats.add(concurrentTasksStats.getLastValue());
			longLoopScheduledTasksStats.add(scheduledTasksStats.getLastValue());
			longLoopLongestLocalTask = lastLongestLocalRunnable.toString();
			longLoopLongestConcurrentTask = lastLongestConcurrentRunnable.toString();
			longLoopLongestScheduledTask = lastLongestScheduledRunnable.toString();
			lastLongLoopStats = getStatsSnapshot(timestamp);
		}
	}

	void updateSelectorSelectTime(long selectorSelectTime) {
		selectorSelectTimeStats.add((int) selectorSelectTime);
	}

	void updateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys, int connectKeys, int readKeys, int writeKeys) {
		selectedKeysStats.add(lastSelectedKeys);
		invalidKeysStats.add(invalidKeys);
		acceptKeysStats.add(acceptKeys);
		connectKeysStats.add(connectKeys);
		readKeysStats.add(readKeys);
		writeKeysStats.add(writeKeys);
	}

	void updateSelectedKeysTimeStats(@Nullable Stopwatch sw) {
		if (sw != null)
			selectedKeysTimeStats.add((int) sw.elapsed(TimeUnit.MILLISECONDS));
	}

	private void updateTaskDuration(DynamicStatsCounter counter, DurationRunnable longestCounter, Runnable runnable, @Nullable Stopwatch sw) {
		if (sw != null) {
			int elapsed = (int) sw.elapsed(TimeUnit.MICROSECONDS);
			counter.add(elapsed);
			if (elapsed > longestCounter.getDuration()) {
				longestCounter.update(runnable, elapsed);
			}
		}
	}

	void updateLocalTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(localTaskDuration, lastLongestLocalRunnable, runnable, sw);
	}

	void updateLocalTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			localTasksTimeStats.add((int) sw.elapsed(TimeUnit.MILLISECONDS));
		localTasksStats.add(newTasks);
	}

	void updateConcurrentTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(concurrentTaskDuration, lastLongestConcurrentRunnable, runnable, sw);
	}

	void updateConcurrentTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			concurrentTasksTimeStats.add((int) sw.elapsed(TimeUnit.MICROSECONDS));
		concurrentTasksStats.add(newTasks);
	}

	void updateScheduledTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(scheduledTaskDuration, lastLongestScheduledRunnable, runnable, sw);
	}

	void updateScheduledTasksStats(int newTasks, @Nullable Stopwatch sw) {
		if (sw != null)
			scheduledTasksTimeStats.add((int) sw.elapsed(TimeUnit.MILLISECONDS));
		scheduledTasksStats.add(newTasks);
	}

	// Exceptions stats
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

	private final Map<ExceptionMarker, LastExceptionCounter> exceptionCounters = new HashMap<>();

	public static ExceptionMarker exceptionMarker(Class<?> clazz, String name) {
		return new ExceptionMarkerImpl(clazz, name);
	}

	LastExceptionCounter getExceptionCounter(ExceptionMarker marker) {
		return exceptionCounters.get(marker);
	}

	LastExceptionCounter ensureExceptionCounter(ExceptionMarker marker) {
		if (!exceptionCounters.containsKey(marker))
			exceptionCounters.put(marker, new LastExceptionCounter(marker.getMarker()));
		return exceptionCounters.get(marker);
	}

	void updateExceptionCounter(ExceptionMarker marker, Throwable e, Object o, long timestamp) {
		ensureExceptionCounter(marker).update(e, o, timestamp);
	}

	void resetExceptionCounter(ExceptionMarker marker) {
		LastExceptionCounter counter = exceptionCounters.get(marker);
		if (counter != null)
			counter.reset();
	}

	// Snapshot stats
	public final class StatsSnapshot {
		private final long timestamp;
		private final long numberLoop;
		private final long selectorSelectTime;
		private final long selectedKeysTime;
		private final long acceptKeys;
		private final long connectKeys;
		private final long readKeys;
		private final long writeKeys;
		private final long invalidKeys;
		private final long localRunnables;
		private final long concurrentRunnables;
		private final long scheduledRunnables;
		private final long localRunnablesTime;
		private final long concurrentRunnablesTime;
		private final long scheduledRunnablesTime;

		private StatsSnapshot(long timestamp) {
			this.timestamp = timestamp;
			this.numberLoop = monitoringLoop;
			this.selectorSelectTime = (long)selectorSelectTimeStats.getLastValue();
			this.selectedKeysTime = (long)selectedKeysTimeStats.getLastValue();
			this.acceptKeys = (long)acceptKeysStats.getLastValue();
			this.connectKeys = (long)connectKeysStats.getLastValue();
			this.readKeys = (long)readKeysStats.getLastValue();
			this.writeKeys = (long)writeKeysStats.getLastValue();
			this.invalidKeys = (long)invalidKeysStats.getLastValue();
			this.localRunnables = (long)localTasksStats.getLastValue();
			this.concurrentRunnables = (long)concurrentTasksStats.getLastValue();
			this.scheduledRunnables = (long)scheduledTasksStats.getLastValue();
			this.localRunnablesTime = (long)localTasksTimeStats.getLastValue();
			this.concurrentRunnablesTime = (long)concurrentTasksTimeStats.getLastValue();
			this.scheduledRunnablesTime = (long)scheduledTasksTimeStats.getLastValue();
		}

		public long getTimestamp() {
			return timestamp;
		}

		public long getNumberLoop() {
			return numberLoop;
		}

		public long getSelectorSelectTime() {
			return selectorSelectTime;
		}

		public long getSelectedKeysTime() {
			return selectedKeysTime;
		}

		public long getAcceptKeys() {
			return acceptKeys;
		}

		public long getConnectKeys() {
			return connectKeys;
		}

		public long getReadKeys() {
			return readKeys;
		}

		public long getWriteKeys() {
			return writeKeys;
		}

		public long getInvalidKeys() {
			return invalidKeys;
		}

		public long getLocalRunnables() {
			return localRunnables;
		}

		public long getConcurrentRunnables() {
			return concurrentRunnables;
		}

		public long getScheduledRunnables() {
			return scheduledRunnables;
		}

		public long getLocalRunnablesTime() {
			return localRunnablesTime;
		}

		public long getConcurrentRunnablesTime() {
			return concurrentRunnablesTime;
		}

		public long getScheduledRunnablesTime() {
			return scheduledRunnablesTime;
		}
	}

	public StatsSnapshot getStatsSnapshot(long timestamp) {
		if (!monitoring) return null;
		return new StatsSnapshot(timestamp);
	}

	// JMX
	@Override
	public void resetStats() {
		selectorSelectTimeStats.reset();
		businessLogicTimeStats.reset();

		selectedKeysStats.reset();
		invalidKeysStats.reset();
		acceptKeysStats.reset();
		connectKeysStats.reset();
		readKeysStats.reset();
		writeKeysStats.reset();

		localTasksStats.reset();
		concurrentTasksStats.reset();
		scheduledTasksStats.reset();

		localTaskDuration.reset();
		concurrentTaskDuration.reset();
		scheduledTaskDuration.reset();

		selectedKeysTimeStats.reset();
		localTasksTimeStats.reset();
		concurrentTasksTimeStats.reset();
		scheduledTasksTimeStats.reset();

		for (LastExceptionCounter counter : exceptionCounters.values()) {
			counter.reset();
		}

		longLoopsRate.reset();
		longLoopLocalTasksStats.reset();
		lastLongestLocalRunnable.reset();
		longLoopConcurrentTasksStats.reset();
		lastLongestConcurrentRunnable.reset();
		longLoopScheduledTasksStats.reset();
		lastLongestScheduledRunnable.reset();
	}

	@Override
	public void startMonitoring() {
		monitoring = true;
		monitoringTimestamp = System.currentTimeMillis();
		monitoringLoop = 0;

		lastLongestLocalRunnable.reset();
		lastLongestConcurrentRunnable.reset();
		lastLongestScheduledRunnable.reset();
	}

	@Override
	public void stopMonitoring() {
		monitoring = false;
		monitoringTimestamp = 0;
	}

	@Override
	public boolean isMonitoring() {
		return monitoring;
	}

	@Override
	public long getMonitoringLoop() {
		return monitoringLoop;
	}

	@Override
	public String getMonitoringTime() {
		if (!isMonitoring())
			return null;
		return MBeanFormat.formatDuration(System.currentTimeMillis() - monitoringTimestamp);
	}

	@Override
	public int getSelectedKeys() {
		return (int)selectedKeysStats.getLastValue();
	}

	@Override
	public long getInvalidKeys() {
		return (long)invalidKeysStats.getLastValue();
	}

	@Override
	public long getAcceptKeys() {
		return (long)acceptKeysStats.getLastValue();
	}

	@Override
	public long getConnectKeys() {
		return (long)connectKeysStats.getLastValue();
	}

	@Override
	public long getReadKeys() {
		return (long)readKeysStats.getLastValue();
	}

	@Override
	public long getWriteKeys() {
		return (long)writeKeysStats.getLastValue();
	}

	@Override
	public String getSelectedKeysStats() {
		return selectedKeysStats.toString();
	}

	@Override
	public String getInvalidKeysStats() {
		return invalidKeysStats.toString();
	}

	@Override
	public String getAcceptKeysStats() {
		return acceptKeysStats.toString();
	}

	@Override
	public String getConnectKeysStats() {
		return connectKeysStats.toString();
	}

	@Override
	public String getReadKeysStats() {
		return readKeysStats.toString();
	}

	@Override
	public String getWriteKeysStats() {
		return writeKeysStats.toString();
	}

	@Override
	public int getSelectedKeysMillis() {
		return (int)selectedKeysTimeStats.getLastValue();
	}

	@Override
	public String getSelectedKeysMillisStats() {
		return selectedKeysTimeStats.toString();
	}

	@Override
	public long getBusinessLogicMillis() {
		return (long)businessLogicTimeStats.getLastValue();
	}

	@Override
	public String getBusinessLogicMillisStats() {
		return businessLogicTimeStats.toString();
	}

	@Override
	public long getSelectorSelectMillis() {
		return (long)selectorSelectTimeStats.getLastValue();
	}

	@Override
	public String getSelectorSelectMillisStats() {
		return selectorSelectTimeStats.toString();
	}

	@Override
	public CompositeData[] getLastExceptions() throws OpenDataException {
		if (exceptionCounters.isEmpty())
			return null;
		List<CompositeData> results = new ArrayList<>();
		for (LastExceptionCounter counter : exceptionCounters.values()) {
			results.add(counter.compositeData());
		}
		return results.toArray(new CompositeData[results.size()]);
	}

	// Tasks stats
	@Override
	public int getLocalTaskMicros() {
		return (int)localTaskDuration.getLastValue();
	}

	@Override
	public String getLocalTaskStats() {
		return localTaskDuration.toString();
	}

	@Override
	public String getLocalTaskLongestMicros() {
		return lastLongestLocalRunnable.toString();
	}

	@Override
	public int getLocalTasksMillis() {
		return (int)localTasksTimeStats.getLastValue();
	}

	@Override
	public String getLocalTasksStats() {
		return localTasksTimeStats.toString();
	}

	@Override
	public int getLocalTasksPerLoop() {
		return (int)localTasksStats.getLastValue();
	}

	@Override
	public String getLocalTasksPerLoopStats() {
		return localTasksStats.toString();
	}

	@Override
	public int getConcurrentTaskMicros() {
		return (int)concurrentTaskDuration.getLastValue();
	}

	@Override
	public String getConcurrentTaskStats() {
		return concurrentTaskDuration.toString();
	}

	@Override
	public String getConcurrentTaskLongestMicros() {
		return lastLongestConcurrentRunnable.toString();
	}

	@Override
	public int getConcurrentTasksMillis() {
		return (int)concurrentTasksTimeStats.getLastValue();
	}

	@Override
	public String getConcurrentTasksStats() {
		return concurrentTasksTimeStats.toString();
	}

	@Override
	public int getConcurrentTasksPerLoop() {
		return (int)concurrentTasksStats.getLastValue();
	}

	@Override
	public String getConcurrentTasksPerLoopStats() {
		return concurrentTasksStats.toString();
	}

	@Override
	public int getScheduledTaskMicros() {
		return (int)scheduledTaskDuration.getLastValue();
	}

	@Override
	public String getScheduledTaskStats() {
		return scheduledTaskDuration.toString();
	}

	@Override
	public String getScheduledTaskLongestMicros() {
		return lastLongestScheduledRunnable.toString();
	}

	@Override
	public int getScheduledTasksMillis() {
		return (int)scheduledTasksTimeStats.getLastValue();
	}

	@Override
	public String getScheduledTasksStats() {
		return scheduledTasksTimeStats.toString();
	}

	@Override
	public int getScheduledTasksPerLoop() {
		return (int)scheduledTasksStats.getLastValue();
	}

	@Override
	public String getScheduledTasksPerLoopStats() {
		return scheduledTasksStats.toString();
	}

	// long loop stats
	@Override
	public long getLongLoopMillis() {
		return longLoopMillis;
	}

	@Override
	public void setLongLoopMillis(long timeLongLoop) {
		this.longLoopMillis = timeLongLoop;
	}

	@Override
	public String getLongLoopsRate() {
		return String.valueOf(longLoopsRate.getRate());
	}

	@Override
	public long getLongLoopLocalTasks() {
		return (long)longLoopLocalTasksStats.getLastValue();
	}

	@Override
	public String getLongLoopLocalTasksStats() {
		return longLoopLocalTasksStats.toString();
	}

	@Override
	public String getLongLoopLocalTaskLongest() {
		return longLoopLongestLocalTask;
	}

	@Override
	public long getLongLoopConcurrentTasks() {
		return (long)longLoopConcurrentTasksStats.getLastValue();
	}

	@Override
	public String getLongLoopConcurrentTasksStats() {
		return longLoopConcurrentTasksStats.toString();
	}

	@Override
	public String getLongLoopConcurrentTaskLongest() {
		return longLoopLongestConcurrentTask;
	}

	@Override
	public long getLongLoopScheduledTasks() {
		return (long)longLoopScheduledTasksStats.getLastValue();
	}

	@Override
	public String getLongLoopScheduledTasksStats() {
		return longLoopScheduledTasksStats.toString();
	}

	@Override
	public String getLongLoopScheduledTaskLongest() {
		return longLoopLongestScheduledTask;
	}

	@Override
	public CompositeData getLastLongLoopStats() throws OpenDataException {
		if (lastLongLoopStats == null)
			return null;
		return CompositeDataBuilder.builder("LongLoop stats")
				.add("NumberLoop", SimpleType.LONG, lastLongLoopStats.getNumberLoop())
				.add("SelectorSelectMillis", SimpleType.LONG, lastLongLoopStats.getSelectorSelectTime())
				.add("SelectedKeysMillis", SimpleType.LONG, lastLongLoopStats.getSelectedKeysTime())
				.add("AcceptKeys", SimpleType.LONG, lastLongLoopStats.getAcceptKeys())
				.add("ConnectKeys", SimpleType.LONG, lastLongLoopStats.getConnectKeys())
				.add("ReadKeys", SimpleType.LONG, lastLongLoopStats.getReadKeys())
				.add("WriteKeys", SimpleType.LONG, lastLongLoopStats.getWriteKeys())
				.add("InvalidKeys", SimpleType.LONG, lastLongLoopStats.getInvalidKeys())
				.add("LocalRunnables", SimpleType.LONG, lastLongLoopStats.getLocalRunnables())
				.add("ConcurrentRunnables", SimpleType.LONG, lastLongLoopStats.getConcurrentRunnables())
				.add("ScheduledRunnables", SimpleType.LONG, lastLongLoopStats.getScheduledRunnables())
				.add("LocalRunnablesMillis", SimpleType.LONG, lastLongLoopStats.getLocalRunnablesTime())
				.add("ConcurrentRunnablesMillis", SimpleType.LONG, lastLongLoopStats.getConcurrentRunnablesTime())
				.add("ScheduledRunnablesMillis", SimpleType.LONG, lastLongLoopStats.getScheduledRunnablesTime())
				.build();
	}

}
