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

import java.util.*;

public class ScheduledTaskStats implements JmxStats<ScheduledTaskStats> {
	private final SortedMap<Integer, Integer> delayToTasks;
	private int delayGranularity = 1;
	private int maxLinesToShow = 100;

	public ScheduledTaskStats() {
		this.delayToTasks = new TreeMap<>();
	}

	public ScheduledTaskStats(Map<Integer, Integer> delayToTasks, int delayGranularity, int maxLinesToShow) {
		this.delayToTasks = new TreeMap<>(delayToTasks);
		this.delayGranularity = delayGranularity;
		this.maxLinesToShow = maxLinesToShow;
	}

	@Override
	public void add(ScheduledTaskStats another) {
		delayGranularity = another.delayGranularity;
		maxLinesToShow = another.maxLinesToShow;
		if (delayToTasks.isEmpty()) {
			delayToTasks.putAll(another.delayToTasks);
		} else {
			for (Map.Entry<Integer, Integer> anotherEntry : another.delayToTasks.entrySet()) {
				int delay = anotherEntry.getKey();
				addValue(delay, anotherEntry.getValue(), delayToTasks);
			}
		}
	}

	private static void addValue(Integer key, Integer value, Map<Integer, Integer> map) {
		Integer oldValue = map.get(key);
		if (oldValue == null) {
			map.put(key, value);
		} else {
			map.put(key, oldValue + value);
		}
	}

	public void setDelayGranularity(int delayGranularity) {
		this.delayGranularity = delayGranularity;
	}

	public void setMaxLinesToShow(int maxLinesToShow) {
		this.maxLinesToShow = maxLinesToShow;
	}

	@JmxAttribute
	public List<String> getScheduledTasksSortedByDelay() {
		List<String> lines = new ArrayList<>();
		lines.add("     Delay     Tasks");
		SortedMap<Integer, Integer> grouped = groupedByPeriod();
		for (Map.Entry<Integer, Integer> entry : grouped.entrySet()) {
			String line = String.format("%10d%10d", entry.getKey(), entry.getValue());
			lines.add(line);
			if (lines.size() > maxLinesToShow) {
				break;
			}
		}
		return lines;
	}

	@JmxAttribute
	public List<String> getScheduledTasksSortedByAmount() {
		Map<Integer, Integer> grouped = groupedByPeriod();

		List<DelayAndTasks> delayAndTasksList = new ArrayList<>(grouped.size());
		for (Map.Entry<Integer, Integer> entry : grouped.entrySet()) {
			delayAndTasksList.add(new DelayAndTasks(entry.getKey(), entry.getValue()));
		}

		Collections.sort(delayAndTasksList, new Comparator<DelayAndTasks>() {
			@Override
			public int compare(DelayAndTasks o1, DelayAndTasks o2) {
				return o2.tasks - o1.tasks;
			}
		});

		List<String> lines = new ArrayList<>();
		lines.add("     Delay     Tasks");
		for (DelayAndTasks delayAndTasks : delayAndTasksList) {
			String line = String.format("%10d%10d", delayAndTasks.getDelay(), delayAndTasks.getTasks());
			lines.add(line);
			if (lines.size() > maxLinesToShow) {
				break;
			}
		}

		return lines;
	}

	private SortedMap<Integer, Integer> groupedByPeriod() {
		SortedMap<Integer, Integer> grouped = new TreeMap<>();
		for (Map.Entry<Integer, Integer> entry : delayToTasks.entrySet()) {
			int roundedKey = (int) ceilMod(entry.getKey(), delayGranularity);
			addValue(roundedKey, entry.getValue(), grouped);
		}
		return grouped;
	}

	public static long ceilMod(long value, long modulo) {
		long remainder = value % modulo;
		return remainder == 0 ? value : value - remainder + modulo;
	}

	private static final class DelayAndTasks {
		private final int delay;
		private final int tasks;

		public DelayAndTasks(int delay, int tasks) {
			this.delay = delay;
			this.tasks = tasks;
		}

		public int getDelay() {
			return delay;
		}

		public int getTasks() {
			return tasks;
		}
	}

}
