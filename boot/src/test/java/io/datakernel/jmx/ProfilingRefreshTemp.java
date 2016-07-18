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

import io.datakernel.eventloop.Eventloop;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

public class ProfilingRefreshTemp {
	Random random = new Random();
	final Eventloop eventloop = new Eventloop();
	final int updPeriod = 200;

	@Ignore
	@Test
	public void test() {
//		JmxRegistry jmxRegistry = new JmxRegistry(ManagementFactory.getPlatformMBeanServer(), JmxMBeans.factory());
//		jmxRegistry.registerSingleton(Key.get(Eventloop.class), eventloop);

		int mbeansAmount = 100;
		List<StatsMBean> mbeans = new ArrayList<>();
		for (int i = 0; i < mbeansAmount; i++) {
			StatsMBean statsMBean = new StatsMBean(eventloop);
			for (int j = 0; j < 100; j++) {
				String key = Integer.toString(j + 1);
				statsMBean.mapStats.put(key, new MapInnerStats());
			}
			mbeans.add(statsMBean);
		}
		JmxMBeans.factory().createFor(mbeans, true);

		eventloop.post(createUpdateTask(mbeans));

		eventloop.run();
	}

	public Runnable createUpdateTask(final List<StatsMBean> mbeans) {
		return new UpdateTask(mbeans);
	}

	public final class UpdateTask implements Runnable {
		List<StatsMBean> mbeans;

		public UpdateTask(List<StatsMBean> mbeans) {
			this.mbeans = mbeans;
		}

		@Override
		public void run() {
			for (StatsMBean statsMBean : mbeans) {
				int rand = random.nextInt(50);
				statsMBean.eventStatsOne.recordEvents(rand + 10);
				statsMBean.eventStatsTwo.recordEvents(rand + 15);
				statsMBean.valueStatsOne.recordValue(rand + 100);
				statsMBean.valueStatsTwo.recordValue(rand + 150);

				statsMBean.childStats.eventStatsOne.recordEvents(3);
				statsMBean.childStats.eventStatsTwo.recordEvents(3);
				statsMBean.childStats.valueStatsOne.recordValue(35);
				statsMBean.childStats.valueStatsTwo.recordValue(35);
				statsMBean.childStats.valueStatsThree.recordValue(35);

//				if (random.nextDouble() < 0.5) {
//					int size = statsMBean.mapStats.size();
////						if (size < 1000) {
////							System.out.println("------");
////							System.out.println("add map element");
////							System.out.println("------");
//					statsMBean.mapStats.put(Integer.toString(size + 1), new MapInnerStats());
////						}
//				}

				for (MapInnerStats mapInnerStats : statsMBean.mapStats.values()) {
					mapInnerStats.eventStats.recordEvents(rand + 15);
					mapInnerStats.valueStats.recordValue(rand + 50);
				}
			}

			eventloop.schedule(eventloop.currentTimeMillis() + updPeriod, createUpdateTask(mbeans));
		}
	}

	public static final class StatsMBean implements EventloopJmxMBean {
		private Eventloop eventloop;

		private EventStats eventStatsOne = new EventStats();
		private EventStats eventStatsTwo = new EventStats();
		private ValueStats valueStatsOne = new ValueStats();
		private ValueStats valueStatsTwo = new ValueStats();

		private ChildStats childStats = new ChildStats();

		private Map<String, MapInnerStats> mapStats = new HashMap<>();

		public StatsMBean(Eventloop eventloop) {
			this.eventloop = eventloop;
		}

		@JmxAttribute
		public EventStats getEventStatsOne() {
			return eventStatsOne;
		}

		@JmxAttribute
		public EventStats getEventStatsTwo() {
			return eventStatsTwo;
		}

		@JmxAttribute
		public ValueStats getValueStatsOne() {
			return valueStatsOne;
		}

		@JmxAttribute
		public ValueStats getValueStatsTwo() {
			return valueStatsTwo;
		}

		@JmxAttribute
		public ChildStats getChildStats() {
			return childStats;
		}

		@JmxAttribute
		public Map<String, MapInnerStats> getMapStats() {
			return mapStats;
		}

		@Override
		public Eventloop getEventloop() {
			return eventloop;
		}
	}

	public static final class ChildStats {
		private EventStats eventStatsOne = new EventStats();
		private EventStats eventStatsTwo = new EventStats();
		private ValueStats valueStatsOne = new ValueStats();
		private ValueStats valueStatsTwo = new ValueStats();
		private ValueStats valueStatsThree = new ValueStats();

		@JmxAttribute
		public EventStats getEventStatsOne() {
			return eventStatsOne;
		}

		@JmxAttribute
		public ValueStats getValueStatsOne() {
			return valueStatsOne;
		}

		@JmxAttribute
		public EventStats getEventStatsTwo() {
			return eventStatsTwo;
		}

		@JmxAttribute
		public ValueStats getValueStatsTwo() {
			return valueStatsTwo;
		}

		@JmxAttribute
		public ValueStats getValueStatsThree() {
			return valueStatsThree;
		}
	}

	public static final class MapInnerStats implements JmxRefreshable {
		private EventStats eventStats = new EventStats();
		private ValueStats valueStats = new ValueStats();

		@JmxAttribute
		public EventStats getEventStats() {
			return eventStats;
		}

		@JmxAttribute
		public ValueStats getValueStats() {
			return valueStats;
		}

		@Override
		public void refresh(long timestamp) {
			eventStats.refresh(timestamp);
			valueStats.refresh(timestamp);
		}
	}
}
