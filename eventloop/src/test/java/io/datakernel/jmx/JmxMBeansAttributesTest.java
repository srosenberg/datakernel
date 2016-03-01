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

import io.datakernel.jmx.helper.JmxStatsStub;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class JmxMBeansAttributesTest {

	@Test
	public void retreivesProperMBeanInfo() throws Exception {
		MBeanWithSimpleAttrsAndPojo mbeanOneSample = new MBeanWithSimpleAttrsAndPojo("data", new SamplePojo(5, 100));
		DynamicMBean mbean = createDynamicMBeanFor(mbeanOneSample);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();

		MBeanAttributeInfo[] attributesInfoArr = mBeanInfo.getAttributes();
		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(attributesInfoArr);

		assertEquals(3, nameToAttr.size());

		assertTrue(nameToAttr.containsKey("info"));
		assertTrue(nameToAttr.get("info").isReadable());

		assertTrue(nameToAttr.containsKey("details_count"));
		assertTrue(nameToAttr.get("details_count").isReadable());

		assertTrue(nameToAttr.containsKey("details_sum"));
		assertTrue(nameToAttr.get("details_sum").isReadable());
	}

	@Test
	public void retreivesProperAttributeValues() throws Exception {
		MBeanWithSimpleAttrsAndPojo mbeanOneSample = new MBeanWithSimpleAttrsAndPojo("data", new SamplePojo(5, 100L));
		DynamicMBean mbean = createDynamicMBeanFor(mbeanOneSample);

		assertEquals("data", mbean.getAttribute("info"));
		assertEquals(5, mbean.getAttribute("details_count"));
		assertEquals(100L, mbean.getAttribute("details_sum"));
	}

	/*
	 * tests for aggregation
	 */
	@Test
	public void ifAppropriateAttributesInDifferentMBeansHaveSameValueReturnsThatValue() throws Exception {
		MBeanWithSingleIntAttr mbean_1 = new MBeanWithSingleIntAttr(1);
		MBeanWithSingleIntAttr mbean_2 = new MBeanWithSingleIntAttr(1);

		DynamicMBean mbean = createDynamicMBeanFor(mbean_1, mbean_2);

		assertEquals(1, mbean.getAttribute("value"));
	}

	@Test
	public void ifAppropriateAttributesInDifferentMBeansHaveDifferentValueReturnsNull() throws Exception {
		MBeanWithSingleIntAttr mbean_1 = new MBeanWithSingleIntAttr(1);
		MBeanWithSingleIntAttr mbean_2 = new MBeanWithSingleIntAttr(2);

		DynamicMBean mbean = createDynamicMBeanFor(mbean_1, mbean_2);

		assertEquals(null, mbean.getAttribute("value"));
	}

	@Test
	public void aggregatesJmxStatsUsingTheirAggregationPolicy() throws Exception {
		JmxStatsStub stats_1 = new JmxStatsStub();
		stats_1.recordValue(100);
		stats_1.recordValue(20);

		JmxStatsStub stats_2 = new JmxStatsStub();
		stats_2.recordValue(5);

		MBeanWithJmxStats mBeanWithJmxStats_1 = new MBeanWithJmxStats(stats_1);
		MBeanWithJmxStats mBeanWithJmxStats_2 = new MBeanWithJmxStats(stats_2);

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithJmxStats_1, mBeanWithJmxStats_2);

		assertEquals(3, mbean.getAttribute("jmxStatsStub_count"));
		assertEquals(125L, mbean.getAttribute("jmxStatsStub_sum"));
	}

	@Test
	public void concatenatesListAttributesFromDifferentMBeans() throws Exception {
		MBeanWithListAttr mBeanWithListAttr_1 = new MBeanWithListAttr(asList("a", "b"));
		MBeanWithListAttr mBeanWithListAttr_2 = new MBeanWithListAttr(new ArrayList<String>());
		MBeanWithListAttr mBeanWithListAttr_3 = new MBeanWithListAttr(asList("w"));

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithListAttr_1, mBeanWithListAttr_2, mBeanWithListAttr_3);

		assertArrayEquals(new String[]{"a", "b", "w"}, (String[]) mbean.getAttribute("list"));
	}

	@Test
	public void properlyAggregateMapsByKeyAccordingToTheirValueAggregationPolicy() throws Exception {
		Map<String, Integer> map_1 = new HashMap<>();
		map_1.put("a", 1);
		map_1.put("b", 2);
		map_1.put("c", 100);

		Map<String, Integer> map_2 = new HashMap<>();
		map_2.put("b", 2);
		map_2.put("c", 200);
		map_2.put("d", 5);

		MBeanWithMap mBeanWithMap_1 = new MBeanWithMap(map_1);
		MBeanWithMap mBeanWithMap_2 = new MBeanWithMap(map_2);

		DynamicMBean mbean = createDynamicMBeanFor(mBeanWithMap_1, mBeanWithMap_2);

		TabularData tabularData = (TabularData) mbean.getAttribute("nameToNumber");

		assertEquals(4, tabularData.size());

		CompositeData row_1 = tabularData.get(keyForTabularData("a"));
		assertEquals(1, row_1.get("value"));

		CompositeData row_2 = tabularData.get(keyForTabularData("b"));
		assertEquals(2, row_2.get("value"));

		CompositeData row_3 = tabularData.get(keyForTabularData("c"));
		assertEquals(null, row_3.get("value"));

		CompositeData row_4 = tabularData.get(keyForTabularData("d"));
		assertEquals(5, row_4.get("value"));
	}

	/*
	 * helper methods
 	 */

	public static DynamicMBean createDynamicMBeanFor(ConcurrentJmxMBean... objects) throws Exception {
		boolean refreshEnabled = false;
		return JmxMBeans.factory().createFor(asList(objects), refreshEnabled);
	}

	public static Map<String, MBeanAttributeInfo> nameToAttribute(MBeanAttributeInfo[] attrs) {
		Map<String, MBeanAttributeInfo> nameToAttr = new HashMap<>();
		for (MBeanAttributeInfo attr : attrs) {
			nameToAttr.put(attr.getName(), attr);
		}
		return nameToAttr;
	}

	public static Object[] keyForTabularData(String key) {
		return new String[]{key};
	}

	/*
	 * helper classes
	 */

	public static final class SamplePojo {
		private final int count;
		private final long sum;

		public SamplePojo(int count, long sum) {
			this.count = count;
			this.sum = sum;
		}

		@JmxAttribute
		public int getCount() {
			return count;
		}

		@JmxAttribute
		public long getSum() {
			return sum;
		}
	}

	public static final class MBeanWithSimpleAttrsAndPojo implements ConcurrentJmxMBean {
		private final String info;
		private final SamplePojo samplePojo;

		public MBeanWithSimpleAttrsAndPojo(String info, SamplePojo samplePojo) {
			this.info = info;
			this.samplePojo = samplePojo;
		}

		@JmxAttribute
		public String getInfo() {
			return info;
		}

		@JmxAttribute(name = "details")
		public SamplePojo getSamplePojo() {
			return samplePojo;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}

	public static final class MBeanWithListAttr implements ConcurrentJmxMBean {
		private final List<String> list;

		public MBeanWithListAttr(List<String> list) {
			this.list = list;
		}

		@JmxAttribute
		public List<String> getList() {
			return list;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}

	public static final class MBeanWithSingleIntAttr implements ConcurrentJmxMBean {
		private final int value;

		public MBeanWithSingleIntAttr(int value) {
			this.value = value;
		}

		@JmxAttribute
		public int getValue() {
			return value;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}

	public static final class MBeanWithJmxStats implements ConcurrentJmxMBean {
		private final JmxStatsStub jmxStatsStub;

		public MBeanWithJmxStats(JmxStatsStub jmxStatsStub) {
			this.jmxStatsStub = jmxStatsStub;
		}

		@JmxAttribute
		public JmxStatsStub getJmxStatsStub() {
			return jmxStatsStub;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}

	public static final class MBeanWithMap implements ConcurrentJmxMBean {
		private final Map<String, Integer> nameToNumber;

		public MBeanWithMap(Map<String, Integer> nameToNumber) {
			this.nameToNumber = nameToNumber;
		}

		@JmxAttribute
		public Map<String, Integer> getNameToNumber() {
			return nameToNumber;
		}

		@Override
		public Executor getJmxExecutor() {
			return Executors.newSingleThreadExecutor();
		}
	}
}