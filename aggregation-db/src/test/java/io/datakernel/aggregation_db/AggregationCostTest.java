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

package io.datakernel.aggregation_db;

import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class AggregationCostTest {
	@Test
	public void testAggregationCost1() throws Exception {
		AggregationMetadata aggregationMetadata = AggregationMetadata.create(asList("date", "publisher", "advertiser"), asList("clicks"));
		AggregationQuery query = AggregationQuery.create()
				.key("date")
				.field("clicks");

		assertEquals(100 * 100 * 100, aggregationMetadata.getCost(query), 1e-5);
	}

	@Test
	public void testAggregationCost2() throws Exception {
		AggregationMetadata aggregationMetadata = AggregationMetadata.create(asList("date", "publisher", "advertiser"), asList("clicks"));
		AggregationQuery query = AggregationQuery.create()
				.key("date")
				.field("clicks")
				.eq("date", 1);

		assertEquals(100 * 100, aggregationMetadata.getCost(query), 1e-5);
	}

	@Test
	public void testAggregationCost3() throws Exception {
		AggregationMetadata aggregationMetadata = AggregationMetadata.create(asList("date", "publisher", "advertiser"), asList("clicks"));
		AggregationQuery query = AggregationQuery.create()
				.keys(asList("date", "advertiser"))
				.field("clicks")
				.eq("date", 1)
				.eq("advertiser", 1);

		assertEquals(100 * 100, aggregationMetadata.getCost(query), 1e-5);
	}

	@Test
	public void testAggregationCost4() throws Exception {
		AggregationMetadata aggregationMetadata = AggregationMetadata.create(asList("date", "publisher", "advertiser"), asList("clicks"));
		AggregationQuery query = AggregationQuery.create()
				.keys(asList("date", "publisher"))
				.field("clicks")
				.eq("date", 1)
				.eq("publisher", 1);

		assertEquals(100, aggregationMetadata.getCost(query), 1e-5);
	}
}
