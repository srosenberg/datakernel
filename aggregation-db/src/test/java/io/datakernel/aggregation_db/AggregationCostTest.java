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
				.withKey("date")
				.withField("clicks");

		assertEquals(100 * 100 * 100, aggregationMetadata.getCost(query), 1e-5);
	}

	@Test
	public void testAggregationCost2() throws Exception {
		AggregationMetadata aggregationMetadata = AggregationMetadata.create(asList("date", "publisher", "advertiser"), asList("clicks"));
		AggregationQuery query = AggregationQuery.create()
				.withKey("date")
				.withField("clicks")
				.withEq("date", 1);

		assertEquals(100 * 100, aggregationMetadata.getCost(query), 1e-5);
	}

	@Test
	public void testAggregationCost3() throws Exception {
		AggregationMetadata aggregationMetadata = AggregationMetadata.create(asList("date", "publisher", "advertiser"), asList("clicks"));
		AggregationQuery query = AggregationQuery.create()
				.withKeys(asList("date", "advertiser"))
				.withField("clicks")
				.withEq("date", 1)
				.withEq("advertiser", 1);

		assertEquals(100 * 100, aggregationMetadata.getCost(query), 1e-5);
	}

	@Test
	public void testAggregationCost4() throws Exception {
		AggregationMetadata aggregationMetadata = AggregationMetadata.create(asList("date", "publisher", "advertiser"), asList("clicks"));
		AggregationQuery query = AggregationQuery.create()
				.withKeys(asList("date", "publisher"))
				.withField("clicks")
				.withEq("date", 1)
				.withEq("publisher", 1);

		assertEquals(100, aggregationMetadata.getCost(query), 1e-5);
	}
}
