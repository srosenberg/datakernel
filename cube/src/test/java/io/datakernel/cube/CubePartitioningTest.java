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

package io.datakernel.cube;

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.aggregation.fieldtype.FieldTypes;
import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.Cube.AggregationScheme;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogToCubeMetadataStorage;
import io.datakernel.logfs.LogToCubeRunner;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation.AggregationPredicates.alwaysTrue;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofDouble;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.cube.CubeTestUtils.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class CubePartitioningTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final String DATABASE_PROPERTIES_PATH = "test.properties";
	private static final SQLDialect DATABASE_DIALECT = SQLDialect.MYSQL;
	private static final String LOG_PARTITION_NAME = "partitionA";
	private static final List<String> LOG_PARTITIONS = asList(LOG_PARTITION_NAME);
	private static final String LOG_NAME = "testlog";

	@Ignore("Requires DB access to run")
	@SuppressWarnings("ConstantConditions")
	@Test
	public void test() throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool();

		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		Path logsDir = temporaryFolder.newFolder().toPath();

		Configuration jooqConfiguration = getJooqConfiguration(DATABASE_PROPERTIES_PATH, DATABASE_DIALECT);
		AggregationChunkStorage aggregationChunkStorage =
				LocalFsChunkStorage.create(eventloop, executor, aggregationsDir);
		CubeMetadataStorageSql cubeMetadataStorageSql =
				CubeMetadataStorageSql.create(eventloop, executor, jooqConfiguration, "processId");
		LogToCubeMetadataStorage logToCubeMetadataStorage =
				getLogToCubeMetadataStorage(eventloop, executor, jooqConfiguration, cubeMetadataStorageSql);

		Cube cube = Cube.create(eventloop, executor, classLoader, cubeMetadataStorageSql, aggregationChunkStorage)
				.withDimension("date", FieldTypes.ofLocalDate())
				.withDimension("advertiser", FieldTypes.ofInt())
				.withDimension("campaign", FieldTypes.ofInt())
				.withDimension("banner", FieldTypes.ofInt())
				.withMeasure("impressions", sum(ofLong()))
				.withMeasure("clicks", sum(ofLong()))
				.withMeasure("conversions", sum(ofLong()))
				.withMeasure("revenue", sum(ofDouble()))
				.withRelation("campaign", "advertiser")
				.withRelation("banner", "campaign")
				.withAggregation(AggregationScheme.id("date").withDimensions("date").withMeasures(LogItem.MEASURES).withPartitioningKey("date"));

		LogManager<LogItem> logManager = getLogManager(LogItem.class, eventloop, executor, classLoader, logsDir);
		LogToCubeRunner<LogItem> logToCubeRunner = LogToCubeRunner.create(eventloop, cube, logManager,
				LogItemSplitter.factory(), LOG_NAME, LOG_PARTITIONS, logToCubeMetadataStorage);

		// Save and aggregate logs
		List<LogItem> listOfRandomLogItems = LogItem.getListOfRandomLogItems(100);
		StreamProducers.OfIterator<LogItem> producerOfRandomLogItems = new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(IgnoreCompletionCallback.create());
		eventloop.run();

		List<LogItem> listOfRandomLogItems2 = LogItem.getListOfRandomLogItems(300);
		producerOfRandomLogItems = new StreamProducers.OfIterator<>(eventloop, listOfRandomLogItems2.iterator());
		producerOfRandomLogItems.streamTo(logManager.consumer(LOG_PARTITION_NAME));
		eventloop.run();

		logToCubeRunner.processLog(IgnoreCompletionCallback.create());
		eventloop.run();

		// Load metadata
		cube.loadChunks(IgnoreCompletionCallback.create());
		eventloop.run();

		Map<Long, AggregationChunk> chunks = cube.getAggregation("date").getMetadata().getChunks();
		assertEquals(22, chunks.size());

		CubeQuery query = CubeQuery.create().withAttributes("date").withMeasures("clicks");
		StreamConsumers.ToList<LogItem> queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class).streamTo(queryResultConsumer);
		eventloop.run();

		// Aggregate manually
		Map<Integer, Long> map = new HashMap<>();
		aggregateToMap(map, listOfRandomLogItems);
		aggregateToMap(map, listOfRandomLogItems2);

		// Check query results
		for (LogItem logItem : queryResultConsumer.getList()) {
			assertEquals(logItem.clicks, map.get(logItem.date).longValue());
		}

		int consolidations = 0;
		while (true) {
			cube.loadChunks(IgnoreCompletionCallback.create());
			eventloop.run();

			ResultCallbackFuture<Boolean> callback = ResultCallbackFuture.create();
			cube.consolidate(100, callback);
			eventloop.run();
			boolean consolidated = callback.isDone() ? callback.get() : false;
			if (consolidated)
				++consolidations;
			else
				break;
		}
		assertEquals(6, consolidations);

		// Load metadata
		cube.loadChunks(IgnoreCompletionCallback.create());
		eventloop.run();

		// Query
		queryResultConsumer = new StreamConsumers.ToList<>(eventloop);
		cube.queryRawStream(asList("date"), asList("clicks"), alwaysTrue(), LogItem.class).streamTo(queryResultConsumer);
		eventloop.run();

		// Check query results
		for (LogItem logItem : queryResultConsumer.getList()) {
			assertEquals(logItem.clicks, map.get(logItem.date).longValue());
		}

		// Check that every chunk contains only one date
		chunks = cube.getAggregation("date").getMetadata().getChunks();
		assertEquals(11, chunks.size());
		for (AggregationChunk chunk : chunks.values()) {
			assertEquals(chunk.getMinPrimaryKey().get(0), chunk.getMaxPrimaryKey().get(0));
		}
	}

	private void aggregateToMap(Map<Integer, Long> map, List<LogItem> logItems) {
		for (LogItem logItem : logItems) {
			int date = logItem.date;
			long clicks = logItem.clicks;
			if (map.get(date) == null) {
				map.put(date, clicks);
			} else {
				Long clicksForDate = map.get(date);
				map.put(date, clicksForDate + clicks);
			}
		}
	}
}
