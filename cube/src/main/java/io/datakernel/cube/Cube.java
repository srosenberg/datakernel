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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import io.datakernel.aggregation.*;
import io.datakernel.aggregation.AggregationMetadataStorage.LoadedChunks;
import io.datakernel.aggregation.fieldtype.FieldType;
import io.datakernel.aggregation.measure.Measure;
import io.datakernel.aggregation.util.AsyncResultsTracker.AsyncResultsTrackerMultimap;
import io.datakernel.async.*;
import io.datakernel.codegen.*;
import io.datakernel.cube.CubeMetadataStorage.CubeLoadedChunks;
import io.datakernel.cube.asm.MeasuresFunction;
import io.datakernel.cube.asm.RecordFunction;
import io.datakernel.cube.asm.TotalsFunction;
import io.datakernel.cube.attributes.AttributeResolver;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.ValueStats;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamMap;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.stream.processor.StreamSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.*;
import static com.google.common.collect.Sets.*;
import static com.google.common.primitives.Primitives.isWrapperType;
import static io.datakernel.aggregation.AggregationUtils.*;
import static io.datakernel.aggregation.util.AsyncResultsTracker.ofMultimap;
import static io.datakernel.async.AsyncRunnables.runInParallel;
import static io.datakernel.codegen.ExpressionComparator.leftField;
import static io.datakernel.codegen.ExpressionComparator.rightField;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.cube.Utils.*;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;

/**
 * Represents an OLAP cube. Provides methods for loading and querying data.
 * Also provides functionality for managing aggregations.
 */
@SuppressWarnings("unchecked")
public final class Cube implements ICube, EventloopJmxMBean {
	private static final Logger logger = LoggerFactory.getLogger(Cube.class);

	public static final int DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD = 300;

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final DefiningClassLoader classLoader;
	private final CubeMetadataStorage cubeMetadataStorage;
	private final AggregationChunkStorage aggregationChunkStorage;

	private final Map<String, FieldType> fieldTypes = new LinkedHashMap<>();
	private final Map<String, FieldType> dimensionTypes = new LinkedHashMap<>();
	private final Map<String, Measure> measures = new LinkedHashMap<>();
	private final Map<String, ComputedMeasure> computedMeasures = newLinkedHashMap();

	private ListenableResultCallback<LoadedChunks> loadChunksCallback;

	private static final class AttributeResolverContainer {
		private final List<String> attributes = new ArrayList<>();
		private final List<String> dimensions;
		private final AttributeResolver resolver;

		private AttributeResolverContainer(List<String> dimensions, AttributeResolver resolver) {
			this.dimensions = dimensions;
			this.resolver = resolver;
		}
	}

	private final List<AttributeResolverContainer> attributeResolvers = newArrayList();
	private final Map<String, Class<?>> attributeTypes = new LinkedHashMap<>();
	private final Map<String, AttributeResolverContainer> attributes = new LinkedHashMap<>();

	private final Map<String, String> childParentRelationships = new LinkedHashMap<>();
	private final Multimap<String, String> parentChildRelationships = LinkedHashMultimap.create();

	// settings
	private int aggregationsChunkSize = Aggregation.DEFAULT_CHUNK_SIZE;
	private int aggregationsSorterItemsInMemory = Aggregation.DEFAULT_SORTER_ITEMS_IN_MEMORY;
	private int aggregationsSorterBlockSize = Aggregation.DEFAULT_SORTER_BLOCK_SIZE;
	private int aggregationsMaxChunksToConsolidate = Aggregation.DEFAULT_MAX_CHUNKS_TO_CONSOLIDATE;
	private boolean aggregationsIgnoreChunkReadingExceptions = false;

	private int maxOverlappingChunksToProcessLogs = Cube.DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD;
	private int maxIncrementalReloadPeriodMillis = Aggregation.DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD_MILLIS;

	private static final class AggregationContainer {
		private final Aggregation aggregation;
		private final List<String> measures;
		private final AggregationPredicate predicate;

		private AggregationContainer(Aggregation aggregation, List<String> measures, AggregationPredicate predicate) {
			this.aggregation = aggregation;
			this.measures = measures;
			this.predicate = predicate;
		}

		@Override
		public String toString() {
			return aggregation.toString();
		}
	}

	// state
	private Map<String, AggregationContainer> aggregations = new LinkedHashMap<>();
	private int lastRevisionId;
	private long lastReloadTimestamp;

	private CubeClassLoaderCache classLoaderCache;

	// JMX
	private final ValueStats queryTimes = ValueStats.create().withSmoothingWindow(10 * 60);
	private long queryErrors;
	private Exception queryLastError;
	private long consolidationStarted;
	private long consolidationLastTimeMillis;
	private int consolidations;
	private Exception consolidationLastError;

	private Cube(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	             CubeMetadataStorage cubeMetadataStorage, AggregationChunkStorage aggregationChunkStorage) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.classLoader = classLoader;
		this.cubeMetadataStorage = cubeMetadataStorage;
		this.aggregationChunkStorage = aggregationChunkStorage;
	}

	public static Cube create(Eventloop eventloop, ExecutorService executorService, DefiningClassLoader classLoader,
	                          CubeMetadataStorage cubeMetadataStorage, AggregationChunkStorage aggregationChunkStorage) {
		return new Cube(eventloop, executorService, classLoader, cubeMetadataStorage, aggregationChunkStorage);
	}

	@VisibleForTesting
	static Cube createUninitialized() {
		return new Cube(null, null, null, null, null);
	}

	public Cube withAttribute(String attribute, AttributeResolver resolver) {
		checkArgument(!this.attributes.containsKey(attribute), "Attribute %s has already been defined", attribute);
		int pos = attribute.indexOf('.');
		if (pos == -1)
			throw new IllegalArgumentException();
		String dimension = attribute.substring(0, pos);
		String attributeName = attribute.substring(pos + 1);
		checkArgument(resolver.getAttributeTypes().containsKey(attributeName), "Resolver does not support %s", attribute);
		checkArgument(!isWrapperType(resolver.getAttributeTypes().get(attributeName)), "Unsupported attribute type for %s", attribute);
		List<String> dimensions = buildDrillDownChain(dimension);
		checkArgument(dimensions.size() == resolver.getKeyTypes().length, "Parent dimensions: %s, key types: %s", dimensions, newArrayList(resolver.getKeyTypes()));
		for (int i = 0; i < dimensions.size(); i++) {
			String d = dimensions.get(i);
			checkArgument(((Class<?>) dimensionTypes.get(d).getInternalDataType()).equals(resolver.getKeyTypes()[i]), "Dimension type mismatch for %s", d);
		}
		AttributeResolverContainer resolverContainer = null;
		for (AttributeResolverContainer r : attributeResolvers) {
			if (r.resolver == resolver) {
				resolverContainer = r;
				break;
			}
		}
		if (resolverContainer == null) {
			resolverContainer = new AttributeResolverContainer(dimensions, resolver);
			attributeResolvers.add(resolverContainer);
		}
		resolverContainer.attributes.add(attribute);
		attributes.put(attribute, resolverContainer);
		attributeTypes.put(attribute, resolver.getAttributeTypes().get(attributeName));
		return this;
	}

	public Cube withAttributes(Map<String, AttributeResolver> attributes) {
		Cube cube = this;
		for (String attribute : attributes.keySet()) {
			AttributeResolver resolver = attributes.get(attribute);
			cube = cube.withAttribute(attribute, resolver);
		}
		return cube;
	}

	public Cube withClassLoaderCache(CubeClassLoaderCache classLoaderCache) {
		this.classLoaderCache = classLoaderCache;
		return this;
	}

	public Cube withDimension(String dimensionId, FieldType type) {
		checkState(aggregations.isEmpty());
		dimensionTypes.put(dimensionId, type);
		fieldTypes.put(dimensionId, type);
		return this;
	}

	public Cube withDimensions(Map<String, FieldType> dimensions) {
		Cube self = this;
		for (String dimension : dimensions.keySet()) {
			self = self.withDimension(dimension, dimensions.get(dimension));
		}
		return self;
	}

	public Cube withMeasure(String measureId, Measure measure) {
		checkState(aggregations.isEmpty());
		measures.put(measureId, measure);
		fieldTypes.put(measureId, measure.getFieldType());
		return this;
	}

	public Cube withMeasures(Map<String, Measure> measures) {
		Cube self = this;
		for (String measure : measures.keySet()) {
			self = self.withMeasure(measure, measures.get(measure));
		}
		return self;
	}

	public Cube withComputedMeasure(String measureId, ComputedMeasure computedMeasure) {
		this.computedMeasures.put(measureId, computedMeasure);
		return this;
	}

	public Cube withComputedMeasures(Map<String, ComputedMeasure> computedMeasures) {
		this.computedMeasures.putAll(computedMeasures);
		return this;
	}

	public Cube withRelation(String child, String parent) {
		this.childParentRelationships.put(child, parent);
		parentChildRelationships.put(parent, child);
		return this;
	}

	public Cube withRelations(Map<String, String> childParentRelationships) {
		this.childParentRelationships.putAll(childParentRelationships);
		for (Map.Entry<String, String> parentChildEntry : childParentRelationships.entrySet()) {
			String parent = parentChildEntry.getKey();
			String child = parentChildEntry.getValue();
			parentChildRelationships.put(child, parent);
		}
		return this;
	}

	public static final class AggregationConfig {
		private final String id;
		private List<String> dimensions = new ArrayList<>();
		private List<String> measures = new ArrayList<>();
		private AggregationPredicate predicate = AggregationPredicates.alwaysTrue();
		private List<String> partitioningKey = new ArrayList<>();
		private int chunkSize;
		private int sorterItemsInMemory;
		private int sorterBlockSize;
		private int maxChunksToConsolidate;

		public AggregationConfig(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}

		public static AggregationConfig id(String id) {
			return new AggregationConfig(id);
		}

		public AggregationConfig withDimensions(Collection<String> dimensions) {
			this.dimensions.addAll(dimensions);
			return this;
		}

		public AggregationConfig withDimensions(String... dimensions) {
			return withDimensions(asList(dimensions));
		}

		public AggregationConfig withMeasures(Collection<String> measures) {
			this.measures.addAll(measures);
			return this;
		}

		public AggregationConfig withMeasures(String... measures) {
			return withMeasures(asList(measures));
		}

		public AggregationConfig withPredicate(AggregationPredicate predicate) {
			this.predicate = predicate;
			return this;
		}

		public AggregationConfig withPartitioningKey(List<String> partitioningKey) {
			this.partitioningKey.addAll(partitioningKey);
			return this;
		}

		public AggregationConfig withPartitioningKey(String... partitioningKey) {
			this.partitioningKey.addAll(asList(partitioningKey));
			return this;
		}

		public AggregationConfig withChunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
			return this;
		}

		public AggregationConfig withSorterItemsInMemory(int sorterItemsInMemory) {
			this.sorterItemsInMemory = sorterItemsInMemory;
			return this;
		}

		public AggregationConfig withSorterBlockSize(int sorterBlockSize) {
			this.sorterBlockSize = sorterBlockSize;
			return this;
		}

		public AggregationConfig withMaxChunksToConsolidate(int maxChunksToConsolidate) {
			this.maxChunksToConsolidate = maxChunksToConsolidate;
			return this;
		}
	}

	public Cube withAggregation(AggregationConfig aggregationConfig) {
		addAggregation(aggregationConfig);
		return this;
	}

	private Cube addAggregation(final AggregationConfig config) {
		checkArgument(!aggregations.containsKey(config.id), "Aggregation '%s' is already defined", config.id);

		AggregationMetadataStorage aggregationMetadataStorage = new AggregationMetadataStorage() {
			@Override
			public void createChunkId(ResultCallback<Long> callback) {
				cubeMetadataStorage.createChunkId(Cube.this, config.id, callback);
			}

			@Override
			public void loadChunks(final int lastRevisionId, final ResultCallback<LoadedChunks> callback) {
				if (loadChunksCallback != null) {
					loadChunksCallback.addListener(callback);
					return;
				}
				loadChunksCallback = ListenableResultCallback.create();
				loadChunksCallback.addListener(callback);

				cubeMetadataStorage.loadChunks(Cube.this, lastRevisionId, newHashSet(config.id), new ResultCallback<CubeLoadedChunks>() {
					@Override
					public void onResult(CubeLoadedChunks result) {
						loadChunksIntoAggregations(result, true);
						logger.info("Loading chunks for cube completed");
						ListenableResultCallback<LoadedChunks> callback = loadChunksCallback;
						loadChunksCallback = null;
						callback.setResult(new LoadedChunks(lastRevisionId, EMPTY_LIST, EMPTY_LIST));
					}

					@Override
					protected void onException(Exception e) {
						ListenableResultCallback<LoadedChunks> callback = loadChunksCallback;
						loadChunksCallback = null;
						callback.setException(e);
					}
				});
			}

			@Override
			public void startConsolidation(List<AggregationChunk> chunksToConsolidate, CompletionCallback callback) {
				cubeMetadataStorage.startConsolidation(Cube.this, config.id, chunksToConsolidate, callback);
			}

			@Override
			public void saveConsolidatedChunks(List<AggregationChunk> originalChunks, List<AggregationChunk.NewChunk> consolidatedChunks, CompletionCallback callback) {
				cubeMetadataStorage.saveConsolidatedChunks(Cube.this, config.id, originalChunks, consolidatedChunks, callback);
			}
		};

		Aggregation aggregation = Aggregation.create(eventloop, executorService, classLoader, aggregationMetadataStorage, aggregationChunkStorage)
				.withKeys(toMap(config.dimensions, forMap(this.dimensionTypes)))
				.withMeasures(projectMeasures(Cube.this.measures, config.measures))
				.withIgnoredMeasures(filterKeys(measuresAsFields(Cube.this.measures), not(in(config.measures))))
				.withPartitioningKey(config.partitioningKey)
				.withChunkSize(config.chunkSize != 0 ? config.chunkSize : aggregationsChunkSize)
				.withSorterItemsInMemory(config.sorterItemsInMemory != 0 ? config.sorterItemsInMemory : aggregationsSorterItemsInMemory)
				.withSorterBlockSize(config.sorterBlockSize != 0 ? config.sorterBlockSize : aggregationsSorterBlockSize)
				.withMaxChunksToConsolidate(config.maxChunksToConsolidate != 0 ? config.maxChunksToConsolidate : aggregationsMaxChunksToConsolidate)
				.withIgnoreChunkReadingExceptions(aggregationsIgnoreChunkReadingExceptions);

		aggregations.put(config.id, new AggregationContainer(aggregation, config.measures, config.predicate));
		logger.info("Added aggregation {} for id '{}'", aggregation, config.id);
		return this;
	}

	public Cube withAggregations(Collection<AggregationConfig> aggregations) {
		for (AggregationConfig aggregation : aggregations) {
			addAggregation(aggregation);
		}
		return this;
	}

	public Class<?> getAttributeInternalType(String attribute) {
		if (dimensionTypes.containsKey(attribute))
			return dimensionTypes.get(attribute).getInternalDataType();
		if (attributeTypes.containsKey(attribute))
			return attributeTypes.get(attribute);
		return null;
	}

	public Class<?> getMeasureInternalType(String field) {
		if (measures.containsKey(field))
			return measures.get(field).getFieldType().getInternalDataType();
		if (computedMeasures.containsKey(field))
			return computedMeasures.get(field).getType(measures);
		return null;
	}

	public Type getAttributeType(String attribute) {
		if (dimensionTypes.containsKey(attribute))
			return dimensionTypes.get(attribute).getDataType();
		if (attributeTypes.containsKey(attribute))
			return attributeTypes.get(attribute);
		return null;
	}

	public Type getMeasureType(String field) {
		if (measures.containsKey(field))
			return measures.get(field).getFieldType().getDataType();
		if (computedMeasures.containsKey(field))
			return computedMeasures.get(field).getType(measures);
		return null;
	}

	@Override
	public Map<String, Type> getAttributeTypes() {
		Map<String, Type> result = newLinkedHashMap();
		for (String dimension : dimensionTypes.keySet()) {
			result.put(dimension, dimensionTypes.get(dimension).getDataType());
		}
		for (String attribute : attributeTypes.keySet()) {
			result.put(attribute, attributeTypes.get(attribute));
		}
		return result;
	}

	@Override
	public Map<String, Type> getMeasureTypes() {
		Map<String, Type> result = newLinkedHashMap();
		for (String measure : measures.keySet()) {
			result.put(measure, measures.get(measure).getFieldType().getDataType());
		}
		for (String measure : computedMeasures.keySet()) {
			result.put(measure, computedMeasures.get(measure).getType(measures));
		}
		return result;
	}

	public Aggregation getAggregation(String aggregationId) {
		return aggregations.get(aggregationId).aggregation;
	}

	public void incrementLastRevisionId() {
		++lastRevisionId;
	}

	public int getLastRevisionId() {
		return lastRevisionId;
	}

	public <T> StreamConsumer<T> consumer(Class<T> inputClass, ResultCallback<Multimap<String, AggregationChunk.NewChunk>> callback) {
		return consumer(inputClass, AggregationPredicates.alwaysTrue(), callback);
	}

	public <T> StreamConsumer<T> consumer(Class<T> inputClass, AggregationPredicate predicate, ResultCallback<Multimap<String, AggregationChunk.NewChunk>> callback) {
		return consumer(inputClass, scanKeyFields(inputClass), scanMeasureFields(inputClass), predicate, callback);
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this cube.
	 * The returned {@link StreamConsumer} writes to {@link Aggregation}'s chosen using the specified dimensions, measures and input class.
	 *
	 * @param inputClass class of input records
	 * @param callback   callback which is called when records are committed
	 * @param <T>        data records type
	 * @return consumer for streaming data to cube
	 */
	public <T> StreamConsumer<T> consumer(Class<T> inputClass, Map<String, String> dimensionFields, Map<String, String> measureFields,
	                                      final AggregationPredicate predicate,
	                                      final ResultCallback<Multimap<String, AggregationChunk.NewChunk>> callback) {
		logger.info("Started consuming data. Dimensions: {}. Measures: {}", dimensionFields.keySet(), measureFields.keySet());

		final StreamSplitter<T> streamSplitter = StreamSplitter.create(eventloop);
		final AsyncResultsTrackerMultimap<String, AggregationChunk.NewChunk> tracker = ofMultimap(callback);

		for (final Map.Entry<String, AggregationContainer> entry : aggregations.entrySet()) {
			final String aggregationId = entry.getKey();
			AggregationContainer container = entry.getValue();
			final Aggregation aggregation = container.aggregation;
			if (!all(aggregation.getKeys(), in(dimensionFields.keySet())))
				continue;
			if (AggregationPredicates.and(container.predicate, predicate).simplify() == alwaysFalse())
				continue;

			Map<String, String> aggregationKeyFields = filterKeys(dimensionFields, in(aggregation.getKeys()));
			Map<String, String> aggregationMeasureFields = filterKeys(measureFields, in(container.measures));
			if (aggregationMeasureFields.isEmpty())
				continue;

			tracker.startOperation();
			StreamConsumer<T> groupReducer = aggregation.consumer(inputClass,
					aggregationKeyFields, aggregationMeasureFields,
					new ResultCallback<List<AggregationChunk.NewChunk>>() {
						@Override
						public void onResult(List<AggregationChunk.NewChunk> chunks) {
							tracker.completeWithResults(aggregationId, chunks);
						}

						@Override
						public void onException(Exception e) {
							tracker.completeWithException(e);
						}
					});

			streamSplitter.newOutput().streamTo(groupReducer);
		}
		tracker.shutDown();

		return streamSplitter.getInput();
	}

	public Set<String> getCompatibleMeasures(Collection<String> dimensions, AggregationPredicate where) {
		Set<String> result = newLinkedHashSet();
		List<String> allDimensions = newArrayList(concat(dimensions, where.getDimensions()));

		for (AggregationContainer aggregationContainer : aggregations.values()) {
			if (!all(allDimensions, in(aggregationContainer.aggregation.getKeys())))
				continue;
			result.addAll(aggregationContainer.measures);
		}

		for (String computedMeasure : computedMeasures.keySet()) {
			if (result.containsAll(computedMeasures.get(computedMeasure).getMeasureDependencies())) {
				result.add(computedMeasure);
			}
		}
		return result;
	}

	/**
	 * Returns a {@link StreamProducer} of the records retrieved from cube for the specified query.
	 *
	 * @param <T>         type of output objects
	 * @param resultClass class of output records
	 * @return producer that streams query results
	 */
	public <T> StreamProducer<T> queryRawStream(List<String> dimensions, List<String> storedMeasures, AggregationPredicate where,
	                                            Class<T> resultClass) throws QueryException {
		return queryRawStream(dimensions, storedMeasures, where, resultClass, classLoader);
	}

	public <T> StreamProducer<T> queryRawStream(List<String> dimensions, List<String> storedMeasures, AggregationPredicate where,
	                                            Class<T> resultClass, DefiningClassLoader queryClassLoader) throws QueryException {
		where = where.simplify();
		storedMeasures = newArrayList(storedMeasures);
		List<String> allDimensions = newArrayList(concat(dimensions, where.getDimensions()));

		final Map<AggregationContainer, Double> scores = new HashMap<>();
		List<AggregationContainer> compatibleAggregations = new ArrayList<>();
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			if (!all(allDimensions, in(aggregationContainer.aggregation.getKeys())))
				continue;
			List<String> compatibleMeasures = newArrayList(filter(storedMeasures, in(aggregationContainer.measures)));
			if (compatibleMeasures.isEmpty())
				continue;

			compatibleAggregations.add(aggregationContainer);
			AggregationQuery aggregationQuery = AggregationQuery.create(dimensions, compatibleMeasures, where);
			double score = aggregationContainer.aggregation.estimateCost(aggregationQuery);
			scores.put(aggregationContainer, score);
		}

		Collections.sort(compatibleAggregations, new Comparator<AggregationContainer>() {
			@Override
			public int compare(AggregationContainer aggregation1, AggregationContainer aggregation2) {
				return Double.compare(scores.get(aggregation1), scores.get(aggregation2));
			}
		});

		Class resultKeyClass = createKeyClass(projectKeys(dimensionTypes, dimensions), queryClassLoader);

		CubeQueryPlan queryPlan = CubeQueryPlan.create();

		StreamReducer<Comparable, T, Object> streamReducer = StreamReducer.create(eventloop, Ordering.natural());
		StreamProducer<T> queryResultProducer = streamReducer.getOutput();

		for (AggregationContainer aggregationContainer : compatibleAggregations) {
			List<String> compatibleMeasures = newArrayList(filter(storedMeasures, in(aggregationContainer.measures)));
			if (compatibleMeasures.isEmpty())
				continue;
			storedMeasures.removeAll(compatibleMeasures);

			queryPlan.addAggregationMeasures(aggregationContainer.aggregation, compatibleMeasures);

			Class aggregationClass = createRecordClass(projectKeys(dimensionTypes, dimensions),
					measuresAsFields(projectMeasures(measures, compatibleMeasures)), queryClassLoader);

			AggregationQuery aggregationQuery = AggregationQuery.create(dimensions, compatibleMeasures, where);
			StreamProducer aggregationProducer = aggregationContainer.aggregation.query(aggregationQuery, aggregationClass, queryClassLoader);

			if (storedMeasures.isEmpty() && streamReducer.getInputs().isEmpty()) {
				/*
				If query is fulfilled from the single aggregation,
				just use mapper instead of reducer to copy requested fields.
				 */
				StreamMap.MapperProjection mapper = AggregationUtils.createMapper(aggregationClass, resultClass, dimensions,
						compatibleMeasures, queryClassLoader);
				StreamMap streamMap = StreamMap.create(eventloop, mapper);
				aggregationProducer.streamTo(streamMap.getInput());
				queryResultProducer = streamMap.getOutput();
				queryPlan.setOptimizedAwayReducer(true);
				break;
			}

			Function keyFunction = AggregationUtils.createKeyFunction(aggregationClass, resultKeyClass, dimensions, queryClassLoader);

			StreamReducers.Reducer reducer = aggregationContainer.aggregation.aggregationReducer(aggregationClass, resultClass,
					dimensions, compatibleMeasures, queryClassLoader);

			StreamConsumer streamReducerInput = streamReducer.newInput(keyFunction, reducer);

			aggregationProducer.streamTo(streamReducerInput);
		}

		if (!storedMeasures.isEmpty()) {
			logger.info("Could not build query plan for {}. Incomplete plan: {}", storedMeasures, queryPlan);
		} else {
			logger.info("Picked following aggregations ({}) for {}", queryPlan.getNumberOfAggregations(), queryPlan);
		}

		return queryResultProducer;
	}

	public boolean containsExcessiveNumberOfOverlappingChunks() {
		boolean excessive = false;

		for (AggregationContainer aggregationContainer : aggregations.values()) {
			int numberOfOverlappingChunks = aggregationContainer.aggregation.getNumberOfOverlappingChunks();
			if (numberOfOverlappingChunks > maxOverlappingChunksToProcessLogs) {
				logger.info("Aggregation {} contains {} overlapping chunks", aggregationContainer.aggregation, numberOfOverlappingChunks);
				excessive = true;
			}
		}

		return excessive;
	}

	public void loadChunks(final CompletionCallback callback) {
		final boolean incremental = eventloop.currentTimeMillis() - lastReloadTimestamp <= maxIncrementalReloadPeriodMillis;
		logger.info("Loading chunks for cube (incremental={})", incremental);
		int revisionId = incremental ? lastRevisionId : 0;

		cubeMetadataStorage.loadChunks(this, revisionId, aggregations.keySet(), new ResultCallback<CubeLoadedChunks>() {
			@Override
			public void onResult(CubeLoadedChunks result) {
				loadChunksIntoAggregations(result, incremental);
				logger.info("Loading chunks for cube completed");
				callback.setComplete();
			}

			@Override
			public void onException(Exception e) {
				logger.error("Loading chunks for cube failed", e);
				callback.setException(e);
			}
		});
	}

	private void loadChunksIntoAggregations(CubeLoadedChunks result, boolean incremental) {
		this.lastRevisionId = result.lastRevisionId;
		this.lastReloadTimestamp = eventloop.currentTimeMillis();

		for (Map.Entry<String, AggregationContainer> entry : aggregations.entrySet()) {
			String aggregationId = entry.getKey();
			List<Long> consolidatedChunkIds = result.consolidatedChunkIds.get(aggregationId);
			List<AggregationChunk> newChunks = result.newChunks.get(aggregationId);
			LoadedChunks loadedChunks = new LoadedChunks(result.lastRevisionId,
					consolidatedChunkIds == null ? Collections.<Long>emptyList() : consolidatedChunkIds,
					newChunks == null ? Collections.<AggregationChunk>emptyList() : newChunks);
			Aggregation aggregation = entry.getValue().aggregation;
			aggregation.loadChunks(loadedChunks, incremental);
		}
	}

	public void consolidate(final ResultCallback<Boolean> callback) {
		if (consolidationStarted != 0) {
			logger.warn("Consolidation has already been started {} seconds ago", (eventloop.currentTimeMillis() - consolidationStarted) / 1000);
			callback.setResult(false);
			return;
		}
		consolidationStarted = eventloop.currentTimeMillis();

		logger.info("Launching consolidation");
		consolidate(false, new ArrayList<>(this.aggregations.values()).iterator(), new ResultCallback<Boolean>() {
			@Override
			protected void onResult(Boolean result) {
				if (result) {
					consolidationLastTimeMillis = eventloop.currentTimeMillis() - consolidationStarted;
					consolidations++;
				}
				consolidationStarted = 0;
				callback.setResult(result);
			}

			@Override
			protected void onException(Exception e) {
				consolidationStarted = 0;
				consolidationLastError = e;
				callback.setException(e);
			}
		});
	}

	private void consolidate(final boolean found, final Iterator<AggregationContainer> iterator, final ResultCallback<Boolean> callback) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				if (iterator.hasNext()) {
					final Aggregation aggregation = iterator.next().aggregation;
					aggregation.consolidateHotSegment(new ResultCallback<Boolean>() {
						@Override
						public void onResult(final Boolean hotSegmentConsolidated) {
							aggregation.consolidateMinKey(new ResultCallback<Boolean>() {
								@Override
								public void onResult(Boolean minKeyConsolidated) {
									consolidate(hotSegmentConsolidated || minKeyConsolidated || found, iterator, callback);
								}

								@Override
								public void onException(Exception e) {
									logger.error("Min key consolidation in aggregation '{}' failed", aggregation, e);
									consolidate(found, iterator, callback);
								}
							});
						}

						@Override
						public void onException(Exception e) {
							logger.error("Consolidating hot segment in aggregation '{}' failed", aggregation, e);
							consolidate(found, iterator, callback);
						}
					});
				} else {
					callback.setResult(found);
				}
			}
		});
	}

	private static class DrillDownsAndChains {
		public Set<QueryResult.Drilldown> drilldowns;
		public Set<List<String>> chains;

		public DrillDownsAndChains(Set<QueryResult.Drilldown> drilldowns, Set<List<String>> chains) {
			this.drilldowns = drilldowns;
			this.chains = chains;
		}
	}

	private DrillDownsAndChains getDrillDownsAndChains(Set<String> dimensions, Set<String> measures, AggregationPredicate where) {
		Set<QueryResult.Drilldown> drilldowns = newHashSet();
		Set<List<String>> chains = newHashSet();

		List<String> queryDimensions = newArrayList(concat(dimensions, where.getFullySpecifiedDimensions().keySet()));

		for (AggregationContainer aggregationContainer : aggregations.values()) {
			Set<String> aggregationMeasures = newHashSet();
			aggregationMeasures.addAll(aggregationContainer.measures);

			if (!all(queryDimensions, in(aggregationContainer.aggregation.getKeys())) || !any(measures, in(aggregationMeasures)) ||
					AggregationPredicates.and(aggregationContainer.predicate, where).simplify() == alwaysFalse())
				continue;

			Set<String> availableMeasures = newHashSet();
			intersection(aggregationMeasures, measures).copyInto(availableMeasures);

			Iterable<String> filteredDimensions = filter(aggregationContainer.aggregation.getKeys(), not(in(queryDimensions)));
			Set<List<String>> filteredChains = buildDrillDownChains(newHashSet(queryDimensions), filteredDimensions);
			Set<List<String>> allChains = buildDrillDownChains(Sets.<String>newHashSet(), aggregationContainer.aggregation.getKeys());

			for (List<String> drillDownChain : filteredChains) {
				drilldowns.add(QueryResult.Drilldown.create(drillDownChain, availableMeasures));
			}

			chains.addAll(allChains);
		}

		Set<List<String>> longestChains = buildLongestChains(chains);

		return new DrillDownsAndChains(drilldowns, longestChains);
	}

	private List<String> buildDrillDownChain(Set<String> usedDimensions, String dimension) {
		LinkedList<String> drillDown = new LinkedList<>();
		drillDown.add(dimension);
		String child = dimension;
		String parent;
		while ((parent = childParentRelationships.get(child)) != null && !usedDimensions.contains(parent)) {
			drillDown.addFirst(parent);
			child = parent;
		}
		return drillDown;
	}

	@VisibleForTesting
	Set<List<String>> buildDrillDownChains(Set<String> usedDimensions, Iterable<String> availableDimensions) {
		Set<List<String>> drillDowns = newHashSet();
		for (String dimension : availableDimensions) {
			List<String> drillDown = buildDrillDownChain(usedDimensions, dimension);
			drillDowns.add(drillDown);
		}
		return drillDowns;
	}

	private Set<List<String>> buildLongestChains(Set<List<String>> allChains) {
		List<List<String>> chainsList = newArrayList(allChains);
		Set<List<String>> longestChains = newHashSet();

		outer:
		for (int i = 0; i < chainsList.size(); ++i) {
			List<String> chain1 = chainsList.get(i);

			for (int j = 0; j < chainsList.size(); ++j) {
				if (i == j) continue;

				List<String> chain2 = chainsList.get(j);
				if (startsWith(chain2, chain1))
					continue outer;
			}

			if (chain1.size() > 1)
				longestChains.add(chain1);
		}

		return longestChains;
	}

	private List<String> buildDrillDownChain(String dimension) {
		return buildDrillDownChain(Sets.<String>newHashSet(), dimension);
	}

	public Map<String, List<AggregationMetadata.ConsolidationDebugInfo>> getConsolidationDebugInfo() {
		Map<String, List<AggregationMetadata.ConsolidationDebugInfo>> m = newHashMap();

		for (Map.Entry<String, AggregationContainer> aggregationEntry : aggregations.entrySet()) {
			m.put(aggregationEntry.getKey(), aggregationEntry.getValue().aggregation.getConsolidationDebugInfo());
		}

		return m;
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	// visible for testing
	public void setLastReloadTimestamp(long lastReloadTimestamp) {
		this.lastReloadTimestamp = lastReloadTimestamp;
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.setLastReloadTimestamp(lastReloadTimestamp);
		}
	}

	// region temp query() method
	@Override
	public void query(CubeQuery cubeQuery, final ResultCallback<QueryResult> resultCallback) throws QueryException {
		DefiningClassLoader queryClassLoader = getQueryClassLoader(new CubeClassLoaderCache.Key(
				newLinkedHashSet(cubeQuery.getAttributes()),
				newLinkedHashSet(cubeQuery.getMeasures()),
				cubeQuery.getWhere().getDimensions()));
		final long queryStarted = eventloop.currentTimeMillis();
		new RequestContext().execute(queryClassLoader, cubeQuery, new ResultCallback<QueryResult>() {
			@Override
			protected void onResult(QueryResult result) {
				queryTimes.recordValue((int) (eventloop.currentTimeMillis() - queryStarted));
				resultCallback.setResult(result);
			}

			@Override
			protected void onException(Exception e) {
				queryErrors++;
				queryLastError = e;
				resultCallback.setException(e);
			}
		});
	}
	// endregion

	private DefiningClassLoader getQueryClassLoader(CubeClassLoaderCache.Key key) {
		if (classLoaderCache == null)
			return this.classLoader;
		return classLoaderCache.getOrCreate(key);
	}

	private class RequestContext {
		DefiningClassLoader queryClassLoader;
		CubeQuery query;

		AggregationPredicate queryPredicate;
		AggregationPredicate queryHaving;
		Set<String> queryDimensions = newLinkedHashSet();
		Set<String> queryMeasures = newLinkedHashSet();
		Map<String, Object> fullySpecifiedDimensions;

		Set<String> resultAttributes = newLinkedHashSet();

		DrillDownsAndChains drillDownsAndChains;

		Set<String> resultMeasures = newLinkedHashSet();
		Set<String> resultStoredMeasures = newLinkedHashSet();
		Set<String> resultComputedMeasures = newLinkedHashSet();

		Class<?> resultClass;
		Predicate<?> havingPredicate;

		List<String> resultOrderings = newArrayList();
		Comparator<?> comparator;

		MeasuresFunction measuresFunction;
		TotalsFunction totalsFunction;

		RecordScheme recordScheme;
		RecordFunction recordFunction;

		void execute(DefiningClassLoader queryClassLoader, CubeQuery query, final ResultCallback<QueryResult> resultCallback) throws QueryException {
			this.queryClassLoader = queryClassLoader;
			this.query = query;

			queryPredicate = query.getWhere().simplify();
			queryHaving = query.getHaving().simplify();
			fullySpecifiedDimensions = queryPredicate.getFullySpecifiedDimensions();

			prepareDimensions();
			prepareMeasures();
			drillDownsAndChains = getDrillDownsAndChains(queryDimensions, queryMeasures, queryPredicate);

			resultClass = createResultClass(resultAttributes, resultMeasures, Cube.this, queryClassLoader);
			measuresFunction = createMeasuresFunction();
			totalsFunction = createTotalsFunction();
			comparator = createComparator();
			havingPredicate = createHavingPredicate();
			recordScheme = createRecordScheme();
			recordFunction = createRecordFunction();

			StreamConsumers.ToList consumer = StreamConsumers.toList(eventloop);
			StreamProducer queryResultProducer = queryRawStream(newArrayList(queryDimensions), newArrayList(resultStoredMeasures), queryPredicate,
					resultClass, queryClassLoader);
			queryResultProducer.streamTo(consumer);

			consumer.setResultCallback(new ForwardingResultCallback<List<Object>>(resultCallback) {
				@Override
				protected void onResult(List<Object> results) {
					processResults(results, resultCallback);
				}
			});
		}

		void prepareDimensions() throws QueryException {
			for (String attribute : query.getAttributes()) {
				List<String> dimensions = newArrayList();
				if (dimensionTypes.containsKey(attribute)) {
					dimensions = buildDrillDownChain(attribute);
				} else if (attributes.containsKey(attribute)) {
					AttributeResolverContainer resolverContainer = attributes.get(attribute);
					for (String dimension : resolverContainer.dimensions) {
						dimensions.addAll(buildDrillDownChain(dimension));
					}
				} else
					throw new QueryException("Attribute not found: " + attribute);
				queryDimensions.addAll(dimensions);
				resultAttributes.addAll(dimensions);
				resultAttributes.add(attribute);
			}
		}

		void prepareMeasures() throws QueryException {
			queryMeasures.addAll(newArrayList(filter(query.getMeasures(), in(getCompatibleMeasures(queryDimensions, queryPredicate)))));
			for (String queryMeasure : queryMeasures) {
				if (measures.containsKey(queryMeasure)) {
					resultStoredMeasures.add(queryMeasure);
					resultMeasures.add(queryMeasure);
				} else if (computedMeasures.containsKey(queryMeasure)) {
					ComputedMeasure expression = computedMeasures.get(queryMeasure);
					Set<String> dependencies = expression.getMeasureDependencies();
					resultStoredMeasures.addAll(dependencies);
					resultComputedMeasures.add(queryMeasure);
					resultMeasures.addAll(dependencies);
					resultMeasures.add(queryMeasure);
				}
			}
		}

		RecordScheme createRecordScheme() {
			RecordScheme recordScheme = RecordScheme.create();
			for (String attribute : resultAttributes) {
				recordScheme = recordScheme.withField(attribute,
						getAttributeType(attribute));
			}
			for (String measure : queryMeasures) {
				recordScheme = recordScheme.withField(measure,
						getMeasureType(measure));
			}
			return recordScheme;
		}

		RecordFunction createRecordFunction() {
			ExpressionSequence copyAttributes = ExpressionSequence.create();
			ExpressionSequence copyMeasures = ExpressionSequence.create();

			for (String field : recordScheme.getFields()) {
				int fieldIndex = recordScheme.getFieldIndex(field);
				if (dimensionTypes.containsKey(field)) {
					copyAttributes.add(call(arg(1), "put", value(fieldIndex),
							cast(dimensionTypes.get(field).toValue(
									field(cast(arg(0), resultClass), field)), Object.class)));
				} else if (measures.containsKey(field)) {
					VarField fieldValue = field(cast(arg(0), resultClass), field);
					copyMeasures.add(call(arg(1), "put", value(fieldIndex),
							cast(measures.get(field).getFieldType().toValue(
									measures.get(field).valueOfAccumulator(fieldValue)), Object.class)));
				} else {
					copyMeasures.add(call(arg(1), "put", value(fieldIndex),
							cast(field(cast(arg(0), resultClass), field.replace('.', '$')), Object.class)));
				}
			}

			return ClassBuilder.create(queryClassLoader, RecordFunction.class)
					.withMethod("copyAttributes", copyAttributes)
					.withMethod("copyMeasures", copyMeasures)
					.buildClassAndCreateNewInstance();
		}

		MeasuresFunction createMeasuresFunction() {
			ClassBuilder<MeasuresFunction> builder = ClassBuilder.create(queryClassLoader, MeasuresFunction.class);
			List<Expression> computeSequence = new ArrayList<>();

			for (String computedMeasure : resultComputedMeasures) {
				builder = builder.withField(computedMeasure, computedMeasures.get(computedMeasure).getType(measures));
				Expression record = cast(arg(0), resultClass);
				computeSequence.add(set(field(record, computedMeasure),
						computedMeasures.get(computedMeasure).getExpression(record, measures)));
			}
			return builder.withMethod("computeMeasures", sequence(computeSequence))
					.buildClassAndCreateNewInstance();
		}

		private Predicate createHavingPredicate() {
			if (queryHaving == AggregationPredicates.alwaysTrue())
				return com.google.common.base.Predicates.alwaysTrue();
			if (queryHaving == AggregationPredicates.alwaysFalse())
				return com.google.common.base.Predicates.alwaysFalse();
			return ClassBuilder.create(classLoader, Predicate.class)
					.withMethod("apply", boolean.class, singletonList(Object.class),
							queryHaving.createPredicateDef(cast(arg(0), resultClass), fieldTypes))
					.buildClassAndCreateNewInstance();
		}

		@SuppressWarnings("unchecked")
		Comparator<Object> createComparator() {
			if (query.getOrderings().isEmpty())
				return Ordering.allEqual();

			ExpressionComparator comparator = ExpressionComparator.create();

			for (CubeQuery.Ordering ordering : query.getOrderings()) {
				String field = ordering.getField();

				if (fullySpecifiedDimensions.containsKey(field))
					continue;

				if (resultMeasures.contains(field) || resultAttributes.contains(field)) {
					String property = field.replace('.', '$');
					comparator = comparator.with(
							ordering.isAsc() ? leftField(resultClass, property) : rightField(resultClass, property),
							ordering.isAsc() ? rightField(resultClass, property) : leftField(resultClass, property),
							true);
					resultOrderings.add(field);
				}
			}

			return ClassBuilder.create(queryClassLoader, Comparator.class)
					.withMethod("compare", comparator)
					.buildClassAndCreateNewInstance();
		}

		@SuppressWarnings("unchecked")
		void processResults(final List<Object> results, final ResultCallback<QueryResult> callback) {
			final Object totals;
			try {
				totals = resultClass.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}

			if (results.isEmpty()) {
				totalsFunction.zero(totals);
			} else {
				Iterator<Object> iterator = results.iterator();
				Object first = iterator.next();
				measuresFunction.computeMeasures(first);
				totalsFunction.init(totals, first);
				while (iterator.hasNext()) {
					Object next = iterator.next();
					measuresFunction.computeMeasures(next);
					totalsFunction.accumulate(totals, next);
				}
			}
			totalsFunction.computeMeasures(totals);

			List<AsyncRunnable> tasks = new ArrayList<>();
			for (final AttributeResolverContainer resolverContainer : attributeResolvers) {
				List<String> attributes = new ArrayList<>(resolverContainer.attributes);
				attributes.retainAll(resultAttributes);
				if (!attributes.isEmpty()) {
					tasks.add(new AsyncRunnable() {
						@Override
						public void run(CompletionCallback callback) {
							resolveAttributes(results, resolverContainer.resolver,
									resolverContainer.dimensions, resolverContainer.attributes,
									(Class) resultClass, queryClassLoader, callback);
						}
					});
				}
			}

			final Map<String, Object> filterAttributes = newLinkedHashMap();
			for (final AttributeResolverContainer resolverContainer : attributeResolvers) {
				if (fullySpecifiedDimensions.keySet().containsAll(resolverContainer.dimensions)) {
					tasks.add(new AsyncRunnable() {
						@Override
						public void run(CompletionCallback callback) {
							resolveSpecifiedDimensions(resolverContainer, filterAttributes, callback);
						}
					});
				}
			}

			runInParallel(eventloop, tasks).run(new ForwardingCompletionCallback(callback) {
				@Override
				protected void onComplete() {
					processResults2(results, totals, filterAttributes, callback);
				}
			});
		}

		void processResults2(List<Object> results, Object totals, Map<String, Object> filterAttributes, final ResultCallback<QueryResult> callback) {
			results = newArrayList(Iterables.filter(results, (Predicate<Object>) havingPredicate));

			int totalCount = results.size();

			results = applyLimitAndOffset(results);

			List<Record> resultRecords = new ArrayList<>(results.size());
			for (Object result : results) {
				Record record = Record.create(recordScheme);
				recordFunction.copyAttributes(result, record);
				recordFunction.copyMeasures(result, record);
				resultRecords.add(record);
			}

			Record totalRecord = Record.create(recordScheme);
			recordFunction.copyMeasures(totals, totalRecord);

			QueryResult result = QueryResult.create(recordScheme,
					resultRecords, totalRecord, totalCount,
					newArrayList(resultAttributes), newArrayList(queryMeasures), resultOrderings,
					drillDownsAndChains.drilldowns, drillDownsAndChains.chains, filterAttributes);
			callback.setResult(result);
		}

		private void resolveSpecifiedDimensions(final AttributeResolverContainer resolverContainer, final Map<String, Object> result,
		                                        final CompletionCallback callback) {
			Object[] key = new Object[resolverContainer.dimensions.size()];
			for (int i = 0; i < resolverContainer.dimensions.size(); i++) {
				String dimension = resolverContainer.dimensions.get(i);
				key[i] = fullySpecifiedDimensions.get(dimension);
			}
			final Object[] attributesPlaceholder = new Object[1];

			resolverContainer.resolver.resolveAttributes(singletonList((Object) key),
					new AttributeResolver.KeyFunction() {
						@Override
						public Object[] extractKey(Object result) {
							return (Object[]) result;
						}
					},
					new AttributeResolver.AttributesFunction() {
						@Override
						public void applyAttributes(Object result, Object[] attributes) {
							attributesPlaceholder[0] = attributes;
						}
					},
					new ForwardingCompletionCallback(callback) {
						@Override
						protected void onComplete() {
							for (int i = 0; i < resolverContainer.attributes.size(); i++) {
								String attribute = resolverContainer.attributes.get(i);
								result.put(attribute, ((Object[]) attributesPlaceholder[0])[i]);
							}
							callback.setComplete();
						}
					});
		}

		List applyLimitAndOffset(List results) {
			Integer offset = query.getOffset();
			Integer limit = query.getLimit();
			int start;
			int end;

			if (offset == null) {
				start = 0;
				offset = 0;
			} else if (offset >= results.size()) {
				return newArrayList();
			} else {
				start = offset;
			}

			if (limit == null) {
				end = results.size();
				limit = results.size();
			} else if (start + limit > results.size()) {
				end = results.size();
			} else {
				end = start + limit;
			}

			if (comparator != null) {
				int upperBound = offset + limit > results.size() ? results.size() : offset + limit;
				return Ordering.from(comparator).leastOf(results, upperBound).subList(offset, upperBound);
			}

			return results.subList(start, end);
		}

		TotalsFunction createTotalsFunction() {
			ExpressionSequence zeroSequence = ExpressionSequence.create();
			ExpressionSequence initSequence = ExpressionSequence.create();
			ExpressionSequence accumulateSequence = ExpressionSequence.create();
			for (String field : resultStoredMeasures) {
				Measure measure = measures.get(field);
				zeroSequence.add(measure.zeroAccumulator(
						field(cast(arg(0), resultClass), field)));
				initSequence.add(measure.initAccumulatorWithAccumulator(
						field(cast(arg(0), resultClass), field),
						field(cast(arg(1), resultClass), field)));
				accumulateSequence.add(measure.reduce(
						field(cast(arg(0), resultClass), field),
						field(cast(arg(1), resultClass), field)));
			}

			ExpressionSequence computeSequence = ExpressionSequence.create();
			for (String computedMeasure : resultComputedMeasures) {
				Expression result = cast(arg(0), resultClass);
				computeSequence.add(set(field(result, computedMeasure),
						computedMeasures.get(computedMeasure).getExpression(result, measures)));
			}
			return ClassBuilder.create(queryClassLoader, TotalsFunction.class)
					.withMethod("zero", zeroSequence)
					.withMethod("init", initSequence)
					.withMethod("accumulate", accumulateSequence)
					.withMethod("computeMeasures", computeSequence)
					.buildClassAndCreateNewInstance();
		}

	}
	// endregion

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("aggregations", aggregations)
				.add("lastRevisionId", lastRevisionId)
				.toString();
	}

	// jmx
	@JmxAttribute
	public int getAggregationsChunkSize() {
		return aggregationsChunkSize;
	}

	@JmxAttribute
	public void setAggregationsChunkSize(int aggregationsChunkSize) {
		this.aggregationsChunkSize = aggregationsChunkSize;
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.setChunkSize(aggregationsChunkSize);
		}
	}

	public Cube withAggregationsChunkSize(int aggregationsChunkSize) {
		this.aggregationsChunkSize = aggregationsChunkSize;
		return this;
	}

	@JmxAttribute
	public int getAggregationsSorterItemsInMemory() {
		return aggregationsSorterItemsInMemory;
	}

	@JmxAttribute
	public void setAggregationsSorterItemsInMemory(int aggregationsSorterItemsInMemory) {
		this.aggregationsSorterItemsInMemory = aggregationsSorterItemsInMemory;
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.setSorterItemsInMemory(aggregationsSorterItemsInMemory);
		}
	}

	public Cube withAggregationsSorterItemsInMemory(int aggregationsSorterItemsInMemory) {
		this.aggregationsSorterItemsInMemory = aggregationsSorterItemsInMemory;
		return this;
	}

	@JmxAttribute
	public int getAggregationsSorterBlockSize() {
		return aggregationsSorterBlockSize;
	}

	@JmxAttribute
	public void setAggregationsSorterBlockSize(int aggregationsSorterBlockSize) {
		this.aggregationsSorterBlockSize = aggregationsSorterBlockSize;
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.setSorterBlockSize(aggregationsSorterBlockSize);
		}
	}

	public Cube withAggregationsSorterBlockSize(int aggregationsSorterBlockSize) {
		this.aggregationsSorterBlockSize = aggregationsSorterBlockSize;
		return this;
	}

	@JmxAttribute
	public int getAggregationsMaxChunksToConsolidate() {
		return aggregationsMaxChunksToConsolidate;
	}

	@JmxAttribute
	public void setAggregationsMaxChunksToConsolidate(int aggregationsMaxChunksToConsolidate) {
		this.aggregationsMaxChunksToConsolidate = aggregationsMaxChunksToConsolidate;
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.setMaxChunksToConsolidate(aggregationsMaxChunksToConsolidate);
		}
	}

	public Cube withAggregationsMaxChunksToConsolidate(int aggregationsMaxChunksToConsolidate) {
		this.aggregationsMaxChunksToConsolidate = aggregationsMaxChunksToConsolidate;
		return this;
	}

	@JmxAttribute
	public boolean getAggregationsIgnoreChunkReadingExceptions() {
		return aggregationsIgnoreChunkReadingExceptions;
	}

	@JmxAttribute
	public void setAggregationsIgnoreChunkReadingExceptions(boolean aggregationsIgnoreChunkReadingExceptions) {
		this.aggregationsIgnoreChunkReadingExceptions = aggregationsIgnoreChunkReadingExceptions;
		for (AggregationContainer aggregation : aggregations.values()) {
			aggregation.aggregation.setIgnoreChunkReadingExceptions(aggregationsIgnoreChunkReadingExceptions);
		}
	}

	public Cube withAggregationsIgnoreChunkReadingExceptions(boolean aggregationsIgnoreChunkReadingExceptions) {
		this.aggregationsIgnoreChunkReadingExceptions = aggregationsIgnoreChunkReadingExceptions;
		return this;
	}

	@JmxAttribute
	public int getMaxOverlappingChunksToProcessLogs() {
		return maxOverlappingChunksToProcessLogs;
	}

	@JmxAttribute
	public void setMaxOverlappingChunksToProcessLogs(int maxOverlappingChunksToProcessLogs) {
		this.maxOverlappingChunksToProcessLogs = maxOverlappingChunksToProcessLogs;
	}

	public Cube withMaxOverlappingChunksToProcessLogs(int maxOverlappingChunksToProcessLogs) {
		this.maxOverlappingChunksToProcessLogs = maxOverlappingChunksToProcessLogs;
		return this;
	}

	@JmxAttribute
	public int getMaxIncrementalReloadPeriodMillis() {
		return maxIncrementalReloadPeriodMillis;
	}

	@JmxAttribute
	public void setMaxIncrementalReloadPeriodMillis(int maxIncrementalReloadPeriodMillis) {
		this.maxIncrementalReloadPeriodMillis = maxIncrementalReloadPeriodMillis;
	}

	public Cube withMaxIncrementalReloadPeriodMillis(int maxIncrementalReloadPeriodMillis) {
		this.maxIncrementalReloadPeriodMillis = maxIncrementalReloadPeriodMillis;
		return this;
	}

	@JmxAttribute
	public int getActiveReducersBuffersSize() {
		int size = 0;
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			size += aggregationContainer.aggregation.getActiveReducersBuffersSize();
		}
		return size;
	}

	@JmxOperation
	public void flushActiveReducersBuffers() {
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.flushActiveReducersBuffers();
		}
	}

	@JmxAttribute
	public Integer getConsolidationSeconds() {
		return consolidationStarted == 0 ? null : (int) ((eventloop.currentTimeMillis() - consolidationStarted) / 1000);
	}

	@JmxAttribute
	public Integer getConsolidationLastTimeSeconds() {
		return consolidationLastTimeMillis == 0 ? null : (int) ((consolidationLastTimeMillis) / 1000);
	}

	@JmxAttribute
	public int getConsolidations() {
		return consolidations;
	}

	@JmxAttribute
	public Exception getConsolidationLastError() {
		return consolidationLastError;
	}

	@JmxAttribute
	public ValueStats getQueryTimes() {
		return queryTimes;
	}

	@JmxAttribute
	public long getQueryErrors() {
		return queryErrors;
	}

	@JmxAttribute
	public Exception getQueryLastError() {
		return queryLastError;
	}

	@JmxOperation
	public void consolidate() {
		consolidate(IgnoreResultCallback.<Boolean>create());
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

}