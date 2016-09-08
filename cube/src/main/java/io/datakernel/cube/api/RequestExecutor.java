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

package io.datakernel.cube.api;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.*;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.Cube;
import io.datakernel.cube.CubeQuery;
import io.datakernel.cube.DrillDown;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.cube.api.CommonUtils.*;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class RequestExecutor {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Cube cube;
	private final AggregationStructure structure;
	private final ReportingConfiguration reportingConfiguration;
	private final Eventloop eventloop;
	private final Resolver resolver;
	private final LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache;

	public RequestExecutor(Cube cube, AggregationStructure structure, ReportingConfiguration reportingConfiguration,
	                       Eventloop eventloop, Resolver resolver,
	                       LRUCache<ClassLoaderCacheKey, DefiningClassLoader> classLoaderCache) {
		this.cube = cube;
		this.structure = structure;
		this.reportingConfiguration = reportingConfiguration;
		this.eventloop = eventloop;
		this.resolver = resolver;
		this.classLoaderCache = classLoaderCache;
	}

	public void execute(ReportingQuery query, ResultCallback<QueryResult> resultCallback) {
		new Context().execute(query, resultCallback);
	}

	public interface StringMatcher {
		boolean matches(Object obj, String searchString);
	}

	class Context {
		DefiningClassLoader localClassLoader;

		Map<AttributeResolver, List<String>> resolverKeys = newLinkedHashMap();
		Map<String, Class<?>> attributeTypes = newLinkedHashMap();
		Map<String, Object> keyConstants = newHashMap();

		List<String> filterAttributes = newArrayList();
		Map<String, Class<?>> filterAttributeTypes = newLinkedHashMap();

		List<String> queryDimensions = newArrayList();
		Set<String> storedDimensions = newHashSet();
		List<String> cubeQueryDimensions;

		Set<DrillDown> drillDowns;
		Set<List<String>> chains;

		CubeQuery query = new CubeQuery();

		AggregationQuery.Predicates queryPredicates;
		Map<String, AggregationQuery.Predicate> predicates;
		List<AggregationQuery.Predicate> cubeQueryPredicates;

		List<String> queryMeasures;
		Set<String> queryStoredMeasures = newHashSet();
		Set<String> queryComputedMeasures = newHashSet();
		Set<String> storedMeasures = newHashSet();
		Set<String> computedMeasures = newHashSet();
		List<String> cubeQueryStoredMeasures;
		List<String> sortedComputedMeasures;

		Set<String> fields;
		Set<String> metadataFields;

		List<String> attributes = newArrayList();

		List<CubeQuery.Ordering> queryOrderings;
		List<CubeQuery.Ordering> orderings = newArrayList();
		List<String> appliedOrderings = newArrayList();
		boolean sortingRequired;

		Class<QueryResultPlaceholder> resultClass;
		Comparator<QueryResultPlaceholder> comparator;
		Integer limit;
		Integer offset;
		String searchString;

		void execute(ReportingQuery reportingQuery, final ResultCallback<QueryResult> resultCallback) {
			queryDimensions = reportingQuery.getDimensions();
			queryMeasures = reportingQuery.getMeasures();
			queryPredicates = reportingQuery.getFilters();
			predicates = transformPredicates(queryPredicates);
			attributes = asSorted(reportingQuery.getAttributes());
			queryOrderings = reportingQuery.getSort();
			limit = reportingQuery.getLimit();
			offset = reportingQuery.getOffset();
			searchString = reportingQuery.getSearchString();
			fields = reportingQuery.getFields();
			metadataFields = reportingQuery.getMetadataFields();

			processAttributes();
			processDimensions();
			processMeasures();
			buildDrillDowns();
			processComputedMeasures();
			processOrdering();

			cubeQueryDimensions = asSortedList(storedDimensions);
			cubeQueryStoredMeasures = asSortedList(storedMeasures);
			cubeQueryPredicates = asSortedList(predicates.values());
			sortedComputedMeasures = asSortedList(computedMeasures);

			query
					.dimensions(cubeQueryDimensions)
					.measures(cubeQueryStoredMeasures)
					.predicates(cubeQueryPredicates);

			localClassLoader = getLocalClassLoader(new ClassLoaderCacheKey(cubeQueryDimensions, cubeQueryPredicates,
					cubeQueryStoredMeasures, sortedComputedMeasures, attributes));
			resultClass = createResultClass();
			StreamConsumers.ToList<QueryResultPlaceholder> consumerStream = queryCube();
			comparator = sortingRequired ? generateComparator() : null;

			consumerStream.setResultCallback(new ResultCallback<List<QueryResultPlaceholder>>() {
				@Override
				public void onResult(List<QueryResultPlaceholder> results) {
					processResults(results, resultCallback);
				}

				@Override
				public void onException(Exception e) {
					logger.error("Executing query {} failed.", query, e);
					resultCallback.onException(e);
				}
			});
		}

		DefiningClassLoader getLocalClassLoader(ClassLoaderCacheKey key) {
			DefiningClassLoader classLoader = classLoaderCache.get(key);

			if (classLoader != null)
				return classLoader;

			DefiningClassLoader newClassLoader = new DefiningClassLoader(cube.getClassLoader());
			classLoaderCache.put(key, newClassLoader);
			return newClassLoader;
		}

		Map<String, AggregationQuery.Predicate> transformPredicates(AggregationQuery.Predicates predicates) {
			return predicates == null ? Maps.<String, AggregationQuery.Predicate>newHashMap() : predicates.asMap();
		}

		void processDimensions() {
			for (String dimension : queryDimensions) {
				if (!structure.containsKey(dimension))
					throw new QueryException("Cube does not contain dimension with name '" + dimension + "'");
			}

			Set<String> usedDimensions = newHashSet();

			for (AggregationQuery.Predicate predicate : predicates.values()) {
				usedDimensions.add(predicate.key);
			}

			for (String dimension : queryDimensions) {
				storedDimensions.addAll(cube.buildDrillDownChain(usedDimensions, dimension));
			}
		}

		void processAttributes() {
			for (String attribute : attributes) {
				AttributeResolver resolver = reportingConfiguration.getAttributeResolver(attribute);
				if (resolver == null)
					throw new QueryException("Cube does not contain resolver for '" + attribute + "'");

				List<String> keyComponents = reportingConfiguration.getKeyForResolver(resolver);

				boolean usingStoredDimension = false;
				for (String keyComponent : keyComponents) {
					if (predicates != null && predicates.get(keyComponent) instanceof AggregationQuery.PredicateEq) {
						if (usingStoredDimension)
							throw new QueryException("Incorrect filter: using 'equals' predicate when prefix of this " +
									"compound key is not fully defined");
						else
							keyConstants.put(keyComponent,
									((AggregationQuery.PredicateEq) predicates.get(keyComponent)).value);
					} else {
						storedDimensions.add(keyComponent);
						usingStoredDimension = true;
					}
				}

				resolverKeys.put(resolver, keyComponents);
				Class<?> attributeType = reportingConfiguration.getAttributeType(attribute);
				attributeTypes.put(attribute, attributeType);

				if (all(keyComponents, in(predicates.keySet()))) {
					filterAttributes.add(attribute);
					filterAttributeTypes.put(attribute, attributeType);
				}
			}
		}

		void processMeasures() {
			for (String queryMeasure : queryMeasures) {
				if (structure.containsField(queryMeasure)) {
					queryStoredMeasures.add(queryMeasure);
				} else if (reportingConfiguration.containsComputedMeasure(queryMeasure)) {
					ReportingDSLExpression expression = reportingConfiguration.getExpressionForMeasure(queryMeasure);
					queryStoredMeasures.addAll(expression.getMeasureDependencies());
					queryComputedMeasures.add(queryMeasure);
				} else {
					throw new QueryException("Cube does not contain measure with name '" + queryMeasure + "'");
				}
			}

			storedMeasures = cube.getAvailableMeasures(storedDimensions, queryPredicates, queryStoredMeasures);

			for (String computedMeasure : queryComputedMeasures) {
				if (all(reportingConfiguration.getComputedMeasureDependencies(computedMeasure), in(storedMeasures)))
					computedMeasures.add(computedMeasure);
			}
		}

		void buildDrillDowns() {
			boolean drillDownsRequested = nullOrContains(metadataFields, "drillDowns");
			boolean chainsRequested = nullOrContains(metadataFields, "chains");
			if (drillDownsRequested || chainsRequested) {
				Cube.DrillDownsAndChains drillDownsAndChains = cube.getDrillDownsAndChains(storedDimensions,
						queryPredicates, queryStoredMeasures);

				if (drillDownsRequested)
					drillDowns = drillDownsAndChains.drillDowns;

				if (chainsRequested)
					chains = drillDownsAndChains.chains;
			}
		}

		void processComputedMeasures() {
			for (String computedMeasure : queryComputedMeasures) {
				Set<String> dependencies = reportingConfiguration.getComputedMeasureDependencies(computedMeasure);

				if (all(dependencies, in(storedMeasures)))
					computedMeasures.add(computedMeasure);

				if (nullOrContains(metadataFields, "drillDowns")) {
					for (DrillDown drillDown : drillDowns) {
						if (all(dependencies, in(drillDown.getMeasures())))
							drillDown.getMeasures().add(computedMeasure);

						Iterables.removeIf(drillDown.getMeasures(), new Predicate<String>() {
							@Override
							public boolean apply(String measure) {
								return !queryMeasures.contains(measure);
							}
						});
					}
				}
			}
		}

		void processOrdering() {
			if (queryOrderings == null)
				return;

			for (CubeQuery.Ordering ordering : queryOrderings) {
				String orderingField = ordering.getPropertyName();

				if (predicates.get(orderingField) instanceof AggregationQuery.PredicateEq)
					continue;

				if (computedMeasures.contains(orderingField) || attributeTypes.containsKey(orderingField) ||
						storedDimensions.contains(orderingField) || storedMeasures.contains(orderingField)) {
					sortingRequired = true;
					orderings.add(ordering);
					appliedOrderings.add(orderingField);
				}
			}
		}

		Class<QueryResultPlaceholder> createResultClass() {
			AsmBuilder<QueryResultPlaceholder> builder = new AsmBuilder<>(localClassLoader, QueryResultPlaceholder.class);
			for (String dimension : cubeQueryDimensions) {
				KeyType keyType = structure.getKeyType(dimension);
				builder.withField(dimension, keyType.getDataType());
			}
			for (String measure : cubeQueryStoredMeasures) {
				FieldType fieldType = structure.getFieldType(measure);
				builder.withField(measure, fieldType.getDataType());
			}
			for (Map.Entry<String, Class<?>> nameEntry : attributeTypes.entrySet()) {
				builder.withField(nameEntry.getKey(), nameEntry.getValue());
			}
			ExpressionSequence computeSequence = sequence();
			for (String computedMeasure : sortedComputedMeasures) {
				builder.withField(computedMeasure, double.class);
				computeSequence.add(set(getter(self(), computedMeasure),
						reportingConfiguration.getComputedMeasureExpression(computedMeasure)));
			}
			builder.withMethod("computeMeasures", computeSequence);
			return builder.defineClass();
		}

		StreamConsumers.ToList<QueryResultPlaceholder> queryCube() {
			StreamConsumers.ToList<QueryResultPlaceholder> consumerStream = StreamConsumers.toList(eventloop);
			StreamProducer<QueryResultPlaceholder> queryResultProducer = cube.query(resultClass, query, localClassLoader);
			queryResultProducer.streamTo(consumerStream);
			return consumerStream;
		}

		@SuppressWarnings("unchecked")
		Comparator<QueryResultPlaceholder> generateComparator() {
			AsmBuilder<Comparator> builder = new AsmBuilder<>(localClassLoader, Comparator.class);
			ExpressionComparatorNullable comparator = comparatorNullable();

			for (CubeQuery.Ordering ordering : orderings) {
				if (ordering.isAsc())
					comparator.add(
							getter(cast(arg(0), resultClass), ordering.getPropertyName()),
							getter(cast(arg(1), resultClass), ordering.getPropertyName()));
				else
					comparator.add(
							getter(cast(arg(1), resultClass), ordering.getPropertyName()),
							getter(cast(arg(0), resultClass), ordering.getPropertyName()));
			}

			builder.withMethod("compare", comparator);

			return builder.newInstance();
		}

		@SuppressWarnings("unchecked")
		void processResults(List<QueryResultPlaceholder> results, ResultCallback<QueryResult> callback) {
			Class filterAttributesClass;
			Object filterAttributesPlaceholder = null;
			if (nullOrContains(metadataFields, "filterAttributes") && !filterAttributes.isEmpty()) {
				filterAttributesClass = createFilterAttributesClass();
				filterAttributesPlaceholder = instantiate(filterAttributesClass);
				resolver.resolve(singletonList(filterAttributesPlaceholder), filterAttributesClass, filterAttributeTypes,
						resolverKeys, keyConstants, localClassLoader);
			}

			computeMeasures(results);
			resolver.resolve((List) results, resultClass, attributeTypes, resolverKeys, keyConstants, localClassLoader);
			results = performSearch(results);
			TotalsPlaceholder totalsPlaceholder = computeTotals(results);

			List<String> resultMeasures = newArrayList(filter(concat(storedMeasures, computedMeasures),
					new Predicate<String>() {
						@Override
						public boolean apply(String measure) {
							return queryMeasures.contains(measure);
						}
					}));

			callback.onResult(buildResult(applyLimitAndOffset(results), totalsPlaceholder, results.size(),
					resultMeasures, filterAttributesPlaceholder));
		}

		QueryResult buildResult(List results, TotalsPlaceholder totalsPlaceholder,
		                        int count, List<String> resultMeasures, Object filterAttributesPlaceholder) {
			List<String> dimensions = newArrayList(storedDimensions);
			List<String> attributes = this.attributes;
			List<String> filterAttributes = nullOrContains(metadataFields, "filterAttributes") ?
					this.filterAttributes : null;

			return new QueryResult(results, resultClass, totalsPlaceholder, count, drillDowns, chains, dimensions,
					attributes, resultMeasures, appliedOrderings, filterAttributesPlaceholder, filterAttributes, fields,
					metadataFields);
		}

		List performSearch(List results) {
			if (searchString == null)
				return results;

			final StringMatcher matcher = createSearchMatcher(concat(cubeQueryDimensions, attributes));

			return newArrayList(filter(results, new Predicate() {
				@Override
				public boolean apply(Object o) {
					return matcher.matches(o, searchString);
				}
			}));
		}

		List applyLimitAndOffset(List results) {
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

		TotalsPlaceholder computeTotals(List<QueryResultPlaceholder> results) {
			TotalsPlaceholder totalsPlaceholder = createTotalsPlaceholder();

			if (results.isEmpty()) {
				totalsPlaceholder.computeMeasures();
				return totalsPlaceholder;
			}

			totalsPlaceholder.init(results.get(0));

			for (int i = 1; i < results.size(); ++i) {
				totalsPlaceholder.accumulate(results.get(i));
			}

			totalsPlaceholder.computeMeasures();
			return totalsPlaceholder;
		}

		TotalsPlaceholder createTotalsPlaceholder() {
			AsmBuilder<TotalsPlaceholder> builder = new AsmBuilder<>(localClassLoader, TotalsPlaceholder.class);

			for (String field : cubeQueryStoredMeasures) {
				FieldType fieldType = structure.getFieldType(field);
				builder.withField(field, fieldType.getDataType());
			}
			for (String computedMeasure : sortedComputedMeasures) {
				builder.withField(computedMeasure, double.class);
			}

			ExpressionSequence initSequence = sequence();
			ExpressionSequence accumulateSequence = sequence();
			for (String field : cubeQueryStoredMeasures) {
				FieldType fieldType = structure.getFieldType(field);
				initSequence.add(fieldType.fieldProcessor().getOnFirstItemExpression(
						getter(self(), field), fieldType.getDataType(),
						getter(cast(arg(0), resultClass), field), fieldType.getDataType()));
				accumulateSequence.add(fieldType.fieldProcessor().getOnNextItemExpression(
						getter(self(), field), fieldType.getDataType(),
						getter(cast(arg(0), resultClass), field), fieldType.getDataType()));
			}
			builder.withMethod("init", initSequence);
			builder.withMethod("accumulate", accumulateSequence);

			ExpressionSequence computeSequence = sequence();
			for (String computedMeasure : sortedComputedMeasures) {
				computeSequence.add(set(getter(self(), computedMeasure),
						reportingConfiguration.getComputedMeasureExpression(computedMeasure)));
			}
			builder.withMethod("computeMeasures", computeSequence);

			return builder.newInstance();
		}

		void computeMeasures(List<QueryResultPlaceholder> results) {
			for (QueryResultPlaceholder queryResult : results) {
				queryResult.computeMeasures();
			}
		}

		Class createFilterAttributesClass() {
			AsmBuilder<Object> builder = new AsmBuilder<>(localClassLoader, Object.class);
			for (String filterAttribute : filterAttributes) {
				builder.withField(filterAttribute, attributeTypes.get(filterAttribute));
			}
			return builder.defineClass();
		}

		StringMatcher createSearchMatcher(Iterable<String> properties) {
			AsmBuilder<StringMatcher> builder = new AsmBuilder<>(localClassLoader, StringMatcher.class);

			PredicateDefOr predicate = or();

			for (String property : properties) {
				Expression propertyValue = cast(getter(cast(arg(0), resultClass), property), Object.class);

				predicate.add(cmpEq(
						choice(isNull(propertyValue),
								value(false),
								call(call(propertyValue, "toString"), "contains", cast(arg(1), CharSequence.class))),
						value(true)));
			}

			builder.withMethod("matches", boolean.class, asList(Object.class, String.class), predicate);
			return builder.newInstance();
		}
	}
}
