package ru.keich.mon.servicemanager.entity;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.MultiValueMap;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import ru.keich.mon.indexedhashmap.IndexedHashMap;
import ru.keich.mon.indexedhashmap.Metrics;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.QueueInfo;
import ru.keich.mon.servicemanager.QueueThreadReader;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.query.Operator;
import ru.keich.mon.servicemanager.query.QueryParamsParser;
import ru.keich.mon.servicemanager.query.QueryPredicate;
import ru.keich.mon.servicemanager.query.QuerySort;

/*
 * Copyright 2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public abstract class EntityService<K, T extends Entity<K>> {
	static public final Long VERSION_MIN = 0L;
	static final public String METRIC_NAME_PREFIX = "ksm_";
	static final public String METRIC_VERSION_NAME = "version";
	static final public String METRIC_NAME_SERVICENAME = "servicename";
	static final public String METRIC_NAME_OPERATION = "operation";
	static final public String METRIC_NAME_ADDED = "insered";
	static final public String METRIC_NAME_UPDATED = "updated";
	static final public String METRIC_NAME_REMOVED = "removed";
	static final public String METRIC_NAME_OBJECTS = "objects_";
	static final public String METRIC_NAME_SIZE = "size";
	static final public String METRIC_NAME_INDEX = "index";

	private ru.keich.mon.indexedhashmap.Metrics metrics;
	private final ReentrantLock updateMetricsLock = new ReentrantLock();

	private AtomicLong incrementVersion = new AtomicLong(VERSION_MIN + 1);

	final protected IndexedHashMap<K, T> entityCache;
	final protected QueueThreadReader<QueueInfo<K>> entityChangedQueue;
	final protected Map<String, Function<T, Set<Object>>> queryValueMapper = new HashMap<>();

	final public String nodeName;

	private final Counter metricVersion;
	private final Counter metricAdded;
	private final Counter metricUpdated;
	private final Counter metricRemoved;
	private final MeterRegistry registry;
	private final Tags metricTags;

	public EntityService(String nodeName, MeterRegistry registry, Integer threadCount) {
		this.nodeName = nodeName.intern();
		this.registry = registry;
		var serviceName = this.getClass().getSimpleName();
		entityCache = new IndexedHashMap<>();
		entityChangedQueue = new QueueThreadReader<QueueInfo<K>>(serviceName, threadCount, this::queueRead);

		entityCache.addIndexLongUniq(Entity.FIELD_VERSION, Entity::getVersionForIndex);
		entityCache.addIndexEqual(Entity.FIELD_SOURCE, Entity::getSourceForIndex);
		entityCache.addIndexEqual(Entity.FIELD_SOURCEKEY, Entity::getSourceKeyForIndex);
		entityCache.addIndexEqual(Entity.FIELD_SOURCETYPE, Entity::getSourceTypeForIndex);
		entityCache.addIndexSorted(Entity.FIELD_DELETEDON, Entity::getDeletedOnForIndex);
		entityCache.addIndexSorted(Entity.FIELD_CREATEDON, Entity::getCreatedOnForIndex);
		entityCache.addIndexSorted(Entity.FIELD_UPDATEDON, Entity::getUpdatedOnForIndex);
		entityCache.addIndexSmallInt(Item.FIELD_STATUS, BaseStatus.length, Entity::getStatusForIndex);

		entityCache.addIndexEqual(Entity.FIELD_FIELDS, Entity::getFieldsForIndex);
		entityCache.addIndexEqual(Entity.FIELD_FROMHISTORY, Entity::getFromHistoryForIndex);

		metrics = entityCache.getMetrics();
		metricTags = Tags.of(METRIC_NAME_SERVICENAME, serviceName);

		metricVersion = registry.counter(METRIC_NAME_PREFIX + METRIC_VERSION_NAME, metricTags);
		var opr = METRIC_NAME_PREFIX + METRIC_NAME_OPERATION;
		metricAdded = registry.counter(opr, metricTags.and(Tags.of(METRIC_NAME_OPERATION, METRIC_NAME_ADDED)));
		metricUpdated = registry.counter(opr, metricTags.and(Tags.of(METRIC_NAME_OPERATION, METRIC_NAME_UPDATED)));
		metricRemoved = registry.counter(opr, metricTags.and(Tags.of(METRIC_NAME_OPERATION, METRIC_NAME_REMOVED)));

		registry.gauge(METRIC_NAME_PREFIX + METRIC_NAME_OBJECTS + METRIC_NAME_SIZE, metricTags, this, s -> s.getChachedMetrics().objectsSize());

	}
	
	protected void registerIndexMetrics() {
		var indexSize = entityCache.getMetrics().indexSize();
		var idxName = METRIC_NAME_PREFIX + METRIC_NAME_INDEX + "_" + METRIC_NAME_SIZE;
		indexSize.entrySet().forEach(e -> {
			var indexName = e.getKey();
			var indexTags = metricTags.and(Tags.of(METRIC_NAME_INDEX, indexName));
			registry.gauge(idxName, indexTags, this, s -> s.getChachedMetrics().indexSize().get(indexName));
		});
	}

	protected Long getNextVersion() {
		return incrementVersion.incrementAndGet();
	}

	protected abstract void queueRead(QueueInfo<K> info);	

	public abstract void addOrUpdate(T entity);

	public abstract Optional<T> deleteById(K entityId);

	public Optional<T> findById(K id) {
		return Optional.ofNullable(entityCache.get(id));
	}

	public List<T> findByIds(Set<K> ids) {
		return entityCache.get(ids);
	}

	public List<T> deleteByIds(List<K> ids) {
		return ids.stream()
				.map(this::deleteById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
	}

	public List<T> deleteBySourceAndSourceKeyNot(String source, String sourceKey) {
		var sourceIndex = entityCache.keySetIndexEq(Entity.FIELD_SOURCE, source);
		var sourceKeyIndex = entityCache.keySetIndexEq(Entity.FIELD_SOURCEKEY, sourceKey);
		sourceIndex.removeAll(sourceKeyIndex);
		return sourceIndex.stream()
				.map(this::deleteById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
	}

	@Value("${entity.delete.secondsold:30}") Long seconds;

	@Scheduled(fixedRateString = "${entity.delete.fixedrate:60}", timeUnit = TimeUnit.SECONDS)
	public void deleteOldScheduled() {
		entityCache.keySetIndexGetBefore(Entity.FIELD_DELETEDON, Instant.now().minusSeconds(seconds)).forEach(entityCache::remove);
	}

	public Comparator<T> getSortComparator(QuerySort sort) {
		final int mult = sort.getOperator() == Operator.SORTDESC ? -1 : 1;
		switch (sort.getName()) {
		case Entity.FIELD_STATUS:
			return (e1, e2) -> e1.getStatus().compareTo(e2.getStatus()) * mult;
		case Entity.FIELD_CREATEDON:
			return (e1, e2) -> e1.getCreatedOn().compareTo(e2.getCreatedOn()) * mult;
		case Entity.FIELD_UPDATEDON:
			return (e1, e2) -> e1.getUpdatedOn().compareTo(e2.getUpdatedOn()) * mult;
		case Entity.FIELD_DELETEDON:
			return (e1, e2) -> e1.getDeletedOn().compareTo(e2.getDeletedOn()) * mult;
		case Entity.FIELD_SOURCE:
			return (e1, e2) -> e1.getSource().compareTo(e2.getSource()) * mult;
		case Entity.FIELD_SOURCEKEY:
			return (e1, e2) -> e1.getSourceKey().compareTo(e2.getSourceKey()) * mult;
		case Entity.FIELD_SOURCETYPE:
			return (e1, e2) -> e1.getSourceType().compareTo(e2.getSourceType()) * mult;
		}
		return (e1, e2) -> {
			return e1.getVersion().compareTo(e2.getVersion()) * mult;
		};
	}
	
	protected abstract Stream<T> enrich(Stream<T> data, QueryParamsParser qp);
	
	public <R> R sortAndLimitEnrich(MultiValueMap<String, String> reqParam
			,Function<QueryParamsParser, Stream<T>> supplier,
			BiFunction<Stream<T>, QueryParamsParser, R> jsonFilter) {
		var qp = new QueryParamsParser(reqParam, this::fieldValueOf);
		var stream = supplier.apply(qp);
		stream = sortAndLimit(stream, qp.getSorts(), qp.getLimit());
		stream = enrich(stream, qp);
		return jsonFilter.apply(stream, qp);
	}

	private Stream<T> sortAndLimit(Stream<T> data, List<QuerySort> sorts, long limit) {
		if(!sorts.isEmpty()) {
			var comparator = sorts.stream()
					.sorted((s1, s2) -> s1.getOrder() - s2.getOrder())
					.map(this::getSortComparator)
					.reduce(Comparator::thenComparing)
					.orElse((e1, e2) -> 0);
			data = data.sorted(comparator);
		}
		if (limit > 0) {
			return data.limit(limit);
		}
		return data;
	}

	public Stream<T> findByPredicates(List<QueryPredicate> predicates, Set<K> filterbyId) {
		var opt = predicates.stream()
				.map(this::find)
				.reduce((result, el) -> { 
					result.retainAll(el);
					return result;
				});
		var s = opt.orElse(Collections.emptySet()).stream();
		if(filterbyId.size() > 0) {
			s = s.filter(id -> filterbyId.contains(id));
		}
		return	s.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get);
	}
	
	protected abstract EntitySearchResult<K> getEntitySearchResult(String search);

	public Stream<T> findBySearch(String search, Set<K> filterbyId) {
		var s = getEntitySearchResult(search).getResult().stream();
		if(filterbyId.size() > 0) {
			s = s.filter(id -> filterbyId.contains(id));
		}
		return s.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get);
	}

	@SuppressWarnings("rawtypes")
	public Set<K> find(QueryPredicate predicate) {
		var fieldName = predicate.getName();
		var indexNames = entityCache.getIndexNames();
		if (indexNames.contains(fieldName)) {
			switch (predicate.getOperator()) {
			case EQ:
				return entityCache.keySetIndexEq(fieldName, predicate.getValue());
			case NE:
			case CO:
			case NC:
				return entityCache.keySetIndexPredicate(fieldName, predicate.getPredicate());
			case NI:
				var t = entityCache.keySetIndexEq(fieldName, predicate.getValue());
				var r = entityCache.keySet();
				r.removeAll(t);
				return r;
			case LT:
				return entityCache.keySetIndexGetBefore(fieldName, predicate.getValue());
			case GT:
				return entityCache.keySetIndexGetAfter(fieldName, predicate.getValue());
			case GE:
				return entityCache.keySetIndexGetAfterEqual(fieldName, predicate.getValue());
			case ISNULL:
				Set<K> eq;
				if(Entity.FIELD_FIELDS.equals(fieldName)) {
					var subName = predicate.getValue();
					eq = entityCache.keySetIndexPredicate(fieldName, o -> {
						if(((Map.Entry)o).getKey().equals(subName)) return true;
						return false;
					});
				} else {
					eq = entityCache.keySetIndexAll(fieldName);
				}
				var all = entityCache.keySet();
				all.removeAll(eq);
				return all;
			default:
				return new HashSet<>(0);
			}
		} else if (Entity.FIELD_ID.equals(fieldName)) {
			if(predicate.getOperator() == Operator.EQ) {
				var id = predicate.getValue();
				var entity = entityCache.get(id);
				if(entity != null) {
					return new HashSet<>(Collections.singletonList(entity.getId()));
				}
				return new HashSet<>(0);
			}
			return entityCache.keySet().stream().filter(predicate.getPredicate()).collect(Collectors.toSet());
		} else if (queryValueMapper.containsKey(fieldName)) {
			return entityCache.keySetPredicate(queryValueMapper.get(fieldName), predicate.getPredicate());
		}
		return new HashSet<>(0);
	}

	public Stream<T> find(QueryParamsParser qp, Set<K> filterbyId) {
		if(qp.isHasSearch()) {
			return findBySearch(qp.getSearch(), filterbyId);
		} else {
			return findByPredicates(qp.getPredicates(), filterbyId);
		}
	}

	public Stream<T> find(QueryParamsParser qp) {
		if(qp.isHasSearch()) {
			return findBySearch(qp.getSearch(), Collections.emptySet());
		} else {
			return findByPredicates(qp.getPredicates(), Collections.emptySet());
		}
	}

	@Scheduled(fixedRateString = "5", timeUnit = TimeUnit.SECONDS)
	public void updateMetrics() {
		updateMetricsLock.lock();
		try {
			this.metrics = entityCache.getMetrics();
			metricVersion.increment(incrementVersion.doubleValue() - metricVersion.count());
			metricAdded.increment(metrics.added() - metricAdded.count());
			metricUpdated.increment(metrics.updated() - metricUpdated.count());
			metricRemoved.increment(metrics.removed() - metricRemoved.count());
		} finally {
			updateMetricsLock.unlock();
		}
	}

	private Metrics getChachedMetrics() {
		updateMetricsLock.lock();
		try {
			return metrics;
		} finally {
			updateMetricsLock.unlock();
		}
	}
	
	public Object fieldValueOf(String fieldName, String str) {
		return Entity.fieldValueOf(fieldName, str);
	}

}
