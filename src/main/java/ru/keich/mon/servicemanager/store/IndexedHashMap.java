package ru.keich.mon.servicemanager.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import ru.keich.mon.servicemanager.query.predicates.Predicates;
import ru.keich.mon.servicemanager.query.predicates.QueryPredicate;


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

public class IndexedHashMap<K, T extends BaseEntity<K>> {

	static final public String METRIC_NAME_MAP = "indexedhashmap_";
	static final public String METRIC_NAME_OPERATION = "operation";
	static final public String METRIC_NAME_SERVICENAME = "servicename";

	static final public String METRIC_NAME_OBJECTS = "objects_";
	static final public String METRIC_NAME_SIZE = "size";

	static final public String METRIC_NAME_ADDED = "insered";
	static final public String METRIC_NAME_UPDATED = "updated";
	static final public String METRIC_NAME_REMOVED = "removed";
	static final public String METRIC_NAME_INDEX = "index";
	
	private final MeterRegistry registry;
	private final String serviceName;
	
	private Counter metricAdded = EmptyCounter.EMPTY;
	private Counter metricUpdated = EmptyCounter.EMPTY;
	private Counter metricRemoved = EmptyCounter.EMPTY;
	private final AtomicInteger metricObjectsSize = new AtomicInteger(0);
	
	public static class EmptyCounter implements Counter {

		public static EmptyCounter EMPTY = new EmptyCounter();
		
		@Override
		public Id getId() {
			return null;
		}

		@Override
		public void increment(double amount) {

		}

		@Override
		public double count() {
			return 0;
		}
		
	}
	
	public enum IndexType {
		EQUAL, SORTED, UNIQ_SORTED
	}
	
	private Map<K, T> cache = new ConcurrentHashMap<>();
	private Map<String , Index<K, T>> index =  new HashMap<>(); 

	public IndexedHashMap(MeterRegistry registry, String serviceName) {
		super();

		this.registry = registry;
		this.serviceName = serviceName;

		if(Objects.nonNull(registry)) {
			metricAdded = registry.counter(METRIC_NAME_MAP + METRIC_NAME_OPERATION, METRIC_NAME_OPERATION,
					METRIC_NAME_ADDED, METRIC_NAME_SERVICENAME, serviceName);
	
			metricUpdated = registry.counter(METRIC_NAME_MAP + METRIC_NAME_OPERATION, METRIC_NAME_OPERATION,
					METRIC_NAME_UPDATED, METRIC_NAME_SERVICENAME, serviceName);
	
			metricRemoved = registry.counter(METRIC_NAME_MAP + METRIC_NAME_OPERATION, METRIC_NAME_OPERATION,
					METRIC_NAME_REMOVED, METRIC_NAME_SERVICENAME, serviceName);
	
			registry.gauge(METRIC_NAME_MAP + METRIC_NAME_OBJECTS + METRIC_NAME_SIZE,
					Tags.of(METRIC_NAME_SERVICENAME, serviceName), metricObjectsSize);
		}
	}
	
	public void addIndex(String name, IndexType type, Function<T, Set<Object>> mapper) {
		switch(type) {
		case EQUAL:
			index.put(name, new IndexEqual<K,T>(mapper));
			break;
		case SORTED:
			index.put(name, new IndexSorted<K,T>(mapper));
			break;
		case UNIQ_SORTED:
			index.put(name, new IndexSortedUniq<K,T>(mapper));
			break;		
		}
		if(Objects.nonNull(registry)) {
			var tags = Tags.of(METRIC_NAME_SERVICENAME, serviceName, METRIC_NAME_INDEX, name.toLowerCase());
			registry.gauge(METRIC_NAME_MAP + METRIC_NAME_INDEX + "_" + METRIC_NAME_SIZE, tags,
					index.get(name).getMetricObjectsSize());
		}
	}
		
	public Optional<T> put(T entity) {
		return compute(entity.getId(), () -> entity, (o) -> null);
	}
	
	public Optional<T> computeIfPresent(K entityId, Function<T,T> updateTrigger) {
		return Optional.ofNullable(cache.computeIfPresent(entityId, (key, oldEntity) -> {
			final var newEntity = updateTrigger.apply(oldEntity);
			if (Objects.nonNull(newEntity) && newEntity != oldEntity) {
				index.entrySet().forEach(e -> {
					e.getValue().remove(oldEntity);
					e.getValue().append(newEntity);
				});
				metricUpdated.increment();
				return newEntity;
			}
			return oldEntity;
		}));
	}
	
	public Optional<T> compute(K entityId, Supplier<T> insertTrigger, Function<T,T> updateTrigger) {
		metricObjectsSize.set(cache.size());
		return Optional.ofNullable(cache.compute(entityId, (key, oldEntity) -> {
			if (Objects.nonNull(oldEntity)) {
				final var entity = updateTrigger.apply(oldEntity);
				if (Objects.nonNull(entity) && entity != oldEntity) {
					index.entrySet().forEach(e -> {
						e.getValue().remove(oldEntity);
						e.getValue().append(entity);
					});
					metricUpdated.increment();
					return entity;
				}
			} else {
				final var entity = insertTrigger.get();
				if (Objects.nonNull(entity)) {
					index.entrySet().forEach(e -> {
						e.getValue().append(entity);
					});
					metricAdded.increment();
					return entity;
				}
			}
			return oldEntity;
		}));
	}
	
	public Optional<T> get(K id) {
		return Optional.ofNullable(cache.get(id));
	}

	public Optional<T> remove(K id) {
		metricObjectsSize.set(cache.size());
		return Optional.ofNullable(cache.compute(id, (key, oldEntity) -> {
			if (Objects.nonNull(oldEntity)) {
				index.entrySet().forEach(e -> {
					e.getValue().remove(oldEntity);
				});	
				metricRemoved.increment();
			}
			return null;
		}));
	}
	
	private Set<K> findByField(String fieldName, long limit, QueryPredicate predicate) {
		var s = cache.entrySet().stream()
				.filter(entry -> entry.getValue().testQueryPredicate(predicate))
				.map(e -> e.getKey());
		if(limit > 0) {
			return s.limit(limit).collect(Collectors.toSet());
		}
		return s.collect(Collectors.toSet());
	}
	
	public Set<K> keySet(QueryPredicate predicate, long limit) {
		var fieldName = predicate.getName();
		if (index.containsKey(fieldName)) {
			switch (predicate.getOperator()) {
			case EQ:
				var r5 = index.get(fieldName).get(predicate.getValue());
				if(limit > 0) {
					return r5.stream().limit(limit).collect(Collectors.toSet());
				}
				return r5;
			case NE:
			case CO:
			case NC:
				var r4 = index.get(fieldName).findByKey(limit, (p) -> predicate.test(p));
				if(limit > 0) {
					return r4.stream().limit(limit).collect(Collectors.toSet());
				}
				return r4;
			case NI:
				var t = index.get(fieldName).get(predicate.getValue());
				var r = index.get(fieldName).valueSet();
				r.removeAll(t);
				if(limit > 0) {
					return r.stream().limit(limit).collect(Collectors.toSet());
				}
				return r;
			case LT:
				var r3 = index.get(fieldName).getBefore(predicate.getValue());
				if(limit > 0) {
					return r3.stream().limit(limit).collect(Collectors.toSet());
				}
				return r3;
			case GT:
				var r1 = index.get(fieldName).getAfterEqual(predicate.getValue());
				var eq = index.get(fieldName).get(predicate.getValue());
				r1.removeAll(eq);
				if(limit > 0) {
					return r1.stream().limit(limit).collect(Collectors.toSet());
				}
				return r1;
			case GE:
				var r2 = index.get(fieldName).getAfterEqual(predicate.getValue());
				if(limit > 0) {
					return r2.stream().limit(limit).collect(Collectors.toSet());
				}
				return r2;
			default:
				return Collections.emptySet();
			}
		} else {
			return findByField(fieldName, limit, predicate);
		}
	}
	
}