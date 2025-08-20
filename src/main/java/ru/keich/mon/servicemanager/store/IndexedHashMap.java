package ru.keich.mon.servicemanager.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
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
	
	static final public long DEFAULT_LIMIT = -1;
	
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
		EQUAL, SORTED, UNIQ_SORTED, STATUS
	}
	
	private Map<K, T> cache = new ConcurrentHashMap<>();
	private Map<String , Index<K, T>> index =  new HashMap<>(); 
	private Map<String , BiFunction<T, QueryPredicate, Boolean>> query =  new HashMap<>(); 

	public IndexedHashMap(MeterRegistry registry, String serviceName) {
		super();

		this.registry = registry;
		this.serviceName = serviceName;

		if (registry != null) {
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
		case STATUS:
			index.put(name, new IndexStatus<K,T>(mapper));
			break;
		}
		if (registry != null) {
			var tags = Tags.of(METRIC_NAME_SERVICENAME, serviceName, METRIC_NAME_INDEX, name.toLowerCase());
			registry.gauge(METRIC_NAME_MAP + METRIC_NAME_INDEX + "_" + METRIC_NAME_SIZE, tags,
					index.get(name).getMetricObjectsSize());
		}
	}
	
	public void addQueryField(String name, BiFunction<T, QueryPredicate, Boolean> test) {
		query.put(name, test);
	}
		
	public Optional<T> put(T entity) {
		return compute(entity.getId(), () -> entity, Function.identity());
	}
	
	
	private T removeEntity(T entity) {
		for(var entry: index.entrySet()) {
			entry.getValue().remove(entity);
		}
		metricRemoved.increment();
		return null;
	}
	
	private T updateEntity(T oldEntity, T newEntity) {
		for(var entry: index.entrySet()) {
			entry.getValue().removeOldAndAppend(oldEntity, newEntity);
		}
		metricUpdated.increment();
		return newEntity;
	}
	
	private T insertEntity(T entity) {
		for(var entry: index.entrySet()) {
			entry.getValue().append(entity);
		}
		metricAdded.increment();
		return entity;
	}
	
	public Optional<T> compute(K entityId, Supplier<T> insertTrigger, Function<T,T> updateTrigger) {
		metricObjectsSize.set(cache.size());
		return Optional.ofNullable(cache.compute(entityId, (key, oldEntity) -> {
			if (oldEntity != null) {
				final var newEntity = updateTrigger.apply(oldEntity);
				if (newEntity  == null) {
					return removeEntity(oldEntity);
				} else if (newEntity != oldEntity) {
					return updateEntity(oldEntity, newEntity);
				}
			} else {
				final var entity = insertTrigger.get();
				if(entity != null) {
					return insertEntity(entity);
				}
				return null;
			}
			return oldEntity;
		}));
	}
	
	public Optional<T> get(K id) {
		return Optional.ofNullable(cache.get(id));
	}
	
	public List<T> get(Set<K> ids) {
		var out = new ArrayList<T>(ids.size());
		for(var id: ids) {
			var e = cache.get(id);
			if(e != null) {
				out.add(e);
			}
		}
		return out;
	}
	
	private Set<K> findByQuery(QueryPredicate predicate) {
		var fieldName = predicate.getName();
		var test = query.get(fieldName);
		return cache.entrySet().stream()
				.filter(entry -> test.apply(entry.getValue(), predicate))
				.map(e -> e.getKey())
				.collect(Collectors.toSet());
	}
	
	private Set<K> findByIndex(QueryPredicate predicate) {
		var fieldName = predicate.getName();
		switch (predicate.getOperator()) {
		case EQ:
			return index.get(fieldName).get(predicate.getValue());
		case NE:
		case CO:
		case NC:
			return index.get(fieldName).findByKey((p) -> predicate.test(p));
		case NI:
			var t = index.get(fieldName).get(predicate.getValue());
			var r = index.get(fieldName).valueSet();
			r.removeAll(t);
			return r;
		case LT:
			return index.get(fieldName).getBefore(predicate.getValue());
		case GT:
			var r1 = index.get(fieldName).getAfterEqual(predicate.getValue());
			var eq = index.get(fieldName).get(predicate.getValue());
			r1.removeAll(eq);
			return r1;
		case GE:
			return index.get(fieldName).getAfterEqual(predicate.getValue());
		default:
			return Collections.emptySet();
		}
	}
	
	public Set<K> keySet(QueryPredicate predicate) {
		return keySet(predicate, DEFAULT_LIMIT);
	}
	
	public Set<K> keySet(QueryPredicate predicate, long limit) {
		var fieldName = predicate.getName();
		Set<K> ret;
		if (index.containsKey(fieldName)) {
			ret = findByIndex(predicate);
		} else if (query.containsKey(fieldName)) {
			ret = findByQuery(predicate);
		} else {
			throw new RuntimeException("Can't query by field \"" + fieldName + "\"");
		}
		if (limit > 0) {
			ret = ret.stream().limit(limit).collect(Collectors.toSet());
		}
		return ret;
	}
	
}