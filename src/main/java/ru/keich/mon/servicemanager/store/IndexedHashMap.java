package ru.keich.mon.servicemanager.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

	public enum IndexType {
		EQUAL, SORTED, UNIQ_SORTED
	}
	
	private Map<K, T> cache = new ConcurrentHashMap<>();
	private Map<String , Index<K, T>> index =  new HashMap<>(); 

	public IndexedHashMap() {
		super();
	}
	
	public void createIndex(String name, IndexType type, Function<T, Set<Object>> mapper) {
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
				return newEntity;
			}
			return oldEntity;
		}));
	}
	
	public Optional<T> compute(K entityId, Supplier<T> insertTrigger, Function<T,T> updateTrigger) {
		return Optional.ofNullable(cache.compute(entityId, (key, oldEntity) -> {
			if (Objects.nonNull(oldEntity)) {
				final var entity = updateTrigger.apply(oldEntity);
				if (Objects.nonNull(entity) && entity != oldEntity) {
					index.entrySet().forEach(e -> {
						e.getValue().remove(oldEntity);
						e.getValue().append(entity);
					});
					return entity;
				}
			} else {
				final var entity = insertTrigger.get();
				if (Objects.nonNull(entity)) {
					index.entrySet().forEach(e -> {
						e.getValue().append(entity);
					});
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
		return Optional.ofNullable(cache.compute(id, (key, oldEntity) -> {
			if (Objects.nonNull(oldEntity)) {
				index.entrySet().forEach(e -> {
					e.getValue().remove(oldEntity);
				});	
			}
			return null;
		}));
	}
	
	private Set<K> findByField(String fieldName, long limit, QueryPredicate predicate) {
		var methodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
		var stream = cache.entrySet().stream().filter(entry -> {
			var entity = entry.getValue();
			try {
				var v = entry.getValue().getClass().getMethod(methodName).invoke(entity);
				if (predicate.test(v)) {
					return true;
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			return false;
		}).map(e -> e.getKey());
		if(limit > 0) {
			return stream.limit(limit).collect(Collectors.toSet());
		}
		return stream.collect(Collectors.toSet());
	}
	
	public Set<K> keySet(QueryPredicate predicate, long limit) {
		var fieldName = predicate.getName();
		if (index.containsKey(fieldName)) {
			switch (predicate.getOperator()) {
			case EQ:
				return index.get(fieldName).get(predicate.getValue());
			case NE:
			case CO:
			case NC:
				return index.get(fieldName).findByKey(limit, (p) -> predicate.test(p));
			case NI:
				var t = index.get(fieldName).get(predicate.getValue());
				var r = index.get(fieldName).valueSet();
				r.removeAll(t);
				return r;
			case LT:
				return index.get(fieldName).getBefore(predicate.getValue());
			case GT:
				return index.get(fieldName).getAfter(predicate.getValue());
			default:
				return Collections.emptySet();
			}
		} else {
			return findByField(fieldName, limit, predicate);
		}
	}
	
	public Set<K> keySetGreaterEqualFirst(String fieldName, Object value) {
		if (index.containsKey(fieldName)) {
			return index.get(fieldName).getAfterFirst(value);
		}
		var predicate = Predicates.greaterEqual(fieldName, value);
		return findByField(fieldName, 1, predicate);
	}
	
}