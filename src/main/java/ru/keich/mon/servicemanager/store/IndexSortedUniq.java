package ru.keich.mon.servicemanager.store;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

public class IndexSortedUniq<K, T extends BaseEntity<K>> implements Index<K, T> {
	private final Function<T, Set<Object>> mapper;
	private final SortedMap<Object, K> objects = new TreeMap<>();
	private final AtomicInteger metricObjectsSize = new AtomicInteger(0);
	
	public IndexSortedUniq(Function<T, Set<Object>> mapper) {
		this.mapper = mapper;
	}

	@Override
	public Set<K> findByKey(long limit, Predicate<Object> predicate) {
		synchronized (this) {
			var stream =  objects.entrySet().stream()
					.filter(entry -> predicate.test(entry.getKey()))
					.map(Map.Entry::getValue);
			if(limit > 0) {
				return stream.limit(limit).collect(Collectors.toSet());
			}
			return stream.collect(Collectors.toSet());
		}
	}
	
	@Override
	public void append(T entity) {
		synchronized (this) {
			mapper.apply(entity).forEach(key -> {
				if(objects.containsKey(key)) {
					throw new UnsupportedOperationException("Already contains key " + key);
				}
				objects.put(key, entity.getId());
			});
			metricObjectsSize.set(objects.size());
		}
	}

	@Override
	public void remove(final T entity) {
		synchronized (this) {
			mapper.apply(entity).forEach(key -> {
				objects.remove(key);
			});
			metricObjectsSize.set(objects.size());
		}
	}

	@Override
	public Set<K> get(Object key) {
		synchronized (this) {
			return Optional.ofNullable(objects.get(key))
					.map(Collections::singleton)
					.orElse(Collections.emptySet());
		}
	}

	@Override
	public Set<K> getBefore(Object key) {
		synchronized (this) {
			return objects.headMap(key).values().stream().collect(Collectors.toSet());
		}
	}

	@Override
	public Set<K> getAfter(Object key) {
		synchronized (this) {
			return objects.tailMap(key).values().stream().skip(1).collect(Collectors.toSet());
		}
	}

	@Override
	public Set<K> getAfterEqual(Object key) {
		synchronized (this) {
			return objects.tailMap(key).values().stream().collect(Collectors.toSet());
		}
	}

	@Override
	public Set<K> getAfterFirst(Object key) {
		synchronized (this) {
			var view = objects.tailMap(key);
			if(view.isEmpty()) {
				return Collections.emptySet();
			}
			return Collections.singleton(objects.get(view.firstKey()));
		}
	}

	@Override
	public Set<K> valueSet() {
		synchronized (this) {
			return objects.values().stream().collect(Collectors.toSet());
		}
	}
	
	@Override
	public AtomicInteger getMetricObjectsSize() {
		return metricObjectsSize;
	}
	
}
