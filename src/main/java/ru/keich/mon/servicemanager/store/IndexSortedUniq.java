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
	
	private void put(Object key, K id) {
		if(objects.containsKey(key)) {
			throw new UnsupportedOperationException("Already contains key " + key);
		}
		objects.put(key, id);
	}
	
	private void del(Object key, K id) {
		objects.remove(key);
	}

	@Override
	public synchronized Set<K> findByKey(Predicate<Object> predicate) {
		return objects.entrySet().stream()
				.filter(entry -> predicate.test(entry.getKey()))
				.map(Map.Entry::getValue)
				.collect(Collectors.toSet());
	}
	@Override
	public synchronized void append(T entity) {
		mapper.apply(entity).forEach(key -> put(key, entity.getId()));
		metricObjectsSize.set(objects.size());
	}

	@Override
	public synchronized void remove(final T entity) {
		mapper.apply(entity).forEach(key -> del(key, entity.getId()));
		metricObjectsSize.set(objects.size());
	}

	@Override
	public synchronized Set<K> get(Object key) {
		return Optional.ofNullable(objects.get(key))
				.map(Collections::singleton)
				.orElse(Collections.emptySet());
	}

	@Override
	public synchronized Set<K> getBefore(Object key) {
		return objects.headMap(key).values().stream().collect(Collectors.toSet());
	}

	@Override
	public synchronized Set<K> getAfterEqual(Object key) {
		return objects.tailMap(key).values().stream().collect(Collectors.toSet());
	}

	@Override
	public synchronized Set<K> getAfterFirst(Object key) {
		var view = objects.tailMap(key);
		if (view.isEmpty()) {
			return Collections.emptySet();
		}
		return Collections.singleton(objects.get(view.firstKey()));
	}

	@Override
	public synchronized Set<K> valueSet() {
		return objects.values().stream().collect(Collectors.toSet());
	}
	
	@Override
	public AtomicInteger getMetricObjectsSize() {
		return metricObjectsSize;
	}
	
	@Override
	public synchronized void removeOldAndAppend(T oldEntity, T newEntity) {
		final K id = newEntity.getId();
		var oldSet = mapper.apply(oldEntity);
		var newSet = mapper.apply(newEntity);
		
		for(var key: oldSet) {
			if(!newSet.contains(key)) {
				del(key, id);
			}
		}
		for(var key: newSet) {
			if(!oldSet.contains(key)) {
				put(key, id);
			}
		}
		metricObjectsSize.set(objects.size());
	}
	
}
