package ru.keich.mon.servicemanager.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

public class IndexSorted<K, T extends BaseEntity<K>> implements Index<K, T> {
	private static final Object PRESENT = new Object();
	private final Function<T, Set<Object>> mapper;
	private final SortedMap<Object, Map<K, Object>> objects = new TreeMap<>();
	private final AtomicInteger metricObjectsSize = new AtomicInteger(0);
	
	public IndexSorted(Function<T, Set<Object>> mapper) {
		this.mapper = mapper;
	}
	
	private void put(Object key, K id) {
		objects.compute(key, (k, map) -> {
			if (map == null) {
				map = new HashMap<>();
			}
			map.put(id, PRESENT);
			return map;
		});
	}

	private void del(Object key, K id) {
		objects.compute(key, (k, set) -> {
			if (set != null) {
				set.remove(id);
				if (set.isEmpty()) {
					return null;
				}
			}
			return set;
		});
	}

	@Override
	public synchronized Set<K> findByKey( Predicate<Object> predicate) {
		return objects.entrySet().stream()
				.filter(entry -> predicate.test(entry.getKey()))
				.map(Map.Entry::getValue)
				.map(Map::keySet)
				.flatMap(Set::stream)
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
				.map(Map::keySet)
				.map(Set::stream)
				.orElse(Stream.empty())
				.collect(Collectors.toSet());
	}

	@Override
	public synchronized Set<K> getBefore(Object key) {
		return objects.headMap(key).values().stream().flatMap(l -> l.keySet().stream()).collect(Collectors.toSet());
	}
	
	@Override
	public synchronized Set<K> getAfterEqual(Object key) {
		return objects.tailMap(key).values().stream().flatMap(l -> l.keySet().stream()).collect(Collectors.toSet());
	}
	
	@Override
	public synchronized Set<K> getAfterFirst(Object key) {
		var view = objects.tailMap(key);
		if (view.isEmpty()) {
			return Collections.emptySet();
		}
		return objects.get(view.firstKey()).keySet();
	}

	@Override
	public synchronized Set<K> valueSet() {
		return objects.values().stream().flatMap(l -> l.keySet().stream()).collect(Collectors.toSet());
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
		oldSet.stream()
				.filter(key -> !newSet.contains(key))
				.forEach(key -> del(key, id));
		newSet.stream()
				.filter(key -> !oldSet.contains(key))
				.forEach(key -> put(key, id));
		metricObjectsSize.set(objects.size());
	}
	
}
