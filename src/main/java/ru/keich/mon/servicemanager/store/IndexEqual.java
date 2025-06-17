package ru.keich.mon.servicemanager.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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

public class IndexEqual<K, T extends BaseEntity<K>> implements Index<K, T> {
	private static final Object PRESENT = new Object();
	private final Function<T, Set<Object>> mapper;
	private final Map<Object, Map<K, Object>> objects = new HashMap<>();
	private final AtomicInteger metricObjectsSize = new AtomicInteger(0);
	
	public IndexEqual(Function<T, Set<Object>> mapper) {
		this.mapper = mapper;
	}

	@Override
	public synchronized Set<K> findByKey( Predicate<Object> predicate) {
		return objects.entrySet().stream()
				.filter(entry -> {
						return predicate.test(entry.getKey());
					})
				.map(Map.Entry::getValue)
				.map(Map::keySet)
				.flatMap(Set::stream)
				.collect(Collectors.toSet());
	}

	@Override
	public synchronized void append(T entity) {
		mapper.apply(entity).forEach(key -> {
			objects.compute(key, (k, map) ->{
				if(Objects.nonNull(map)) {
					map.put(entity.getId(), PRESENT);
				} else {
					map = new HashMap<>();
					map.put(entity.getId(), PRESENT);
				}
				return map;
			});
		});
		metricObjectsSize.set(objects.size());
	}

	@Override
	public synchronized void remove(final T entity) {
		mapper.apply(entity).forEach(key -> {
			objects.compute(key, (k, map) ->{
				if(Objects.nonNull(map)) {
					if (map.size() == 1) {
						return null;
					}
					map.remove(entity.getId());
				}
				return map;
			});
		});
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
	public Set<K> getBefore(Object key) {
		throw new UnsupportedOperationException("Equal index has't this method");
	}

	@Override
	public Set<K> getAfterEqual(Object key) {
		throw new UnsupportedOperationException("Equal index has't this method");
	}

	@Override
	public Set<K> getAfterFirst(Object key) {
		throw new UnsupportedOperationException("Equal index has't this method");
	}

	@Override
	public synchronized Set<K> valueSet() {
		return objects.values().stream().map(Map::keySet).flatMap(Set::stream).collect(Collectors.toSet());
	}

	@Override
	public AtomicInteger getMetricObjectsSize() {
		return metricObjectsSize;
	}
	
	@Override
	public synchronized void removeOldAndAppend(T oldEntity, T newEntity) {
		remove(oldEntity);
		append(newEntity);
	}

}
