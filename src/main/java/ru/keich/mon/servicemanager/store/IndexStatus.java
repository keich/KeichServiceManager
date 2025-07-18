package ru.keich.mon.servicemanager.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import ru.keich.mon.servicemanager.BaseStatus;

/*
 * Copyright 2025 the original author or authors.
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

public class IndexStatus<K, T extends BaseEntity<K>> implements Index<K, T> {
	private static final Object PRESENT = new Object();
	private final Function<T, Set<Object>> mapper;
	private final AtomicInteger metricObjectsSize = new AtomicInteger(0);
	private final Map<K, Object> objects[] = new Map[BaseStatus.length];
	
	public IndexStatus(Function<T, Set<Object>> mapper) {
		this.mapper = mapper;
		metricObjectsSize.set(BaseStatus.length);
		for (int i = 0; i < BaseStatus.length; i++) {
			objects[i] = new HashMap<K, Object>();
		}
	}
	
	private void put(Object key, K id) {
		BaseStatus status = (BaseStatus)key;
		objects[status.ordinal()].put(id, PRESENT);
	}

	private void del(Object key, K id) {
		BaseStatus status = (BaseStatus)key;
		objects[status.ordinal()].remove(id);
	}

	@Override
	public synchronized Set<K> findByKey( Predicate<Object> predicate) {
		return Stream.of(BaseStatus.values())
				.filter(s -> predicate.test(s))
				.flatMap(s -> objects[s.ordinal()].keySet().stream())
				.collect(Collectors.toSet());
	}

	@Override
	public synchronized void append(T entity) {
		mapper.apply(entity).forEach(key -> put(key, entity.getId()));
	}

	@Override
	public synchronized void remove(final T entity) {
		mapper.apply(entity).forEach(key -> del(key, entity.getId()));
	}

	@Override
	public synchronized Set<K> get(Object key) {
		BaseStatus status = (BaseStatus)key;
		return objects[status.ordinal()].keySet().stream().collect(Collectors.toSet());
	}

	@Override
	public synchronized Set<K> getBefore(Object key) {
		BaseStatus status = (BaseStatus)key;
		return IntStream.range(0,status.ordinal()).boxed()
				.flatMap(s -> objects[s].keySet().stream())
				.collect(Collectors.toSet());
	}
	
	@Override
	public synchronized Set<K> getAfterEqual(Object key) {
		BaseStatus status = (BaseStatus)key;
		return IntStream.range(status.ordinal(), BaseStatus.length).boxed()
				.flatMap(s -> objects[s].keySet().stream())
				.collect(Collectors.toSet());
	}
	
	@Override
	public synchronized Set<K> getAfterFirst(Object key) {
		BaseStatus status = (BaseStatus)key;
		int idx = status.ordinal() + 1;
		if(idx < BaseStatus.length) {
			return objects[idx].keySet().stream().collect(Collectors.toSet());
		}
		return Collections.emptySet();
	}

	@Override
	public synchronized Set<K> valueSet() {
		return IntStream.range(0, BaseStatus.length).boxed()
		.flatMap(s -> objects[s].keySet().stream())
		.collect(Collectors.toSet());
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
	}
	
}
