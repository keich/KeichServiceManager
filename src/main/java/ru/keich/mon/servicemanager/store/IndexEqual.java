package ru.keich.mon.servicemanager.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

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
		objects.compute(key, (k, map) -> {
			if (map != null) {
				map.remove(id);
				if (map.isEmpty()) {
					return null;
				}
			}
			return map;
		});
	}
	
	@Override
	public synchronized Set<K> findByKey( Predicate<Object> predicate) {
		var out = new HashSet<K>();
		for(var entry: objects.entrySet()) {
			if(predicate.test(entry.getKey())) {
				out.addAll(entry.getValue().keySet());
			}
		}
		return out;
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
		var map = objects.get(key);
		if(map == null) {
			return Collections.emptySet();
		}
		return new HashSet<K>(map.keySet());
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
		var out = new HashSet<K>();
		var entries = objects.entrySet();
		for(var entry: entries) {
			out.addAll(entry.getValue().keySet());
		}
		return out;
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
