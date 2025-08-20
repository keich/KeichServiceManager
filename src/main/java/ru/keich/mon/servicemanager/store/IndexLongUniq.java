package ru.keich.mon.servicemanager.store;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

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

public class IndexLongUniq<K, T extends BaseEntity<K>> implements Index<K, T> {
	private final Function<T, Long> mapper;
	private final SortedMap<Object, K> objects = new TreeMap<>();
	private final AtomicInteger metricObjectsSize = new AtomicInteger(0);
	
	public IndexLongUniq(Function<T, Long> mapper) {
		this.mapper = mapper;
	}
	
	private void put(Long key, K id) {
		if(objects.containsKey(key)) {
			throw new UnsupportedOperationException("Already contains key " + key);
		}
		objects.put(key, id);
	}
	
	private void del(Long key, K id) {
		objects.remove(key);
	}

	@Override
	public synchronized Set<K> findByKey(Predicate<Object> predicate) {
		var out = new HashSet<K>();
		for(var entry: objects.entrySet()) {
			if(predicate.test(entry.getKey())) {
				out.add(entry.getValue());
			}
		}
		return out;
	}
	
	@Override
	public synchronized void append(T entity) {
		put(mapper.apply(entity), entity.getId());
		metricObjectsSize.set(objects.size());
	}

	@Override
	public synchronized void remove(final T entity) {
		del(mapper.apply(entity), entity.getId());
		metricObjectsSize.set(objects.size());
	}

	@Override
	public synchronized Set<K> get(Object key) {
		var val = objects.get(key);
		if(val == null) {
			return Collections.emptySet();
		}
		return Collections.singleton(val);
	}

	@Override
	public synchronized Set<K> getBefore(Object key) {
		var out = new HashSet<K>();
		for(var val: objects.headMap(key).values()) {
			out.add(val);
		}
		return out;
	}

	@Override
	public synchronized Set<K> getAfterEqual(Object key) {
		var out = new HashSet<K>();
		for(var val: objects.tailMap(key).values()) {
			out.add(val);
		}
		return out;
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
		return new HashSet<K>(objects.values());
	}
	
	@Override
	public AtomicInteger getMetricObjectsSize() {
		return metricObjectsSize;
	}
	
	@Override
	public synchronized void removeOldAndAppend(T oldEntity, T newEntity) {
		final K id = newEntity.getId();
		var oldVal = mapper.apply(oldEntity);
		var newVal = mapper.apply(newEntity);
		
		if(!oldVal.equals(newVal)) {
			del(oldVal, id);
			put(newVal, id);
		}
		metricObjectsSize.set(objects.size());
	}
	
}
