package ru.keich.mon.servicemanager.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

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
		objects[status.getInt()].put(id, PRESENT);
	}

	private void del(Object key, K id) {
		BaseStatus status = (BaseStatus)key;
		objects[status.getInt()].remove(id);
	}

	@Override
	public synchronized Set<K> findByKey( Predicate<Object> predicate) {
		var out = new HashSet<K>();
		for(var val: BaseStatus.values()) {
			if(predicate.test(val)) {
				out.addAll(objects[val.getInt()].keySet());
			}
		}
		return out;
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
		return new HashSet<K>(objects[status.getInt()].keySet());
	}

	@Override
	public synchronized Set<K> getBefore(Object key) {
		BaseStatus status = (BaseStatus)key;
		var out = new HashSet<K>();
		for(int i = 0; i < status.getInt(); i++) {
			out.addAll(objects[i].keySet());
		}
		return out;
	}
	
	@Override
	public synchronized Set<K> getAfterEqual(Object key) {
		BaseStatus status = (BaseStatus)key;
		var out = new HashSet<K>();
		for(int i = status.getInt(); i < BaseStatus.length; i++) {
			out.addAll(objects[i].keySet());
		}
		return out;
	}
	
	@Override
	public synchronized Set<K> getAfterFirst(Object key) {
		BaseStatus status = (BaseStatus)key;
		int idx = status.getInt() + 1;
		if(idx < BaseStatus.length) {
			return new HashSet<K>(objects[idx].keySet());
		}
		return Collections.emptySet();
	}

	@Override
	public synchronized Set<K> valueSet() {
		var out = new HashSet<K>();
		for(int i = 0; i < BaseStatus.length; i++) {
			out.addAll(objects[i].keySet());
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
	}
	
}
