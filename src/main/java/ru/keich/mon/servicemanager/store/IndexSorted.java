package ru.keich.mon.servicemanager.store;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

public class IndexSorted<K, T extends BaseEntity<K>> implements Index<K, T> {
	private final Function<T, Set<Object>> mapper;
	private final SortedMap<Object, Set<K>> objects = new TreeMap<>();
	
	public IndexSorted(Function<T, Set<Object>> mapper) {
		this.mapper = mapper;
	}

	@Override
	public void append(T entity) {
		mapper.apply(entity).forEach(key -> {
			if(objects.containsKey(key)) {
				objects.get(key).add(entity.getId());
			}else {
				Set<K> set = new HashSet<>();
				set.add(entity.getId());
				objects.put(key, set);
			}
		});
	}

	@Override
	public void remove(final T entity) {
		mapper.apply(entity).forEach(key -> {
			if(objects.containsKey(key)) {
				var set = objects.get(key);
				set.remove(entity.getId());
				if (set.size() == 0) {
					objects.remove(key);
				}
			}
		});
	}

	@Override
	public List<K> get(Object key) {
		return Optional.ofNullable(objects.get(key))
				.map(s -> s.stream().toList())
				.orElse(Collections.emptyList());
	}

	@Override
	public List<K> getBefore(Object key) {
		return objects.headMap(key).values().stream().flatMap(l -> l.stream()).toList();
	}

	@Override
	public List<K> getAfter(Object key) {
		return objects.tailMap(key).values().stream().flatMap(l -> l.stream()).toList();
	}

}
