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
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class IndexSortedUniq<K, T extends BaseEntity<K>> implements Index<K, T> {
	private final Function<T, Set<Object>> mapper;
	private final SortedMap<Object, K> objects = new TreeMap<>();
	
	public IndexSortedUniq(Function<T, Set<Object>> mapper) {
		this.mapper = mapper;
	}

	@Override
	public Set<K> findByKey(long limit, Predicate<Object> predicate) {
		synchronized (this) {
			return objects.keySet().stream()
				.filter(key -> predicate.test(key))
				.map(key -> objects.get(key))
				.limit(limit)
				.collect(Collectors.toSet());
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
		}
	}

	@Override
	public void remove(final T entity) {
		synchronized (this) {
			mapper.apply(entity).forEach(key -> {
				objects.remove(key);
			});
		}
	}

	@Override
	public Set<K> get(Object key) {
		synchronized (this) {
			return Optional.ofNullable(objects.get(key))
			.map(s -> Collections.singleton(s))
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
			return objects.tailMap(key).values().stream().collect(Collectors.toSet());
		}
	}

}
