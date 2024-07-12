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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class IndexEqual<K, T extends BaseEntity<K>> implements Index<K, T> {
	private final Function<T, Set<Object>> mapper;
	private final Map<Object, Set<K>> objects = new HashMap<>();
	
	public IndexEqual(Function<T, Set<Object>> mapper) {
		this.mapper = mapper;
	}

	@Override
	public List<K> findByKey(long limit, Predicate<Object> predicate) {
		synchronized (this) {
			return objects.keySet().stream()
				.filter(key -> predicate.test(key))
				.flatMap(key -> objects.get(key).stream())
				.limit(limit)
				.collect(Collectors.toList());
		}
	}

	@Override
	public void append(T entity) {
		synchronized (this) {
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
	}

	@Override
	public void remove(final T entity) {
		synchronized (this) {
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
	}

	@Override
	public List<K> get(Object key) {
		synchronized (this) {
			return Optional.ofNullable(objects.get(key))
				.map(s -> s.stream().collect(Collectors.toList()))
				.orElse(Collections.emptyList());
		}
	}

	@Override
	public List<K> getBefore(Object key) {
		throw new UnsupportedOperationException("Equal index has't this method");
	}

	@Override
	public List<K> getAfter(Object key) {
		throw new UnsupportedOperationException("Equal index has't this method");
	}

}
