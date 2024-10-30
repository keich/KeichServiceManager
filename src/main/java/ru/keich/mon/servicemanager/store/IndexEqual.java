package ru.keich.mon.servicemanager.store;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

public class IndexEqual<K, T extends BaseEntity<K>> implements Index<K, T> {
	private final Function<T, Set<Object>> mapper;
	private final Map<Object, Set<K>> objects = new HashMap<>();
	
	public IndexEqual(Function<T, Set<Object>> mapper) {
		this.mapper = mapper;
	}

	@Override
	public Set<K> findByKey(long limit, Predicate<Object> predicate) {
		synchronized (this) {
			var stream = objects.entrySet().stream()
					.filter(entry -> {
							return predicate.test(entry.getKey());
						})
					.map(Map.Entry::getValue)
					.flatMap(Set::stream);
			if(limit > 0) {
				return stream.limit(limit).collect(Collectors.toSet());
			}
			return stream.collect(Collectors.toSet());
		}
	}

	@Override
	public void append(T entity) {
		synchronized (this) {
			mapper.apply(entity).forEach(key -> {
				var set = Optional.ofNullable(objects.get(key)).orElse(new HashSet<>());
				set.add(entity.getId());
				objects.put(key, set);
			});
		}
	}

	@Override
	public void remove(final T entity) {
		synchronized (this) {
			mapper.apply(entity).forEach(key -> {
				Optional.ofNullable(objects.get(key)).ifPresent(set -> {
					set.remove(entity.getId());
					if (set.size() == 0) {
						objects.remove(key);
					}
				});
			});
		}
	}

	@Override
	public Set<K> get(Object key) {
		synchronized (this) {
			return Optional.ofNullable(objects.get(key)).stream().flatMap(s -> s.stream()).collect(Collectors.toSet());
		}
	}

	@Override
	public Set<K> getBefore(Object key) {
		throw new UnsupportedOperationException("Equal index has't this method");
	}

	@Override
	public Set<K> getAfter(Object key) {
		throw new UnsupportedOperationException("Equal index has't this method");
	}

	@Override
	public Set<K> getAfterFirst(Object key) {
		throw new UnsupportedOperationException("Equal index has't this method");
	}

	@Override
	public Set<K> valueSet() {
		return objects.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
	}

}
