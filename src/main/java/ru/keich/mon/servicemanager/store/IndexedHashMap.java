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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class IndexedHashMap<K, T extends BaseEntity<K>> {

	public enum IndexType {
		EQUAL, SORTED, UNIQ_SORTED
	}
	
	private Map<K, T> cache = new ConcurrentHashMap<>();
	private Map<String , Index<K, T>> index =  new HashMap<>(); 

	public IndexedHashMap() {
		super();
	}
	
	public void createIndex(String name, IndexType type, Function<T, Set<Object>> mapper) {
		switch(type) {
		case EQUAL:
			index.put(name, new IndexEqual<K,T>(mapper));
			break;
		case SORTED:
			index.put(name, new IndexSorted<K,T>(mapper));
			break;
		case UNIQ_SORTED:
			index.put(name, new IndexSortedUniq<K,T>(mapper));
			break;		
		}
	}
	
	public Set<K> indexGet(String name,Object key) {
		return index.get(name).get(key);
	}
	
	public Set<K> findByKey(String name, long limit, Predicate<Object> predicate) {
		return index.get(name).findByKey(limit, predicate);
	}
	
	public Set<K> indexGetAfter(String name,Object key) {
		return index.get(name).getAfter(key);
	}
	
	public Set<K> indexGetBefore(String name,Object key) {
		return index.get(name).getBefore(key);
	}
	
	public void put(K entityId, Supplier<T> insertTrigger, Function<T,T> updateTrigger, Consumer<T> after) {
		synchronized (this) {
			Optional.ofNullable(cache.get(entityId)).ifPresentOrElse(old -> {
				final var entity = updateTrigger.apply(old);
				if(Objects.nonNull(entity)) {
					final var oldEntity = cache.put(entityId, entity);
					index.entrySet().forEach(e -> {
						e.getValue().remove(oldEntity);
						e.getValue().append(entity);
					});
					after.accept(entity);
				}
			}, () -> {
				final var entity = insertTrigger.get();
				if(Objects.nonNull(entity)) {
					cache.put(entityId, entity);
					index.entrySet().forEach(e -> {
						e.getValue().append(entity);
					});
					after.accept(entity);
				}
			});
		}
	}

	public Optional<T> get(K id) {
		return Optional.ofNullable(cache.get(id));
	}

	public Optional<T> remove(K id) {
		return Optional.ofNullable(cache.remove(id)).map(oldEntity -> {
			index.entrySet().forEach(e -> {
				e.getValue().remove(oldEntity);
			});
			return oldEntity;
		});
	}
	
	public <R> R transaction(Supplier<R> supplier) {
		synchronized (this) {
			return supplier.get();
		}
	}
	
}