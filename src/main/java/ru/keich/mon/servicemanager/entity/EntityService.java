package ru.keich.mon.servicemanager.entity;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.scheduling.annotation.Scheduled;

import io.micrometer.core.instrument.MeterRegistry;
import ru.keich.mon.servicemanager.QueueThreadReader;
import ru.keich.mon.servicemanager.query.predicates.Predicates;
import ru.keich.mon.servicemanager.query.predicates.QueryPredicate;
import ru.keich.mon.servicemanager.store.IndexedHashMap;
import ru.keich.mon.servicemanager.store.IndexedHashMap.IndexType;

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

public abstract class EntityService<K, T extends Entity<K>> {
	static public Long VERSION_MIN = 0L;
	
	private Long incrementVersion = VERSION_MIN + 1;
	
	final protected IndexedHashMap<K, T> entityCache;
	final protected QueueThreadReader<K> entityChangedQueue;
	
	final public String nodeName;
	
	static final public String INDEX_NAME_VERSION= "version";
	static final public String INDEX_NAME_SOURCE = "source";
	static final public String INDEX_NAME_SOURCE_KEY = "source_key";
	static final public String INDEX_NAME_DELETED_ON = "deleted_on";
	static final public String INDEX_NAME_FIELDS = "fields";
	static final public String INDEX_NAME_FROMHISTORY = "fromHistory";

	public EntityService(String nodeName, MeterRegistry registry) {
		this.nodeName = nodeName;
		entityCache = new IndexedHashMap<>(registry, this.getClass().getSimpleName());
		entityChangedQueue = new QueueThreadReader<K>(this.getClass().getSimpleName() + "-entityChangedQueue" , 4, this::entityChanged);
		
		entityCache.createIndex(INDEX_NAME_VERSION, IndexType.UNIQ_SORTED, Entity::getVersionForIndex);
		entityCache.createIndex(INDEX_NAME_SOURCE, IndexType.EQUAL, Entity::getSourceForIndex);
		entityCache.createIndex(INDEX_NAME_SOURCE_KEY, IndexType.EQUAL, Entity::getSourceKeyForIndex);
		entityCache.createIndex(INDEX_NAME_DELETED_ON, IndexType.SORTED, Entity::getDeletedOnForIndex);
		
		entityCache.createIndex(INDEX_NAME_FIELDS, IndexType.EQUAL, Entity::getFieldsForIndex);
		entityCache.createIndex(INDEX_NAME_FROMHISTORY, IndexType.EQUAL, Entity::getFromHistoryForIndex);
	}
	
	protected Long getNextVersion() {
		synchronized (this) {
			Long out = incrementVersion;
			incrementVersion++;
			return out;
		}
	}

	protected abstract void entityChanged(K entityId);	
	
	public abstract void addOrUpdate(T entity);
	
	public abstract Optional<T> deleteById(K entityId);
	
	public Optional<T> findById(K id) {
		return entityCache.get(id);
	}
	
	public List<T> deleteByIds(List<K> ids) {
		return ids.stream()
				.map(this::deleteById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
	}
	
	public List<T> deleteBySourceAndSourceKeyNot(String source, String sourceKey) {
		var predicate = Predicates.equal(INDEX_NAME_SOURCE, source);
		var sourceIndex = entityCache.keySet(predicate, -1);
		predicate = Predicates.equal(INDEX_NAME_SOURCE_KEY, sourceKey);
		var sourceKeyIndex = entityCache.keySet(predicate, -1);
		sourceIndex.removeAll(sourceKeyIndex);
		return sourceIndex.stream()
				.map(this::deleteById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
	}
	
	public <V extends Comparable<V>> List<T> query(List<QueryPredicate> predicates) {
		return predicates.stream()
				.map(p -> {
					return entityCache.keySet(p, -1);
				})
				.reduce((result, el) -> { 
					result.retainAll(el);
					return result;
				})
				.orElse(Collections.emptySet())
				.stream()
				.map(entityCache::get)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
	}
	
	@Scheduled(fixedRateString = "${entity.delete.fixedrate:60}", timeUnit = TimeUnit.SECONDS)
	public void deleteOldScheduled() {
		var predicate = Predicates.lessThan(INDEX_NAME_DELETED_ON, Instant.now().minusSeconds(30));
		entityCache.keySet(predicate, -1).stream()
				.forEach(id -> entityCache.remove(id));
	}
	
}
