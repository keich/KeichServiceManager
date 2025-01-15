package ru.keich.mon.servicemanager.entity;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import io.micrometer.core.instrument.MeterRegistry;
import ru.keich.mon.servicemanager.QueueInfo;
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
	final protected QueueThreadReader<QueueInfo<K>> entityChangedQueue;
	
	final public String nodeName;

	public EntityService(String nodeName, MeterRegistry registry, Integer threadCount) {
		this.nodeName = nodeName;
		entityCache = new IndexedHashMap<>(registry, this.getClass().getSimpleName());
		entityChangedQueue = new QueueThreadReader<QueueInfo<K>>(this.getClass().getSimpleName(), threadCount, this::queueRead);
		
		entityCache.addIndex(Entity.FIELD_VERSION, IndexType.UNIQ_SORTED, Entity::getVersionForIndex);
		entityCache.addIndex(Entity.FIELD_SOURCE, IndexType.EQUAL, Entity::getSourceForIndex);
		entityCache.addIndex(Entity.FIELD_SOURCEKEY, IndexType.EQUAL, Entity::getSourceKeyForIndex);
		entityCache.addIndex(Entity.FIELD_DELETEDON, IndexType.SORTED, Entity::getDeletedOnForIndex);
		entityCache.addIndex(Entity.FIELD_CREATEDON, IndexType.SORTED, Entity::getCreatedOnForIndex);
		entityCache.addIndex(Entity.FIELD_UPDATEDON, IndexType.SORTED, Entity::getUpdatedOnForIndex);
		
		entityCache.addIndex(Entity.FIELD_FIELDS, IndexType.EQUAL, Entity::getFieldsForIndex);
		entityCache.addIndex(Entity.FIELD_FROMHISTORY, IndexType.EQUAL, Entity::getFromHistoryForIndex);
	}
	
	protected Long getNextVersion() {
		synchronized (this) {
			Long out = incrementVersion;
			incrementVersion++;
			return out;
		}
	}

	protected abstract void queueRead(QueueInfo<K> info);	
	
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
		var predicate = Predicates.equal(Entity.FIELD_SOURCE, source);
		var sourceIndex = entityCache.keySet(predicate, -1);
		predicate = Predicates.equal(Entity.FIELD_SOURCEKEY, sourceKey);
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
				.map(p -> entityCache.keySet(p, -1))
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
	
	@Value("${entity.delete.secondsold:30}") Long seconds;
	
	@Scheduled(fixedRateString = "${entity.delete.fixedrate:60}", timeUnit = TimeUnit.SECONDS)
	public void deleteOldScheduled() {
		var predicate = Predicates.lessThan(Entity.FIELD_DELETEDON, Instant.now().minusSeconds(seconds));
		entityCache.keySet(predicate, -1)
				.forEach(id -> entityCache.remove(id));
	}
	
}
