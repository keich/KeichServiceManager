package ru.keich.mon.servicemanager.entity;

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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.scheduling.annotation.Scheduled;

import ru.keich.mon.servicemanager.store.IndexedHashMap;
import ru.keich.mon.servicemanager.store.IndexedHashMap.IndexType;

public class EntityService<K, T extends Entity<K>> {
	private Long incrementVersion = 1L;
	
	final protected IndexedHashMap<K, T> entityCache;
	final protected IndexedHashMap<K, T> entityCacheDeleted;
	
	final protected String nodeName;
	
	static final public String INDEX_NAME_VERSION= "version";
	static final public String INDEX_NAME_SOURCE = "source";
	static final public String INDEX_NAME_SOURCE_KEY = "source_key";
	static final public String INDEX_NAME_DELETED_ON = "deleted_on";
	static final public String INDEX_NAME_FIELDS = "fields";
	
	public EntityService(String nodeName) {
		this.nodeName = nodeName;
		entityCache = new IndexedHashMap<>();
		entityCacheDeleted = new IndexedHashMap<>();
		entityCacheDeleted.createIndex(INDEX_NAME_VERSION, IndexType.UNIQ_SORTED, Entity::getVersionForIndex);
		entityCacheDeleted.createIndex(INDEX_NAME_DELETED_ON, IndexType.SORTED, Entity::getDeletedOnForIndex);
		
		entityCache.createIndex(INDEX_NAME_VERSION, IndexType.UNIQ_SORTED, Entity::getVersionForIndex);
		entityCache.createIndex(INDEX_NAME_SOURCE, IndexType.EQUAL, Entity::getSourceForIndex);
		entityCache.createIndex(INDEX_NAME_SOURCE_KEY, IndexType.EQUAL, Entity::getSourceKeyForIndex);
		
		entityCache.createIndex(INDEX_NAME_FIELDS, IndexType.EQUAL, Entity::getFieldsForIndex);

	}
	
	protected void beforeInsert(T entity) {
		entity.getFromHistory().add(nodeName);
		entity.setVersion(incrementVersion);
		incrementVersion++;
	}
	
	protected void insertFirst(T entity) {

	}
	
	protected boolean insertExist(T old, T entity) {
		entity.setCreatedOn(old.getCreatedOn());
		if (old.hashCode() == entity.hashCode()) {
			if (old.equals(entity)) {
				if (old.getFromHistory().equals(entity.getFromHistory())) {
					return false;
				}
			}
		}
		return true;
	}
	
	protected void afterInsert(T entity) {

	}
	
	protected void afterInsertUnLock(T entity) {

	}
	
	protected void entityRemoved(T entity) {
		entityCacheDeleted.put(entity, e -> {
			
		}, e -> {
			e.setDeletedOn(Instant.now());
			e.setVersion(incrementVersion);
			incrementVersion++;
		}, (old, e) -> {
			return true;
		}, e -> {

		});
	}
	
	public void addOrUpdate(T entity) {
		entityCache.put(entity, e -> {
			beforeInsert(e);
		}, e -> {
			insertFirst(e);
		}, (old, e) -> {
			return insertExist(old, e);
		}, e -> {
			afterInsert(e);
		});
		afterInsertUnLock(entity);
	}
	
	public Optional<T> findById(K id) {
		return entityCache.get(id);
	}
	
	public Optional<T> deleteById(K entityId) {
		var opt = entityCache.remove(entityId);
		opt.ifPresent(entity -> entityRemoved(entity));
		return opt;
	}
	
	public List<T> deleteByIds(List<K> ids) {
		return entityCache.transaction(() ->  {
			return ids.stream()
				.map(id -> entityCache.remove(id))
				.filter(opt -> opt.isPresent())
				.map(opt -> {
					var entity = opt.get();
					entityRemoved(entity);
					return entity;
				})
				.collect(Collectors.toList());
		});
	}
	
	public List<T> deleteBySourceAndSourceKeyNot(String source, String sourceKey) {
		return entityCache.transaction(() ->  {
			var sourceIndex = entityCache.indexGet(INDEX_NAME_SOURCE, source);
			var sourceKeyIndex = entityCache.indexGet(INDEX_NAME_SOURCE_KEY, sourceKey);
			sourceIndex.removeAll(sourceKeyIndex);
			return sourceIndex.stream()
				.map(id -> entityCache.remove(id))
				.filter(opt -> opt.isPresent())
				.map(opt -> {
					var entity = opt.get();
					entityRemoved(entity);
					return entity;
				})
				.collect(Collectors.toList());
		});
	}

	public List<T> findByVersionGreaterThan(Long version) {
		return entityCache.transaction(() -> {
			var list = entityCache.indexGetAfter(INDEX_NAME_VERSION, version).stream()
					.map(id -> entityCache.get(id))
					.filter(opt -> opt.isPresent())
					.map(opt -> opt.get()).collect(Collectors.toList());
			
			var listDeleted = entityCacheDeleted.transaction(() -> {
				return entityCacheDeleted.indexGetAfter(INDEX_NAME_VERSION, version).stream()
						.map(id -> entityCacheDeleted.get(id))
						.filter(opt -> opt.isPresent())
						.map(opt -> opt.get()).collect(Collectors.toList());
					});
			
			list.addAll(listDeleted);
			return list;
		});
	}
	
	public List<T> findByFields(Map<String, String> fields) {
		return entityCache.transaction(() -> {
			return fields.keySet().stream()
			.flatMap(k -> entityCache.indexGet(INDEX_NAME_FIELDS, k).stream())
			.distinct()
			.map(id -> findById(id))
			.filter(o -> o.isPresent())
			.map(o -> o.get())
			.filter(item -> item.getFields().keySet().containsAll(fields.keySet()))
			.collect(Collectors.toList());
		});
	}
	
	//TODO to params
	@Scheduled(fixedRate = 60, timeUnit = TimeUnit.SECONDS)
	public void deleteOldScheduled() {
		entityCacheDeleted.transaction(() -> {
			entityCacheDeleted.indexGetBefore(INDEX_NAME_DELETED_ON, Instant.now().minusSeconds(3600)).stream()
			.forEach(id -> entityCacheDeleted.remove(id));
			return null;
		});
	}
}
