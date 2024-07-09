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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import ru.keich.mon.servicemanager.query.Filter;
import ru.keich.mon.servicemanager.query.Operator;
import ru.keich.mon.servicemanager.query.QueryId;
import ru.keich.mon.servicemanager.store.IndexedHashMap;
import ru.keich.mon.servicemanager.store.IndexedHashMap.IndexType;

public abstract class EntityService<K, T extends Entity<K>> {
	static public Long VERSION_MIN = 0L;
	
	static public QueryId PRODUCER_ID_ALL = new QueryId("", Operator.ALL);
	
	private Long incrementVersion = VERSION_MIN + 1;
	
	final protected IndexedHashMap<K, T> entityCache;
	
	final public String nodeName;
	
	static final public String INDEX_NAME_VERSION= "version";
	static final public String INDEX_NAME_SOURCE = "source";
	static final public String INDEX_NAME_SOURCE_KEY = "source_key";
	static final public String INDEX_NAME_DELETED_ON = "deleted_on";
	static final public String INDEX_NAME_FIELDS = "fields";
	
	final protected Map<QueryId, Function<String, List<K>>> queryProducer = new HashMap<QueryId, Function<String, List<K>>>();
	final protected Map<QueryId, BiFunction<T, String, Boolean>> queryFilter = new HashMap<QueryId, BiFunction<T, String, Boolean>>();
	
	public EntityService(String nodeName) {
		this.nodeName = nodeName;
		entityCache = new IndexedHashMap<>();
		
		entityCache.createIndex(INDEX_NAME_VERSION, IndexType.UNIQ_SORTED, Entity::getVersionForIndex);
		entityCache.createIndex(INDEX_NAME_SOURCE, IndexType.EQUAL, Entity::getSourceForIndex);
		entityCache.createIndex(INDEX_NAME_SOURCE_KEY, IndexType.EQUAL, Entity::getSourceKeyForIndex);
		entityCache.createIndex(INDEX_NAME_DELETED_ON, IndexType.SORTED, Entity::getDeletedOnForIndex);
		
		entityCache.createIndex(INDEX_NAME_FIELDS, IndexType.EQUAL, Entity::getFieldsForIndex);
		
		queryProducer.put(PRODUCER_ID_ALL, value -> {
			return entityCache.indexGetAfter(INDEX_NAME_VERSION, VERSION_MIN);
		});
		
		queryProducer.put(new QueryId("source", Operator.EQ), source -> {
			return entityCache.indexGet(INDEX_NAME_SOURCE, source);
		});
		
		queryFilter.put(new QueryId("source", Operator.NE), (entity, value)  -> {
			return !entity.getSource().equals(value);
		});
		
		queryProducer.put(new QueryId("sourceKey", Operator.EQ), sourceKey -> {
			return entityCache.indexGet(INDEX_NAME_SOURCE_KEY, sourceKey);
		});
		
		queryFilter.put(new QueryId("sourceKey", Operator.NE), (entity, value)  -> {
			return !entity.getSourceKey().equals(value);
		});
		
		queryProducer.put(new QueryId("version", Operator.GT), strVersion -> {
			var version = Long.valueOf(strVersion);
			return entityCache.indexGetAfter(INDEX_NAME_VERSION, version);
		});
		
		queryProducer.put(new QueryId("version", Operator.LT), strVersion -> {
			var version = Long.valueOf(strVersion);
			return entityCache.indexGetBefore(INDEX_NAME_VERSION, version);
		});
		
		queryProducer.put(new QueryId("version", Operator.EQ), strVersion -> {
			var version = Long.valueOf(strVersion);
			return entityCache.indexGet(INDEX_NAME_VERSION, version);
		});
		
		queryFilter.put(new QueryId("version", Operator.NE), (entity, strVersion)  -> {
			var version = Long.valueOf(strVersion);
			return entity.getVersion() != version;
		});
		
		queryProducer.put(new QueryId("deletedOn", Operator.GT), strDateTime -> {
			var datetime =  Instant.parse(strDateTime);
			return entityCache.indexGetAfter(INDEX_NAME_DELETED_ON, datetime);
		});
		
		queryProducer.put(new QueryId("deletedOn", Operator.LT), strDateTime -> {
			var datetime =  Instant.parse(strDateTime);
			return entityCache.indexGetBefore(INDEX_NAME_DELETED_ON, datetime);
		});
		
		queryProducer.put(new QueryId("deletedOn", Operator.EQ), strDateTime -> {
			var datetime =  Instant.parse(strDateTime);
			return entityCache.indexGet(INDEX_NAME_DELETED_ON, datetime);
		});
		
		queryFilter.put(new QueryId("deletedOn", Operator.NE), (entity, strDateTime)  -> {
			var createdOn = entity.getCreatedOn();
			if(Objects.isNull(createdOn)) {
				return true;
			}
			var datetime =  Instant.parse(strDateTime);
			return createdOn.equals(datetime);
		});
		
		queryFilter.put(new QueryId("fromHistory", Operator.CO), (entity, value)  -> {
			return entity.getFromHistory().contains(value);
		});
		
		queryFilter.put(new QueryId("fromHistory", Operator.NC), (entity, value)  -> {
			return !entity.getFromHistory().contains(value);
		});
	}
	
	protected Long getNextVersion() {
		synchronized (this) {
			Long out = incrementVersion;
			incrementVersion++;
			return out;
		}
	}
	
	protected boolean isEntityEqual(T old, T entity) {
		if (old.hashCode() == entity.hashCode()) {
			if (old.equals(entity)) {
					return true;
			}
		}
		return false;
	}
	
	protected void entityRemoved(T entity) {

	}
	
	public abstract void addOrUpdate(T entity);
	
	public Optional<T> findById(K id) {
		return entityCache.get(id);
	}
	
	public Optional<T> deleteById(K entityId) {
		return entityCache.transaction(() -> {
			var opt = entityCache.remove(entityId);
			opt.ifPresent(entity -> entityRemoved(entity));
			return opt;
		});
	}
	
	public List<T> deleteByIds(List<K> ids) {
		return entityCache.transaction(() ->  {
			return ids.stream()
				.map(id -> deleteById(id))
				.filter(opt -> opt.isPresent())
				.map(opt -> opt.get())
				.collect(Collectors.toList());
		});
	}
	
	public List<T> deleteBySourceAndSourceKeyNot(String source, String sourceKey) {
		return entityCache.transaction(() ->  {
			var sourceIndex = entityCache.indexGet(INDEX_NAME_SOURCE, source);
			var sourceKeyIndex = entityCache.indexGet(INDEX_NAME_SOURCE_KEY, sourceKey);
			sourceIndex.removeAll(sourceKeyIndex);
			return sourceIndex.stream()
				.map(id -> deleteById(id))
				.filter(opt -> opt.isPresent())
				.map(opt -> opt.get())
				.collect(Collectors.toList());
		});
	}
	
	public List<T> findByFields(Map<String, String> fields) {
		return entityCache.transaction(() -> {
			return fields.entrySet().stream()
			.flatMap(k -> entityCache.indexGet(INDEX_NAME_FIELDS, k).stream())
			.distinct()
			.map(id -> findById(id))
			.filter(o -> o.isPresent())
			.map(o -> o.get())
			.filter(item -> item.getFields().keySet().containsAll(fields.keySet()))
			.collect(Collectors.toList());
		});
	}
	
	// And function only
	public List<T> query(List<Filter> filters) {
		// TODO error for undefined filters
		var producers = filters.stream()
		.filter(f -> queryProducer.containsKey(f.getId()))
		.collect(Collectors.toList());
		
		if(producers.size() == 0) {
			producers = Collections.singletonList(new Filter(PRODUCER_ID_ALL,""));
		}
		
		var opt = producers.stream().map(f -> {
			return new HashSet<>(queryProducer.get(f.getId()).apply(f.getValue()));
		})
		.reduce((result, el) -> { 
			result.retainAll(el);
			return result;
		});
		
		if(opt.isEmpty()) {
			return Collections.emptyList();
		}
		
		var out = opt.get().stream()
		.map(id -> entityCache.get(id))
		.filter(o -> o.isPresent())
		.map(o -> o.get())
		.collect(Collectors.toList());
		
		filters.stream()
		.filter(f -> queryFilter.containsKey(f.getId()))
		.forEach(f -> {
			out.removeIf(entity -> !queryFilter.get(f.getId()).apply(entity, f.getValue()));
		});		
		return out;
	}
	
}
