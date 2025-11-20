package ru.keich.mon.servicemanager.entity;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import io.micrometer.core.instrument.MeterRegistry;
import ru.keich.mon.indexedhashmap.IndexedHashMap;
import ru.keich.mon.indexedhashmap.IndexedHashMap.IndexType;
import ru.keich.mon.indexedhashmap.query.Operator;
import ru.keich.mon.indexedhashmap.query.QuerySort;
import ru.keich.mon.indexedhashmap.query.predicates.Predicates;
import ru.keich.mon.indexedhashmap.query.predicates.QueryPredicate;
import ru.keich.mon.servicemanager.QueueInfo;
import ru.keich.mon.servicemanager.QueueThreadReader;
import ru.keich.mon.servicemanager.item.Item;

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
	static public final Long VERSION_MIN = 0L;
	
	private AtomicLong incrementVersion = new AtomicLong(VERSION_MIN + 1);
	
	final protected IndexedHashMap<K, T> entityCache;
	final protected QueueThreadReader<QueueInfo<K>> entityChangedQueue;
	
	final public String nodeName;

	public EntityService(String nodeName, MeterRegistry registry, Integer threadCount) {
		this.nodeName = nodeName;
		entityCache = new IndexedHashMap<>(registry, this.getClass().getSimpleName());
		entityChangedQueue = new QueueThreadReader<QueueInfo<K>>(registry, this.getClass().getSimpleName(), threadCount, this::queueRead);
		
		entityCache.addIndexLongUniq(Entity.FIELD_VERSION, Entity::getVersionForIndex);
		entityCache.addIndex(Entity.FIELD_SOURCE, IndexType.EQUAL, Entity::getSourceForIndex);
		entityCache.addIndex(Entity.FIELD_SOURCEKEY, IndexType.EQUAL, Entity::getSourceKeyForIndex);
		entityCache.addIndex(Entity.FIELD_SOURCETYPE, IndexType.EQUAL, Entity::getSourceTypeForIndex);
		entityCache.addIndex(Entity.FIELD_DELETEDON, IndexType.SORTED, Entity::getDeletedOnForIndex);
		entityCache.addIndex(Entity.FIELD_CREATEDON, IndexType.SORTED, Entity::getCreatedOnForIndex);
		entityCache.addIndex(Entity.FIELD_UPDATEDON, IndexType.SORTED, Entity::getUpdatedOnForIndex);
		entityCache.addIndexStatus(Item.FIELD_STATUS, Entity::getStatusForIndex);
		
		entityCache.addIndex(Entity.FIELD_FIELDS, IndexType.EQUAL, Entity::getFieldsForIndex);
		entityCache.addIndex(Entity.FIELD_FROMHISTORY, IndexType.EQUAL, Entity::getFromHistoryForIndex);
	}
	
	protected Long getNextVersion() {
		return incrementVersion.incrementAndGet();
	}

	protected abstract void queueRead(QueueInfo<K> info);	
	
	public abstract void addOrUpdate(T entity);
	
	public abstract Optional<T> deleteById(K entityId);
	
	public Optional<T> findById(K id) {
		return Optional.ofNullable(entityCache.get(id));
	}
	
	public List<T> findByIds(Set<K> ids) {
		return entityCache.get(ids);
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
		var sourceIndex = entityCache.keySet(predicate);
		predicate = Predicates.equal(Entity.FIELD_SOURCEKEY, sourceKey);
		var sourceKeyIndex = entityCache.keySet(predicate);
		sourceIndex.removeAll(sourceKeyIndex);
		return sourceIndex.stream()
				.map(this::deleteById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
	}
	
	public Stream<T> query(List<QueryPredicate> predicates) {
		return predicates.stream()
				.map(p -> entityCache.keySet(p))
				.reduce((result, el) -> { 
					result.retainAll(el);
					return result;
				})
				.orElse(Collections.emptySet())
				.stream()
				.map(entityCache::get)
				.filter(Objects::nonNull);
	}
	
	@Value("${entity.delete.secondsold:30}") Long seconds;
	
	@Scheduled(fixedRateString = "${entity.delete.fixedrate:60}", timeUnit = TimeUnit.SECONDS)
	public void deleteOldScheduled() {
		var predicate = Predicates.lessThan(Entity.FIELD_DELETEDON, Instant.now().minusSeconds(seconds));
		entityCache.keySet(predicate).forEach(entityCache::remove);
	}
	
	public Comparator<T> getSortComparator(QuerySort sort) {
		final int mult = sort.getOperator() == Operator.SORTDESC ? -1 : 1;
		switch (sort.getName()) {
		case Entity.FIELD_STATUS:
			return (e1, e2) -> e1.getStatus().compareTo(e2.getStatus()) * mult;
		case Entity.FIELD_CREATEDON:
			return (e1, e2) -> e1.getCreatedOn().compareTo(e2.getCreatedOn()) * mult;
		case Entity.FIELD_UPDATEDON:
			return (e1, e2) -> e1.getUpdatedOn().compareTo(e2.getUpdatedOn()) * mult;
		case Entity.FIELD_DELETEDON:
			return (e1, e2) -> e1.getDeletedOn().compareTo(e2.getDeletedOn()) * mult;
		case Entity.FIELD_SOURCE:
			return (e1, e2) -> e1.getSource().compareTo(e2.getSource()) * mult;
		case Entity.FIELD_SOURCEKEY:
			return (e1, e2) -> e1.getSourceKey().compareTo(e2.getSourceKey()) * mult;
		case Entity.FIELD_SOURCETYPE:
			return (e1, e2) -> e1.getSourceType().compareTo(e2.getSourceType()) * mult;
		}
		return (e1, e2) -> {
			return e1.getVersion().compareTo(e2.getVersion()) * mult;
		};
	}
	
	public Stream<T> sortAndLimit(Stream<T> data, List<QuerySort> sorts, long limit) {
		if(!sorts.isEmpty()) {
			var comparator = sorts.stream()
					.map(s -> getSortComparator(s))
					.reduce(Comparator::thenComparing)
					.orElse((e1, e2) -> 0);
			data = data.sorted(comparator);
		}
		if (limit > 0) {
			return data.limit(limit);
		}
		return data;
	}
	
}
