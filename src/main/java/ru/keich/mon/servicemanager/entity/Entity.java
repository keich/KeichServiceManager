package ru.keich.mon.servicemanager.entity;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonFilter;

import lombok.Getter;
import ru.keich.mon.servicemanager.SourceType;
import ru.keich.mon.servicemanager.store.BaseEntity;

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

@Getter
@JsonFilter("propertiesFilter")
public class Entity<K> extends BaseEntity<K> {
	
	public static final String FIELD_VERSION = "version";
	public static final String FIELD_CREATEDON = "createdOn";
	public static final String FIELD_UPDATEDON = "updatedOn";
	public static final String FIELD_DELETEDON = "deletedOn";
	public static final String FIELD_SOURCE = "source";
	public static final String FIELD_SOURCEKEY = "sourceKey";
	public static final String FIELD_SOURCETYPE = "sourceType";
	public static final String FIELD_FIELDS = "fields";
	public static final String FIELD_FROMHISTORY = "fromHistory";
	
	private final Long version;
	private final String source;
	private final String sourceKey;
	private final SourceType sourceType;

	private final Instant createdOn;
	private final Instant updatedOn;
	private final Instant deletedOn;

	private final Set<String> fromHistory;
	private final Map<String, String> fields;
	
	public Entity(K id,
			Long version,
			String source,
			String sourceKey,
			SourceType sourceType,
			Map<String, String> fields,
			Set<String> fromHistory,
			Instant createdOn,
			Instant updatedOn,
			Instant deletedOn) {
		super(id);
		this.version = version;
		this.source = source;
		this.sourceKey = sourceKey;
		this.sourceType = Optional.ofNullable(sourceType).orElse(SourceType.OTHER);
		this.fromHistory = Optional.ofNullable(fromHistory).map(Collections::unmodifiableSet).orElse(Collections.emptySet());
		this.createdOn = Optional.ofNullable(createdOn).orElse(Instant.now());
		this.updatedOn = Optional.ofNullable(updatedOn).orElse(Instant.now());
		this.deletedOn = deletedOn;
		this.fields = Stream.ofNullable(fields)
				.flatMap(f -> f.entrySet().stream())
				.collect(Collectors.toMap(e -> e.getKey().intern(), e -> e.getValue().intern()));
	}
	
	public static Set<Object> getSourceForIndex(Entity<?> entity) {
		return Collections.singleton(entity.getSource());
	}

	public static Set<Object> getSourceKeyForIndex(Entity<?> entity) {
		return Collections.singleton(entity.getSourceKey());
	}

	public static Set<Object> getVersionForIndex(Entity<?> entity) {
		return Collections.singleton(entity.getVersion());
	}
	
	public static Set<Object> getDeletedOnForIndex(Entity<?> entity) {
		return Optional.ofNullable((Object)entity.deletedOn).map(Collections::singleton).orElse(Collections.emptySet());
	}
	
	public static Set<Object> getUpdatedOnForIndex(Entity<?> entity) {
		return Optional.ofNullable((Object)entity.updatedOn).map(Collections::singleton).orElse(Collections.emptySet());
	}
	
	public static Set<Object> getCreatedOnForIndex(Entity<?> entity) {
		return Optional.ofNullable((Object)entity.createdOn).map(Collections::singleton).orElse(Collections.emptySet());
	}
	
	public static Set<Object> getFieldsForIndex(Entity<?> item) {
		return item.fields.entrySet().stream().collect(Collectors.toSet());
	}
	
	public static Set<Object> getFromHistoryForIndex(Entity<?> item) {
		return item.getFromHistory().stream().collect(Collectors.toSet());
	}

	@Getter
	public static abstract class Builder<K, B extends Entity<K>> {
		
		protected boolean changed = false;
		
		protected final K id;
		protected Long version;
		protected String source;
		protected String sourceKey;
		protected SourceType sourceType;
		protected Instant createdOn;
		protected Instant updatedOn;
		protected Instant deletedOn;
		protected Set<String> fromHistory;
		protected Map<String, String> fields;
		
		public Builder(K id) {
			this.id = id;
		}
		
		public Builder(B entity) {
			this.id = entity.getId();
			this.version = entity.getVersion();
			this.source = entity.getSource();
			this.sourceKey = entity.getSourceKey();
			this.sourceType = entity.getSourceType();
			this.fields = entity.getFields();
			this.createdOn = entity.getCreatedOn();
			this.updatedOn = entity.getUpdatedOn();
			this.deletedOn = entity.getDeletedOn();
			this.fromHistory = new HashSet<String>(entity.getFromHistory());
			this.fields = entity.getFields();
		}
		
		public abstract B build();
		
		public Builder<K, B> source(String source) {
			this.source = source;
			this.changed = true;
			return this;
		}
		
		public  Builder<K, B> sourceKey(String sourceKey) {
			this.sourceKey = sourceKey;
			this.changed = true;
			return this;
		}
		
		public  Builder<K, B> sourceType(SourceType sourceType) {
			this.sourceType = sourceType;
			this.changed = true;
			return this;
		}
		
		public Builder<K, B> version(Long version) {
			this.version = version;
			this.changed = true;
			return this;
		}

		public Builder<K, B> createdOn(Instant createdOn) {
			this.createdOn = createdOn;
			this.changed = true;
			return this;
		}
		
		public Builder<K, B> updatedOn(Instant updatedOn) {
			this.updatedOn = updatedOn;
			this.changed = true;
			return this;
		}

		public Builder<K, B> deletedOn(Instant deletedOn) {
			this.deletedOn = deletedOn;
			this.changed = true;
			return this;
		}

		public Builder<K, B> fromHistory(Set<String> fromHistory) {
			this.fromHistory.clear();
			this.fromHistory.addAll(fromHistory);
			this.changed = true;
			return this;
		}
		
		public Builder<K, B> fromHistoryAdd(String value) {
			this.fromHistory.add(value);
			this.changed = true;
			return this;
		}
		
		public Builder<K, B> fields(Map<String, String> fields) {
			this.fields = fields;
			this.changed = true;
			return this;
		}
		
		public boolean isNotDeleted() {
			return deletedOn == null;
		}
		
		public boolean isDeleted() {
			return deletedOn != null;
		}

	}
	
	public static Object fieldValueOf(String fieldName, String str) {
		switch (fieldName) {
		case FIELD_VERSION:
			return Long.valueOf(str);
		case FIELD_CREATEDON:
		case FIELD_UPDATEDON:
		case FIELD_DELETEDON:
			return Instant.parse(str);
		}
		return str;
	}
	
	public boolean isNotDeleted() {
		return deletedOn == null;
	}
	
	public boolean isDeleted() {
		return deletedOn != null;
	}
	
}
