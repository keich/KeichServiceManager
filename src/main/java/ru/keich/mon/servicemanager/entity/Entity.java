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
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.SourceType;
import ru.keich.mon.servicemanager.event.Event;
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
	public static final String FIELD_STATUS = "status";
	
	private final Long version;
	private final String source;
	private final String sourceKey;
	private final SourceType sourceType;

	private final BaseStatus status;
	
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
			Instant deletedOn,
			BaseStatus status) {
		super(id);
		this.version = version;
		this.source = source;
		this.sourceKey = sourceKey;
		this.sourceType = sourceType == null ? SourceType.OTHER : (sourceType);
		this.fromHistory = Optional.ofNullable(fromHistory).map(Collections::unmodifiableSet).orElse(Collections.emptySet());
		this.createdOn = createdOn == null ? Instant.now() : createdOn;
		this.updatedOn = updatedOn == null ? Instant.now() : updatedOn;
		this.deletedOn = deletedOn;
		this.fields = Stream.ofNullable(fields)
				.flatMap(f -> f.entrySet().stream())
				.collect(Collectors.toMap(e -> e.getKey().intern(), e -> e.getValue().intern()));
		this.status = status == null ? BaseStatus.CLEAR : status;
	}
	
	public static Set<Object> getSourceForIndex(Entity<?> entity) {
		return Collections.singleton(entity.getSource());
	}

	public static Set<Object> getSourceKeyForIndex(Entity<?> entity) {
		return Collections.singleton(entity.getSourceKey());
	}

	public static Long getVersionForIndex(Entity<?> entity) {
		return entity.getVersion();
	}
	
	public static Set<Object> getDeletedOnForIndex(Entity<?> entity) {
		return entity.deletedOn == null ? Collections.emptySet() : Collections.singleton(entity.deletedOn);
	}
	
	public static Set<Object> getUpdatedOnForIndex(Entity<?> entity) {
		return entity.updatedOn == null ? Collections.emptySet() : Collections.singleton(entity.updatedOn);
	}
	
	public static Set<Object> getCreatedOnForIndex(Entity<?> entity) {
		return entity.createdOn == null ? Collections.emptySet() : Collections.singleton(entity.createdOn);
	}
	
	public static Set<Object> getFieldsForIndex(Entity<?> entity) {
		return entity.fields.entrySet().stream().collect(Collectors.toSet());
	}
	
	public static Set<Object> getFromHistoryForIndex(Entity<?> entity) {
		return entity.getFromHistory().stream().collect(Collectors.toSet());
	}
	
	public static BaseStatus getStatusForIndex(Entity<?> entity) {
		return entity.getStatus();
	}
	
	public static Object fieldValueOf(String fieldName, String str) {
		switch (fieldName) {
		case FIELD_VERSION:
			return Long.valueOf(str);
		case FIELD_CREATEDON:
		case FIELD_UPDATEDON:
		case FIELD_DELETEDON:
			return Instant.parse(str);
		case FIELD_STATUS:
			return BaseStatus.fromString(str);
		}
		return str;
	}
	
	public boolean isNotDeleted() {
		return deletedOn == null;
	}
	
	public boolean isDeleted() {
		return deletedOn != null;
	}

	@Getter
	public static abstract class Builder<K, B extends Entity<K>> {
		
		protected boolean changed = false;
		
		protected final K id;
		protected Long version;
		protected String source;
		protected String sourceKey;
		protected SourceType sourceType;
		protected BaseStatus status;
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
			this.status = entity.getStatus();
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
		
		public Builder<K, B> status(BaseStatus status) {
			if(this.status != status) {
				this.status = status;
				this.changed = true;
			}
			return this;
		}
		
		public boolean isNotDeleted() {
			return deletedOn == null;
		}
		
		public boolean isDeleted() {
			return deletedOn != null;
		}

	}
	
}
