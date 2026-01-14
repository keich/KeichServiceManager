package ru.keich.mon.servicemanager.entity;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Getter;
import ru.keich.mon.indexedhashmap.BaseStatus;
import ru.keich.mon.servicemanager.SourceType;

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
public class Entity<K> {
	
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
	public static final String FIELD_ID = "id";

	private final K id;
	
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
		
		this.id = id;
		this.version = version;
		this.source = source;
		this.sourceKey = sourceKey;
		this.sourceType = sourceType == null ? SourceType.OTHER : (sourceType);
		this.fromHistory = fromHistory == null ? Collections.emptySet() : Collections.unmodifiableSet(fromHistory);
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

	public static Set<Object> getSourceTypeForIndex(Entity<?> entity) {
		return Collections.singleton(entity.getSourceType());
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
	
	public static Object parseKeyValString(String str) {
		try {
			var newlinePos = str.indexOf(10);
			if (str.charAt(0) == '$' && newlinePos > 0) {
				var len = Integer.valueOf(str.substring(1, newlinePos));
				var key = str.substring(newlinePos + 1, newlinePos + 1 + len);
				var value = str.substring(newlinePos + 1 + len);
				return Map.entry(key, value);
			}
		} catch (Exception e) {
			// Ignore
		}
		return str;
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
		case FIELD_SOURCETYPE:
			return SourceType.valueOf(str);
		case FIELD_FIELDS:
			return parseKeyValString(str);
		}
		return str;
	}
	
	@JsonIgnore
	public boolean isNotDeleted() {
		return deletedOn == null;
	}
	
	@JsonIgnore
	public boolean isDeleted() {
		return deletedOn != null;
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		@SuppressWarnings("unchecked")
		Entity<K> other = (Entity<K>) obj;
		return Objects.equals(id, other.id);
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
			id = entity.getId();
			version = entity.getVersion();
			source = entity.getSource();
			sourceKey = entity.getSourceKey();
			sourceType = entity.getSourceType();
			fields = entity.getFields();
			createdOn = entity.getCreatedOn();
			updatedOn = entity.getUpdatedOn();
			deletedOn = entity.getDeletedOn();
			fromHistory = new HashSet<String>(entity.getFromHistory());
			fields = entity.getFields();
			status = entity.getStatus();
		}
		
		public abstract B build();
		
		public Builder<K, B> source(String source) {
			this.source = source;
			changed = true;
			return this;
		}
		
		public  Builder<K, B> sourceKey(String sourceKey) {
			this.sourceKey = sourceKey;
			changed = true;
			return this;
		}
		
		public  Builder<K, B> sourceType(SourceType sourceType) {
			this.sourceType = sourceType;
			changed = true;
			return this;
		}
		
		public Builder<K, B> version(Long version) {
			this.version = version;
			changed = true;
			return this;
		}

		public Builder<K, B> createdOn(Instant createdOn) {
			this.createdOn = createdOn;
			changed = true;
			return this;
		}
		
		public Builder<K, B> updatedOn(Instant updatedOn) {
			this.updatedOn = updatedOn;
			changed = true;
			return this;
		}

		public Builder<K, B> deletedOn(Instant deletedOn) {
			this.deletedOn = deletedOn;
			changed = true;
			return this;
		}

		public Builder<K, B> fromHistory(Set<String> fromHistory) {
			this.fromHistory.clear();
			this.fromHistory.addAll(fromHistory);
			changed = true;
			return this;
		}
		
		public Builder<K, B> fromHistoryAdd(String value) {
			this.fromHistory.add(value);
			changed = true;
			return this;
		}
		
		public Builder<K, B> fields(Map<String, String> fields) {
			this.fields = fields;
			changed = true;
			return this;
		}
		
		public Builder<K, B> status(BaseStatus status) {
			if(this.status != status) {
				this.status = status;
				changed = true;
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
