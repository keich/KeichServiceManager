package ru.keich.mon.servicemanager.entity;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonFilter;

import lombok.Getter;
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
	private final Long version;
	private final String source;
	private final String sourceKey;

	private final Instant createdOn;
	private final Instant updatedOn;
	private final Instant deletedOn;

	private final Set<String> fromHistory;
	private final Map<String, String> fields;
	
	public Entity(K id,
			Long version,
			String source,
			String sourceKey,
			Map<String, String> fields,
			Set<String> fromHistory,
			Instant createdOn,
			Instant updatedOn,
			Instant deletedOn) {
		super(id);
		this.version = version;
		this.source = source;
		this.sourceKey = sourceKey;
		this.fromHistory = Optional.ofNullable(fromHistory).map(Collections::unmodifiableSet).orElse(Collections.emptySet());
		this.createdOn = Optional.ofNullable(createdOn).orElse(Instant.now());
		this.updatedOn = Optional.ofNullable(updatedOn).orElse(Instant.now());
		this.deletedOn = Optional.ofNullable(deletedOn).orElse(null);
		this.fields = Optional.ofNullable(fields).map(Collections::unmodifiableMap).orElse(Collections.emptyMap());
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
	
	public static Set<Object> getFieldsForIndex(Entity<?> item) {
		return item.fields.entrySet().stream().collect(Collectors.toSet());
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + source.hashCode();
		result = prime * result + sourceKey.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj))
			return false;
		var other = (Entity<?>) obj;
		return super.equals(other) && source.equals(other.source) && sourceKey.equals(other.sourceKey);
	}

}
