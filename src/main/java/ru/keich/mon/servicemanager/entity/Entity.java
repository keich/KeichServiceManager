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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Getter;
import ru.keich.mon.servicemanager.store.BaseEntity;

@Getter
public class Entity<K> extends BaseEntity<K> {
	private Long version = 0L;
	private final String source;
	private final String sourceKey;

	private Instant createdOn = Instant.now();
	private Instant updatedOn = Instant.now();
	private Instant deletedOn;

	private Set<String> fromHistory = new HashSet<String>();
	protected final Map<String, String> fields;
	
	public Entity(K id, String source, String sourceKey, Map<String, String> fields) {
		super(id);
		this.source = source.intern();
		this.sourceKey = sourceKey.intern();
		if (Objects.nonNull(fields)) {
			this.fields = Collections.unmodifiableMap(fields);
		} else {
			this.fields = Collections.emptyMap();
		}
	}

	protected void setCreatedOn(Instant createdOn) {
		this.createdOn = createdOn;
	}

	protected void setUpdatedOn(Instant updatedOn) {
		this.updatedOn = updatedOn;
	}

	protected void setDeletedOn(Instant deletedOn) {
		this.deletedOn = deletedOn;
	}

	protected void setVersion(Long version) {
		this.version = version;
	}
	
	public static Set<Object> getSourceForIndex(Entity<?> entity){
		return Collections.singleton(entity.getSource());
	}
	
	public static Set<Object> getSourceKeyForIndex(Entity<?> entity){
		return Collections.singleton(entity.getSourceKey());
	}
	
	public static Set<Object> getVersionForIndex(Entity<?> entity){
		return Collections.singleton(entity.getVersion());
	}
	
	public static Set<Object> getDeletedOnForIndex(Entity<?> entity){
		if(Objects.isNull(entity.deletedOn)) {
			return Collections.emptySet();
		}
		return Collections.singleton(entity.getDeletedOn());
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
