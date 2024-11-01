package ru.keich.mon.servicemanager.item;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.entity.Entity;

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
public class Item extends Entity<String> {

	private final BaseStatus status;

	private final Map<String, ItemRule> rules;

	private final Map<String, ItemFilter> filters;

	private final Set<String> childrenIds;

	@JsonProperty(access = JsonProperty.Access.READ_ONLY)
	private final boolean hasChildren;
	
	private final String name;
	
	@JsonIgnore
	private final Map<String, BaseStatus> eventsStatus;
	
	@JsonCreator
	public Item(
			@JsonProperty(value = "id", required = true) String id,
			@JsonProperty(value = "version", required = false) Long version,
			@JsonProperty(value = "source", required = true) String source,
			@JsonProperty(value = "sourceKey", required = true) String sourceKey,
			@JsonProperty("status") BaseStatus status,
			@JsonProperty("name") String name,
			@JsonProperty("fields") Map<String, String> fields,
			@JsonProperty("rules") Map<String, ItemRule> rules,
			@JsonProperty("filters") Map<String, ItemFilter> filters,
			@JsonProperty("childrenIds") Set<String> childrenIds,
			@JsonProperty("fromHistory") Set<String> fromHistory,
			@JsonProperty(value = "createdOn", required = false) Instant createdOn,
			@JsonProperty(value = "updatedOn", required = false) Instant updatedOn,
			@JsonProperty(value = "deletedOn", required = false) Instant deletedOn,
			@JsonProperty(value = "eventsStatus") Map<String, BaseStatus> eventsStatus) {
		super(id, version, source, sourceKey, fields, fromHistory, createdOn, updatedOn, deletedOn);

		this.name = name;
		this.status = Optional.ofNullable(status).orElse(BaseStatus.CLEAR);
		
		this.rules = Optional.ofNullable(rules).map(Collections::unmodifiableMap).orElse(Collections.emptyMap());
		this.filters = Optional.ofNullable(filters).map(Collections::unmodifiableMap).orElse(Collections.emptyMap());
		this.childrenIds = Optional.ofNullable(childrenIds).map(Collections::unmodifiableSet).orElse(Collections.emptySet());
		
		this.eventsStatus = Optional.ofNullable(eventsStatus).map(Collections::unmodifiableMap).orElse(Collections.emptyMap());
		
		if(this.childrenIds.size() > 0) {
			this.hasChildren = true;
		}else {
			this.hasChildren = false;
		}
	}	

	public static Set<Object> getFiltersForIndex(Item item) {
		return item.filters.entrySet().stream()
				.map(Map.Entry::getValue)
				.map(ItemFilter::getEqualFields)
		  		.map(Map::entrySet)
				.flatMap(Set::stream)
				.collect(Collectors.toSet());
	}
	
	public static Set<Object> getParentsForIndex(Item item) {
		return Collections.unmodifiableSet(item.getChildrenIds());
	}
	
	public static Set<Object> getNameUpperCaseForIndex(Entity<?> entity) {
		Item item = (Item)entity;
		return  Optional.ofNullable(item.getName())
				.map(String::toUpperCase)
				.map(s -> Collections.singleton((Object)s))
				.orElse(Collections.emptySet());
	}
	
	public static Set<Object> getEventsIndex(Item item) {
		return Collections.unmodifiableSet(item.getEventsStatus().keySet());
	}

	@Override
	public String toString() {
		return "Item [name=" + name + ", status=" + status + ", fields=" + getFields() + ", rules=" + rules + ", filters=" + filters
				+ ", getVersion()=" + getVersion() + ", getSource()=" + getSource() + ", getSourceKey()="
				+ getSourceKey() + ", getCreatedOn()=" + getCreatedOn() + ", getUpdatedOn()=" + getUpdatedOn()
				+ ", getDeletedOn()=" + getDeletedOn() + ", getFromHistory()=" + getFromHistory() + ", getId()="
				+ getId() + "]";
	}

	@Getter
	public static class Builder {
		protected final String id;
		protected Long version;
		protected String source;
		protected String sourceKey;
		protected BaseStatus status;
		protected Instant createdOn;
		protected Instant updatedOn;
		protected Instant deletedOn;
		protected Set<String> fromHistory;
		protected Map<String, String> fields;
		protected Map<String, ItemRule> rules;
		protected Map<String, ItemFilter> filters;
		protected Map<String, BaseStatus> eventsStatus;
		protected Set<String> childrenIds;
		protected String name;
		
		protected boolean changed = false;

		public Builder(String id) {
			this.id = id;
		}
		
		public Builder(Item item) {
			this.id = item.getId();
			this.version = item.getVersion();
			this.source = item.getSource();
			this.sourceKey = item.getSourceKey();
			this.status = item.getStatus();
			this.name = item.getName();
			this.fields = item.getFields();
			this.rules = item.getRules();
			this.filters = item.getFilters();
			this.childrenIds = item.getChildrenIds();
			this.fromHistory = item.getFromHistory();
			this.createdOn = item.getCreatedOn();
			this.updatedOn = item.getUpdatedOn();
			this.deletedOn = item.getDeletedOn();
			this.eventsStatus = item.getEventsStatus();
		}
		
		public Item build() {
			return new Item(this.id,
			this.version,
			this.source,
			this.sourceKey,
			this.status,
			this.name,
			this.fields,
			this.rules,
			this.filters,
			this.childrenIds,
			this.fromHistory,
			this.createdOn,
			this.updatedOn,
			this.deletedOn,
			this.eventsStatus);
		}

		public Builder source(String source) {
			this.source = source;
			this.changed = true;
			return this;
		}
		
		public Builder sourceKey(String sourceKey) {
			this.sourceKey = sourceKey;
			this.changed = true;
			return this;
		}
		
		public Builder name(String name) {
			this.name = name;
			this.changed = true;
			return this;
		}
		
		public Builder version(Long version) {
			this.version = version;
			this.changed = true;
			return this;
		}

		public Builder createdOn(Instant createdOn) {
			this.createdOn = createdOn;
			this.changed = true;
			return this;
		}
		
		public Builder updatedOn(Instant updatedOn) {
			this.updatedOn = updatedOn;
			this.changed = true;
			return this;
		}

		public Builder deletedOn(Instant deletedOn) {
			this.deletedOn = deletedOn;
			this.changed = true;
			return this;
		}

		public Builder fromHistory(Set<String> fromHistory) {
			this.fromHistory = fromHistory;
			this.changed = true;
			return this;
		}
		
		public Builder status(BaseStatus status) {
			if(this.status != status) {
				this.status = status;
				this.changed = true;
			}
			return this;
		}
		
		public Builder eventsStatus(Map<String, BaseStatus> eventsStatus) {
			this.eventsStatus = Collections.unmodifiableMap(eventsStatus);
			this.changed = true;
			return this;
		}
		
	}
	
}