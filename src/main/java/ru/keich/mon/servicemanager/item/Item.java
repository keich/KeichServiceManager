package ru.keich.mon.servicemanager.item;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import lombok.Getter;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.SourceType;
import ru.keich.mon.servicemanager.entity.Entity;
import ru.keich.mon.servicemanager.query.predicates.QueryPredicate;

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

	public static final String FIELD_NAME = "name";
	public static final String FIELD_EVENTIDS = "events";
	public static final String FIELD_PARENTS = "parents";
	public static final String FIELD_FILTERS_EQL = "filters_equal";
	public static final String FIELD_STATUS = "status";
	public static final String FIELD_AGGSTATUS = "aggStatus";
	
	private final BaseStatus status;

	private final Map<String, ItemRule> rules;

	private final Map<String, ItemFilter> filters;

	private final Set<String> childrenIds;

	@JsonProperty(access = JsonProperty.Access.READ_ONLY)
	private final boolean hasChildren;
	
	private final String name;
	
	@JsonIgnore
	private Map<String, BaseStatus> eventsStatus = Collections.emptyMap();
	
	@JsonSerialize(using = AggregateStatusSerializer.class)
	@JsonProperty(access = JsonProperty.Access.READ_ONLY)
	private AggregateStatus aggStatus = new AggregateStatus(); 
	
	private List<Item> children;
	private List<Item> parents;
	
	@JsonCreator
	public Item(
			@JsonProperty(value = "id", required = true) String id,
			@JsonProperty(value = "version", required = false) Long version,
			@JsonProperty(value = "source", required = true) String source,
			@JsonProperty(value = "sourceKey", required = true) String sourceKey,
			@JsonProperty(value = "sourceType", required = false) SourceType sourceType,
			@JsonProperty("status") BaseStatus status,
			@JsonProperty("name") String name,
			@JsonProperty("fields") Map<String, String> fields,
			@JsonProperty("rules") Map<String, ItemRule> rules,
			@JsonProperty("filters") Map<String, ItemFilter> filters,
			@JsonProperty("childrenIds") Set<String> childrenIds,
			@JsonProperty("fromHistory") Set<String> fromHistory,
			@JsonProperty(value = "createdOn", required = false) Instant createdOn,
			@JsonProperty(value = "updatedOn", required = false) Instant updatedOn,
			@JsonProperty(value = "deletedOn", required = false) Instant deletedOn) {
		super(id, version, source, sourceKey, sourceType, fields, fromHistory, createdOn, updatedOn, deletedOn);

		this.name = name;
		this.status = Optional.ofNullable(status).orElse(BaseStatus.CLEAR);
		
		this.rules = Optional.ofNullable(rules).map(Collections::unmodifiableMap).orElse(Collections.emptyMap());
		this.filters = Optional.ofNullable(filters).map(Collections::unmodifiableMap).orElse(Collections.emptyMap());
		this.childrenIds = Optional.ofNullable(childrenIds).map(Collections::unmodifiableSet).orElse(Collections.emptySet());

		if(this.childrenIds.size() > 0) {
			this.hasChildren = true;
		}else {
			this.hasChildren = false;
		}
		
	}
	
	public Item(
			String id,
			Long version,
			String source,
			String sourceKey,
			SourceType sourceType,
			BaseStatus status,
			String name,
			Map<String, String> fields,
			Map<String, ItemRule> rules,
			Map<String, ItemFilter> filters,
			Set<String> childrenIds,
			Set<String> fromHistory,
			Instant createdOn,
			Instant updatedOn,
			Instant deletedOn,
			Map<String, BaseStatus> eventsStatus,
			AggregateStatus aggStatus,
			List<Item> children,
			List<Item> parents) {
		this(id, version, source, sourceKey, sourceType, status, name, fields, rules, filters, childrenIds, fromHistory, createdOn,
				updatedOn, deletedOn);

		this.eventsStatus = eventsStatus;
		
		this.children = children;
		
		this.parents = parents;
		
		this.aggStatus = Optional.ofNullable(aggStatus).orElse(new AggregateStatus());

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
	
	public static Set<Object> getNameForIndex(Item item) {
		return  Optional.ofNullable(item.getName())
				.map(String::toUpperCase)
				.map(s -> Collections.singleton((Object)s))
				.orElse(Collections.emptySet());
	}
	
	public static Set<Object> getEventsIdsForIndex(Item item) {
		return Collections.unmodifiableSet(item.getEventsStatus().keySet());
	}
	
	public static boolean getAggStatusForQuery(Item item, QueryPredicate predicate) {
		return predicate.test(item.getAggStatus().getMax());
	}
	
	public static Set<Object> getStatusForIndex(Item item) {
		return Collections.singleton(item.getStatus());
	}
	
	public static Object fieldValueOf(String fieldName, String str) {
		switch (fieldName) {
		case FIELD_NAME:
			return str.toUpperCase();
		case FIELD_STATUS:
		case FIELD_AGGSTATUS:
			return BaseStatus.fromString(str);
		}
		return Entity.fieldValueOf(fieldName, str);
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
	public static class Builder extends Entity.Builder<String, Item>  {
		
		
		@Override
		public String toString() {
			return "Builder [status=" + status + ", eventsStatus=" + eventsStatus + ", getId()=" + getId() + "]";
		}

		protected BaseStatus status;
		protected Map<String, ItemRule> rules;
		protected Map<String, ItemFilter> filters;
		protected Map<String, BaseStatus> eventsStatus;
		protected AggregateStatus aggStatus;
		protected Set<String> childrenIds;
		protected String name;
		protected List<Item> children;
		protected List<Item> parents;
		
		

		public Builder(String id) {
			super(id);
		}
		
		public Builder(Item item) {
			super(item);
			this.status = item.getStatus();
			this.name = item.getName();
			this.rules = item.getRules();
			this.filters = item.getFilters();
			this.childrenIds = item.getChildrenIds();
			this.eventsStatus = new HashMap<>(item.getEventsStatus());
			this.aggStatus = new AggregateStatus(item.getAggStatus());
			this.children = item.getChildren();
			this.parents = item.getParents();
		}
		
		@Override
		public Item build() {
			return new Item(this.id,
			this.version,
			this.source,
			this.sourceKey,
			this.sourceType,
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
			this.eventsStatus,
			this.aggStatus,
			children,
			parents);
		}
		
		public Builder name(String name) {
			this.name = name;
			this.changed = true;
			return this;
		}
		
		public Builder status(BaseStatus status) {
			if(this.status != status) {
				this.status = status;
				this.changed = true;
				aggStatus.set(status);
			}
			return this;
		}
		
		public Builder aggStatus(AggregateStatus aggStatus) {
			this.aggStatus = new AggregateStatus(aggStatus);
			this.changed = true;
			return this;
		}
		
		public Builder eventsStatus(Map<String, BaseStatus> eventsStatus) {
			this.eventsStatus.clear();
			this.eventsStatus.putAll(eventsStatus);
			this.changed = true;
			return this;
		}
		
		public Builder eventsStatusUpdate(Consumer<Map<String, BaseStatus>> s) {
			s.accept(eventsStatus);
			this.changed = true;
			return this;
		}
		
		public Builder setChildren(List<Item> children) {
			this.children = children;
			return this;
		}
		
		public Builder setParents(List<Item> parents) {
			this.parents = parents;
			return this;
		}
			
	}
	
}