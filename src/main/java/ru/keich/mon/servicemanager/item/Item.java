package ru.keich.mon.servicemanager.item;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import lombok.Getter;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.SourceType;
import ru.keich.mon.servicemanager.entity.Entity;
import ru.keich.mon.servicemanager.event.Event;
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
	public static final String FIELD_AGGSTATUS = "aggStatus";
	public static final String FIELD_EVENTS= "events";
	public static final String FIELD_EVENTSSTATUS= "eventsStatus";

	private final Map<String, ItemRule> rules;

	private final Map<String, ItemFilter> filters;

	private final Set<String> childrenIds;

	private final boolean hasChildren;
	
	private final String name;
	
	
	private final Map<String, BaseStatus> eventsStatus;
	
	@JsonSerialize(using = AggregateStatusSerializer.class)
	private final AggregateStatus aggStatus; 
	
	private final List<Item> children;
	
	private final List<Item> parents;
	
	private final List<Event> events;
	
	@JsonCreator
	public Item(
			@JsonProperty(value = "id", required = true) String id,
			@JsonProperty(value = "version") Long version,
			@JsonProperty(value = "source", required = true) String source,
			@JsonProperty(value = "sourceKey", required = true) String sourceKey,
			@JsonProperty(value = "sourceType") SourceType sourceType,
			@JsonProperty("status") BaseStatus status,
			@JsonProperty("name") String name,
			@JsonProperty("fields") Map<String, String> fields,
			@JsonProperty("rules") Map<String, ItemRule> rules,
			@JsonProperty("filters") Map<String, ItemFilter> filters,
			@JsonProperty("childrenIds") Set<String> childrenIds,
			@JsonProperty("fromHistory") Set<String> fromHistory,
			@JsonProperty(value = "createdOn") Instant createdOn,
			@JsonProperty(value = "updatedOn") Instant updatedOn,
			@JsonProperty(value = "deletedOn") Instant deletedOn,
			@JsonProperty(value = "eventsStatus") Map<String, BaseStatus> eventsStatus,
			@JsonProperty(value = "aggStatus", access = JsonProperty.Access.READ_ONLY) AggregateStatus aggStatus,
			@JsonProperty(value = "children") List<Item> children,
			@JsonProperty(value = "parents") List<Item> parents,
			@JsonProperty(value = "events") List<Event> events
			) {
		super(id, version, source, sourceKey, sourceType, fields, fromHistory, createdOn, updatedOn, deletedOn, status);

		this.name = name == null ? "" : name;
		
		this.rules = rules == null ? Collections.emptyMap() : Collections.unmodifiableMap(rules);
		this.filters = filters == null ? Collections.emptyMap() : Collections.unmodifiableMap(filters);
		this.childrenIds = childrenIds == null ? Collections.emptySet() : Collections.unmodifiableSet(childrenIds);
		this.hasChildren = this.childrenIds.size() > 0;
		this.eventsStatus = eventsStatus == null ? Collections.emptyMap() : Collections.unmodifiableMap(eventsStatus);
		this.children = children == null ? Collections.emptyList() : Collections.unmodifiableList(children);
		this.parents = parents == null ? Collections.emptyList() : Collections.unmodifiableList(parents);
		this.events = events == null ? Collections.emptyList() : Collections.unmodifiableList(events);
		this.aggStatus = aggStatus == null ? new AggregateStatus() : aggStatus;
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
	
	public static boolean testNameForQuery(Item item, QueryPredicate predicate) {
		return predicate.test(item.getName().toUpperCase());
	}
	
	public static Set<Object> getEventsIdsForIndex(Item item) {
		return Collections.unmodifiableSet(item.getEventsStatus().keySet());
	}
	
	public static boolean testAggStatusForQuery(Item item, QueryPredicate predicate) {
		return predicate.test(item.getAggStatus().getMax());
	}
	
	public static Object fieldValueOf(String fieldName, String str) {
		switch (fieldName) {
		case FIELD_NAME:
			return str.toUpperCase();
		case FIELD_AGGSTATUS:
			return BaseStatus.fromString(str);
		}
		return Entity.fieldValueOf(fieldName, str);
	}
	@Override
	public String toString() {
		return "Item [name=" + name + ", status=" + getStatus() + ", fields=" + getFields() + ", rules=" + rules + ", filters=" + filters
				+ ", getVersion()=" + getVersion() + ", getSource()=" + getSource() + ", getSourceKey()="
				+ getSourceKey() + ", getCreatedOn()=" + getCreatedOn() + ", getUpdatedOn()=" + getUpdatedOn()
				+ ", getDeletedOn()=" + getDeletedOn() + ", getFromHistory()=" + getFromHistory() + ", getId()="
				+ getId() + "]";
	}

	@Getter 
	public static class Builder extends Entity.Builder<String, Item>  {
		
		
		@Override
		public String toString() {
			return "Builder [status=" + status + ", getId()=" + getId() + "]";
		}

		protected Map<String, ItemRule> rules;
		protected Map<String, ItemFilter> filters;
		protected Map<String, BaseStatus> eventsStatus;
		protected AggregateStatus aggStatus;
		protected Set<String> childrenIds;
		protected String name;
		protected List<Item> children;
		protected List<Item> parents;
		protected List<Event> events;
		
		

		public Builder(String id) {
			super(id);
		}
		
		public Builder(Item item) {
			super(item);
			this.name = item.getName();
			this.rules = item.getRules();
			this.filters = item.getFilters();
			this.childrenIds = item.getChildrenIds();
			this.eventsStatus = new HashMap<>(item.getEventsStatus());
			this.children = item.getChildren();
			this.parents = item.getParents();
			this.events = item.getEvents();
			this.aggStatus = new AggregateStatus(item.getAggStatus());
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
			parents,
			events);
		}
		
		public Builder name(String name) {
			this.name = name;
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
		
		public Builder setEvents(List<Event> events) {
			this.events = events;
			return this;
		}

		@Override
		public Builder source(String source) {
			super.source(source);
			return this;
		}

		@Override
		public Builder sourceKey(String sourceKey) {
			super.sourceKey(sourceKey);
			return this;
		}

		@Override
		public Builder sourceType(SourceType sourceType) {
			super.sourceType(sourceType);
			return this;
		}

		@Override
		public Builder version(Long version) {
			super.version(version);
			return this;
		}

		@Override
		public Builder createdOn(Instant createdOn) {
			super.createdOn(createdOn);
			return this;
		}

		@Override
		public Builder updatedOn(Instant updatedOn) {
			super.updatedOn(updatedOn);
			return this;
		}

		@Override
		public Builder deletedOn(Instant deletedOn) {
			super.deletedOn(deletedOn);
			return this;
		}

		@Override
		public Builder fromHistory(Set<String> fromHistory) {
			super.fromHistory(fromHistory);
			return this;
		}

		@Override
		public Builder fromHistoryAdd(String value) {
			super.fromHistoryAdd(value);
			return this;
		}

		@Override
		public Builder fields(Map<String, String> fields) {
			super.fields(fields);
			return this;
		}		
		
		public Builder aggStatus(AggregateStatus aggStatus) {
			this.aggStatus = new AggregateStatus(aggStatus);
			this.changed = true;
			return this;
		}
		
		@Override
		public Builder status(BaseStatus status) {
			super.status(status);
			aggStatus.set(status);
			return this;
		}
		
			
	}
	
}