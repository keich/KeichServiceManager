package ru.keich.mon.servicemanager.item;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
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

	private BaseStatus status = BaseStatus.CLEAR;

	private final Map<String, ItemRule> rules;

	private final Map<String, ItemFilter> filters;

	private final Set<String> children;

	@JsonProperty(access = JsonProperty.Access.READ_ONLY)
	private final boolean hasChildren;
	
	private final String name;
	
	@JsonCreator
	public Item(
			@JsonProperty(value = "id", required = true) String id,
			@JsonProperty(value = "version", required = false) Long version,
			@JsonProperty(value = "source", required = true) String source,
			@JsonProperty(value = "sourceKey", required = true) String sourceKey,
			@JsonProperty("name") String name,
			@JsonProperty("fields") Map<String, String> fields,
			@JsonProperty("rules") Map<String, ItemRule> rules,
			@JsonProperty("filters") Map<String, ItemFilter> filters,
			@JsonProperty("children") Set<String> children,
			@JsonProperty("fromHistory") Set<String> fromHistory,
			@JsonProperty(value = "createdOn", required = false) Instant createdOn,
			@JsonProperty(value = "updatedOn", required = false) Instant updatedOn,
			@JsonProperty(value = "deletedOn", required = false) Instant deletedOn) {
		super(id, version, source, sourceKey, fields, fromHistory, createdOn, updatedOn, deletedOn);

		this.name = name;
		
		this.rules = Optional.ofNullable(rules).map(Collections::unmodifiableMap).orElse(Collections.emptyMap());
		this.filters = Optional.ofNullable(filters).map(Collections::unmodifiableMap).orElse(Collections.emptyMap());
		this.children = Optional.ofNullable(children).map(Collections::unmodifiableSet).orElse(Collections.emptySet());
		
		if(this.children.size() > 0) {
			this.hasChildren = true;
		}else {
			this.hasChildren = false;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 41;
		int result = super.hashCode();

		result = prime * result + getFields().size();

		result = prime * result + filters.size();

		result = prime * result + rules.size();

		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj))
			return false;
		Item other = (Item) obj;
		if(filters.size() != other.filters.size()) {
			return false;
		}
		if(rules.size() != other.rules.size()) {
			return false;
		}
		if(filters.size() != other.filters.size()) {
			return false;
		}

		return super.equals(other) && getFields().equals(other.getFields()) && rules.equals(other.rules)
				&& filters.equals(other.filters);
	}	
	
	protected void setStatus(BaseStatus status) {
		this.status = status;
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
		return Collections.unmodifiableSet(item.getChildren());
	}
	
	public static Set<Object> getNameUpperCaseForIndex(Entity<?> entity) {
		Item item = (Item)entity;
		return  Optional.ofNullable(item.getName())
				.map(String::toUpperCase)
				.map(s -> Collections.singleton((Object)s))
				.orElse(Collections.emptySet());
	}

	@Override
	public String toString() {
		return "Item [name=" + name + ", status=" + status + ", fields=" + getFields() + ", rules=" + rules + ", filters=" + filters
				+ ", getVersion()=" + getVersion() + ", getSource()=" + getSource() + ", getSourceKey()="
				+ getSourceKey() + ", getCreatedOn()=" + getCreatedOn() + ", getUpdatedOn()=" + getUpdatedOn()
				+ ", getDeletedOn()=" + getDeletedOn() + ", getFromHistory()=" + getFromHistory() + ", getId()="
				+ getId() + "]";
	}
	
}