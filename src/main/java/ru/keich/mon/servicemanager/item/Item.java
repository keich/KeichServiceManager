package ru.keich.mon.servicemanager.item;

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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.entity.Entity;


@Getter
public class Item extends Entity<String> {

	private BaseStatus status;

	private final Map<String, String> fields;

	private final Map<String, ItemRule> rules;

	private final Map<String, ItemFilter> filters;

	private final Set<String> children;

	@JsonProperty(access = JsonProperty.Access.READ_ONLY)
	private final boolean hasChildren;
	
	@JsonCreator
	public Item(
			@JsonProperty(value = "id", required = true) String id,
			@JsonProperty(value = "source", required = true) String source,
			@JsonProperty(value = "sourceKey", required = true) String sourceKey,
			@JsonProperty("fields") Map<String, String> fields,
			@JsonProperty("rules") Map<String, ItemRule> rules,
			@JsonProperty("filters") Map<String, ItemFilter> filters,
			@JsonProperty("children") Set<String> children) {
		super(id, source, sourceKey);

		if (Objects.nonNull(fields)) {
			this.fields = Collections.unmodifiableMap(fields);
		} else {
			this.fields = Collections.emptyMap();
		}
		if (Objects.nonNull(rules)) {
			this.rules = Collections.unmodifiableMap(rules);
		} else {
			this.rules = Collections.emptyMap();
		}
		if (Objects.nonNull(filters)) {
			this.filters = Collections.unmodifiableMap(filters);
		} else {
			this.filters = Collections.emptyMap();
		}
		if (Objects.nonNull(children) && !children.isEmpty()) {
			this.children = Collections.unmodifiableSet(children);
			this.hasChildren = true;
		} else {
			this.children = Collections.emptySet();
			this.hasChildren = false;
		}

	}

	@Override
	public int hashCode() {
		final int prime = 41;
		int result = super.hashCode();

		result = prime * result + fields.size();

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

		return super.equals(other) 
				&& fields.equals(other.fields) 
				&& rules.equals(other.rules) 
				&& filters.equals(other.filters);
	}	
	
	protected void setStatus(BaseStatus status) {
		this.status = status;
	}

	public static Set<Object> getFiltersForIndex(Item item) {
		return item.filters.entrySet().stream()
			.flatMap(e -> e.getValue().getEqualFields().entrySet().stream())
			.collect(Collectors.toSet());
	}
	
	public static Set<Object> getParentsForIndex(Item item) {
		return Collections.unmodifiableSet(item.getChildren());
	}

	public Optional<ItemFilter> findFiltersByEqualFields(Map<String, String> fields) {
		for(var entry: filters.entrySet()) {
			boolean match = true;
			for(var filterField: entry.getValue().getEqualFields().entrySet()) {
				var fieldName = filterField.getKey();
				var fieldValue = filterField.getValue();
				if(!fields.containsKey(fieldName)) {
					match = false;
					break;
				}
				if (!fields.get(fieldName).equals(fieldValue)) {
					match = false;
					break;
				}
			}
			if(match) {
				return Optional.of(entry.getValue());
			}
		}
		return Optional.empty();
	}

	@Override
	public String toString() {
		return "Item [status=" + status + ", fields=" + fields + ", rules=" + rules + ", filters=" + filters
				+ ", getVersion()=" + getVersion() + ", getSource()=" + getSource() + ", getSourceKey()="
				+ getSourceKey() + ", getCreatedOn()=" + getCreatedOn() + ", getUpdatedOn()=" + getUpdatedOn()
				+ ", getDeletedOn()=" + getDeletedOn() + ", getFromHistory()=" + getFromHistory() + ", getId()="
				+ getId() + "]";
	}
	
}