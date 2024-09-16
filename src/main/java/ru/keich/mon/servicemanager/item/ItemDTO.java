package ru.keich.mon.servicemanager.item;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Getter;
import lombok.Setter;
import ru.keich.mon.servicemanager.BaseStatus;

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
@Setter
@JsonFilter("propertiesFilter")
public class ItemDTO {
	private String id;
	private Long version;
	private String source;
	private String sourceKey;
	private String name;
	private BaseStatus status;
	private Map<String, String> fields;
	private Map<String, ItemRule> rules;
	private Map<String, ItemFilter> filters;
	
	@JsonIgnore
	private Set<String> childrenIds;
	private List<ItemDTO> children = Collections.emptyList();
	
	private List<ItemDTO> parents = Collections.emptyList();
	
	private boolean hasChildren;
	
	private Instant createdOn;
	private Instant updatedOn;
	private Instant deletedOn;
	
	public ItemDTO() {
		
	}
	
	public ItemDTO(Item item) {
		this.id = item.getId();
		this.version = item.getVersion();
		this.source = item.getSource();
		this.sourceKey = item.getSourceKey();
		this.name = item.getName();
		this.status = item.getStatus();
		this.fields = item.getFields();
		this.rules = item.getRules();
		this.filters = item.getFilters();
		this.childrenIds = item.getChildren();
		this.hasChildren = item.isHasChildren();
		this.createdOn = item.getCreatedOn();
		this.updatedOn = item.getUpdatedOn();
		this.deletedOn = item.getDeletedOn();
	}

}
