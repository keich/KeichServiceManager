package ru.keich.mon.servicemanager.event;

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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import ru.keich.mon.servicemanager.entity.EntityService;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.item.ItemFilter;
import ru.keich.mon.servicemanager.item.ItemService;

@Service
public class EventService extends EntityService<String, Event>{

	private ItemService itemService;
	
	public void setItemService(ItemService itemService) {
		this.itemService = itemService;
	}

	public EventService(@Value("${replication.nodename}") String nodeName) {
		super(nodeName);
	}
	
	@Override
	public void addOrUpdate(Event event) {
		entityCache.transaction(() -> {
			final var newFromHistory = new HashSet<String>();
			newFromHistory.addAll(event.getFromHistory());
			newFromHistory.add(nodeName);
			entityCache.put(event.getId(), () -> {
				// TODO temporary
				var summary = event.getSummary();
				if(Objects.isNull(summary)) {
					var fieldsSummary = event.getFields().get("summary");
					if(Objects.nonNull(fieldsSummary)) {
						summary = fieldsSummary;
					}
				}
				var node = event.getNode();
				if(Objects.isNull(node)) {
					var fieldsNode = event.getFields().get("node");
					if(Objects.nonNull(fieldsNode)) {
						node = fieldsNode;
					}
				}
				return new Event(event.getId(),
						getNextVersion(),
						event.getSource(),
						event.getSourceKey(),
						node,
						summary,
						event.getType(),
						event.getStatus(),
						event.getFields(),
						newFromHistory,
						event.getCreatedOn(),
						event.getUpdatedOn(),
						event.getDeletedOn());				
			}, old -> {
				if (isEntityEqual(old, event)) {
					return null;
				}
				return new Event(event.getId(),
						getNextVersion(),
						event.getSource(),
						event.getSourceKey(),
						event.getNode(),
						event.getSummary(),
						event.getType(),
						event.getStatus(),
						event.getFields(),
						newFromHistory,
						old.getCreatedOn(),
						Instant.now(),
						event.getDeletedOn());
			}, addedEvent -> {
				itemService.eventAdded(addedEvent);
			});
			return null;
		});
	}

	@Override
	protected Event entityRemoved(Event event) {
		super.entityRemoved(event);
		itemService.eventRemoved(event);

		
		final var newFromHistory = Collections.singleton(nodeName);
		var deletedEvent= new Event(event.getId(),
				getNextVersion(),
				event.getSource(),
				event.getSourceKey(),
				event.getNode(),
				event.getSummary(),
				event.getType(),
				event.getStatus(),
				event.getFields(),
				newFromHistory,
				event.getCreatedOn(),
				Instant.now(),
				Instant.now());

		entityCache.put(event.getId(), () -> {
			return deletedEvent;
		}, old -> {
			if(Objects.nonNull(old.getDeletedOn())) {
				return null;
			}
			return deletedEvent;
		}, addedItem -> {});
		
		return event;
	}

	public void itemAdded(Item item){
		entityCache.transaction(() -> {
			item.getFilters().entrySet().stream()
					.map(Map.Entry::getValue)
					.map(ItemFilter::getEqualFields)
			  		.map(this::findByFields)
					.flatMap(List::stream)
					.forEach(itemService::eventAdded);
			return null;
		});
	}
}
