package ru.keich.mon.servicemanager.eventrelation;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.store.IndexedHashMap;
import ru.keich.mon.servicemanager.store.IndexedHashMap.IndexType;

@Service
public class EventRelationService {
	protected final IndexedHashMap<EventRelationId, EventRelation> relationCache = new IndexedHashMap<>();
	
	private Map<String, String> maxStatusEventId = new HashMap<>();
	
	static final String INDEX_NAME_RELATIONS_BY_EVENTID = "eventToRel";
	static final String INDEX_NAME_RELATIONS_BY_ITEMID = "itemToRel";
	
	public EventRelationService() {
		relationCache.createIndex(INDEX_NAME_RELATIONS_BY_EVENTID, IndexType.EQUAL, EventRelation::getEventIdsForCache);
		relationCache.createIndex(INDEX_NAME_RELATIONS_BY_ITEMID, IndexType.EQUAL, EventRelation::getItemIdsForCache);
	}
	
	private String findEventIdWithMaxStatus(String itemId) {
		return relationCache.indexGet(INDEX_NAME_RELATIONS_BY_ITEMID, itemId).stream()
				.map(id -> relationCache.get(id))
				.filter(opt -> opt.isPresent())
				.map(opt -> opt.get())
				.map(rel -> Map.entry(rel.getId().getEventId(), rel.getStatus()))
				.reduce(Map.entry("", BaseStatus.CLEAR), (acc, rel) -> {
					if(acc.getValue().lessThenOrEqual(rel.getValue())) {
						acc = rel;
					}
					return acc;
				}).getKey();
	}
	
	private void addCalculateMaxStatus(Item item, Event event, BaseStatus newStatus) {
		final var itemId = item.getId();
		final var eventId = event.getId();
		Optional.ofNullable(maxStatusEventId.get(itemId))
		.ifPresentOrElse(maxEventId -> {
			var opt =  relationCache.get(new EventRelationId(itemId, eventId));
			var oldStatus = opt.map(rel -> rel.getStatus()).orElse(BaseStatus.CLEAR);

			if (maxEventId.equals(eventId)) {
				if(oldStatus.lessThenOrEqual(newStatus)) {
					return;
				}
			}

			opt =  relationCache.get(new EventRelationId(itemId, maxEventId));
			var maxStatus = opt.map(rel -> rel.getStatus()).orElse(BaseStatus.CLEAR);
			
			if(newStatus.lessThenOrEqual(maxStatus)) {
				return;
			}
			maxStatusEventId.put(itemId, findEventIdWithMaxStatus(itemId));
		}, () -> maxStatusEventId.put(itemId, eventId));
	}
	
	private void removeCalculateMaxStatus(String itemId, String eventId) {
		Optional.ofNullable(maxStatusEventId.get(itemId))
		.ifPresent(maxEventId -> {
			if(maxEventId.equals(eventId)) {
				maxStatusEventId.put(itemId, findEventIdWithMaxStatus(itemId));
			}
		});
	}
	
	public void add(Item item, Event event, BaseStatus status) {
		var relation = new EventRelation(item.getId(), event.getId(), status);
		relationCache.put(relation, r -> {
			
		},r -> {
			
		}, (oldRelation, newRelation) -> {
			if(oldRelation.getStatus() == newRelation.getStatus()) {
				return false;
			}
			return true;
		},r -> {
			addCalculateMaxStatus(item, event, status);
		});
	}
	
	private void remove(EventRelationId eventRelationId) {
		relationCache.remove(eventRelationId);
		final var itemId = eventRelationId.getItemId();
		if(relationCache.indexGet(INDEX_NAME_RELATIONS_BY_ITEMID, itemId).size() == 0){
			maxStatusEventId.remove(itemId);//TODO add max in index?
			return;
		}
		final var eventId = eventRelationId.getEventId();
		removeCalculateMaxStatus(itemId, eventId);
		return;
	}
	
	public void itemRemoved(Item item) {
		final var itemId = item.getId();
		relationCache.transaction(() -> {
			getEventIds(item).forEach(eventId -> {
				remove(new EventRelationId(itemId, eventId));
			});
			return null;
		});
	}
	
	public void eventRemoved(Event event) {
		final var eventId = event.getId();
		relationCache.transaction(() -> {
			getItemIds(event).forEach(itemId ->{
				remove(new EventRelationId(itemId, eventId));
			});
			return null;
		});
	}
	
	public List<String> getItemIds(Event event) {
		return relationCache.indexGet(INDEX_NAME_RELATIONS_BY_EVENTID, event.getId()).stream()
			.map(id -> id.getItemId())
			.collect(Collectors.toUnmodifiableList());
	}
	
	public List<String> getEventIds(Item item) {
		return relationCache.indexGet(INDEX_NAME_RELATIONS_BY_ITEMID, item.getId()).stream()
			.map(id -> id.getEventId())
			.collect(Collectors.toUnmodifiableList());
	}
	
	public BaseStatus getMaxStatus(Item item) {
		final var itemId = item.getId();
		return relationCache.transaction(() -> {
			return Optional.ofNullable(maxStatusEventId.get(itemId))
			.map(eventId -> relationCache.get(new EventRelationId(itemId, eventId)))
			.filter(o -> o.isPresent())
			.map(o -> o.get().getStatus())
			.orElse(BaseStatus.CLEAR);
		});
	}
	
}
