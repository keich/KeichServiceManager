package ru.keich.mon.servicemanager.eventrelation;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.query.predicates.Predicates;
import ru.keich.mon.servicemanager.store.IndexedHashMap;
import ru.keich.mon.servicemanager.store.IndexedHashMap.IndexType;

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

@Service
public class EventRelationService {
	protected final IndexedHashMap<EventRelationId, EventRelation> relationCache = new IndexedHashMap<>();
	
	static final String INDEX_NAME_RELATIONS_BY_EVENTID = "eventToRel";
	static final String INDEX_NAME_RELATIONS_BY_ITEMID = "itemToRel";
	static final String INDEX_NAME_RELATIONS_SORT_STATUS = "itemStatus";
	
	public EventRelationService() {
		relationCache.createIndex(INDEX_NAME_RELATIONS_BY_EVENTID, IndexType.EQUAL, EventRelation::getEventIdsForCache);
		relationCache.createIndex(INDEX_NAME_RELATIONS_BY_ITEMID, IndexType.EQUAL, EventRelation::getItemIdsForCache);
		relationCache.createIndex(INDEX_NAME_RELATIONS_SORT_STATUS, IndexType.UNIQ_SORTED, EventRelation::getItemStatusForCache);
	}
	
	public void add(Item item, Event event, BaseStatus status) {
		final var relation = new EventRelation(item.getId(), event.getId(), status);
		
		relationCache.transaction(() -> {
			relationCache.put(relation.getId(), () -> {
				return relation;
			}, old -> {
				if(old.getStatus() == relation.getStatus()) {
					return null;
				}
				return relation;
			}, addedRelation -> {

			});
			return null;
		});
	}
	
	private void remove(EventRelationId eventRelationId) {
		relationCache.remove(eventRelationId);
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
		var predicate = Predicates.equal(INDEX_NAME_RELATIONS_BY_EVENTID, event.getId());
		return relationCache.keySet(predicate, -1).stream()
				.map(EventRelationId::getItemId)
				.collect(Collectors.toUnmodifiableList());
	}
	
	public List<String> getEventIds(Item item) {
		var predicate = Predicates.equal(INDEX_NAME_RELATIONS_BY_ITEMID, item.getId());
		return relationCache.keySet(predicate, -1).stream()
				.map(EventRelationId::getEventId)
				.collect(Collectors.toUnmodifiableList());
	}
	
	public BaseStatus getMaxStatus(Item item) {
		var itemId = item.getId();
		var searchKey = new EventRelation(new EventRelationId(itemId,""), BaseStatus.CRITICAL);		
		return relationCache.keySetGreaterEqualFirst(INDEX_NAME_RELATIONS_SORT_STATUS, searchKey).stream()
				.filter(key -> key.getItemId().equals(itemId))
				.findFirst()
				.flatMap(key -> relationCache.get(key))
				.map(EventRelation::getStatus)
				.orElse(BaseStatus.CLEAR);
	}
	
}
