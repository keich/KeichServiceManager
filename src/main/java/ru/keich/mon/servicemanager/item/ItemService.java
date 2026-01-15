package ru.keich.mon.servicemanager.item;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.java.Log;
import ru.keich.mon.indexedhashmap.query.Operator;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.QueueInfo;
import ru.keich.mon.servicemanager.entity.EntityService;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.event.EventService;
import ru.keich.mon.servicemanager.query.QuerySort;


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
@Log
public class ItemService extends EntityService<String, Item> {
	
	private final EventService eventService;
	
	public ItemService(@Value("${replication.nodename}") String nodeName
			,EventService eventService
			,MeterRegistry registry
			,@Value("${item.thread.count:2}") Integer threadCount
			,@Value("${item.aggstatus.seconds:60}") Long aggStatusSeconds) {
		super(nodeName, registry, threadCount);
		
		AggregateStatus.setSeconds(aggStatusSeconds);
		
		entityCache.addIndexEqual(Item.FIELD_FILTERS_EQL, Item::getFiltersForIndex);
		entityCache.addIndexEqual(Item.FIELD_PARENTS, Item::getParentsForIndex);

		entityCache.addIndexEqual(Item.FIELD_EVENTIDS, Item::getEventsIdsForIndex);
		
		entityCache.addQueryField(Item.FIELD_AGGSTATUS, Item::getAggStatusForQuery);
		entityCache.addQueryField(Item.FIELD_NAME, Item::getNameForQuery);
		
		this.eventService = eventService;
		eventService.setItemService(this);
	}

	@Override
	public void addOrUpdate(Item item) {
		entityCache.compute(item.getId(), (k, oldItem) -> {
			var eventsStatus = item.getEventsStatus();
			var status = item.getStatus();
			var aggStatus = item.getAggStatus();
			var createdOn = item.getCreatedOn();
			Instant deletedOn = item.isDeleted() ? Instant.now() : null;
			if(oldItem != null) {
				eventsStatus = oldItem.getEventsStatus();
				status = oldItem.getStatus();
				aggStatus = oldItem.getAggStatus();
				createdOn = oldItem.getCreatedOn();
			}
			entityChangedQueue.add(new QueueInfo<String>(item.getId(), QueueInfo.QueueInfoType.UPDATE));
			return new Item.Builder(item)
					.eventsStatus(eventsStatus)
					.status(status)
					.aggStatus(aggStatus)
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
					.createdOn(createdOn)
					.updatedOn(Instant.now())
					.deletedOn(deletedOn)
					.build();
		});

	}

	@Override
	public Optional<Item> deleteById(String itemId) {
		return Optional.ofNullable(entityCache.computeIfPresent(itemId,  (k, item) -> {
			if (item.isDeleted()) {
				return item;
			}
			entityChangedQueue.add(new QueueInfo<String>(itemId, QueueInfo.QueueInfoType.UPDATE));
			return new Item.Builder(item)
					.version(getNextVersion())
					.fromHistory(Collections.singleton(nodeName))
					.updatedOn(Instant.now())
					.deletedOn(Instant.now())
					.build();
		}));
	}

	@Override
	protected void queueRead(QueueInfo<String> info) {
		switch(info.getType()) {
		case UPDATE:
			entityCache.computeIfPresent(info.getId(), (id, item) -> {
				var newStatus = BaseStatus.CLEAR;
				if(item.isNotDeleted() && !item.isMaintenanceOn()) {
					newStatus = calculateStatus(item);
				}
				if(item.getStatus() != newStatus) {
					entityChangedQueue.add(new QueueInfo<String>(id, QueueInfo.QueueInfoType.UPDATED));
					return new Item.Builder(item).status(newStatus).build();
				}
				return item;
			});
			break;
		case UPDATED:
			entityCache.computeIfPresent(info.getId(), (id, item) -> {
				findParentIdsById(info.getId())
						.forEach(parentId -> entityChangedQueue.add(new QueueInfo<String>(parentId, QueueInfo.QueueInfoType.UPDATE)));
				return item;
			});
			break;
		default:
			break;
		}
	}

	public void itemUpdateEventsStatus(String itemId, Consumer<Map<String, BaseStatus>> s) {
		entityCache.computeIfPresent(itemId, (k, item) -> {
			entityChangedQueue.add(new QueueInfo<String>(itemId, QueueInfo.QueueInfoType.UPDATE));
			return new Item.Builder(item).eventsStatusUpdate(s).build();
		});
	}

	public void eventRemoved(Event event) {
		entityCache.keySetIndexEq(Item.FIELD_EVENTIDS, event.getId())
				.forEach(itemId -> itemUpdateEventsStatus(itemId, m -> m.remove(event.getId())));
	}

	public void eventChanged(Event event) {
		findFiltersByEqualFields(event.getFields())
				.forEach(itft -> {
					var itemId = itft.getKey().getId();
					var newStatus = itft.getValue().getStatus(event);
					itemUpdateEventsStatus(itemId, m -> m.put(event.getId(), newStatus));
				});
	}

	private BaseStatus calculateStatusByChild(Item parent) {	
		var rules = parent.getRules().values();
		var statuses = findChildren(parent)
				.map(Item::getStatus)
				.toList();
		if(rules.isEmpty()) {
			return ItemRule.doDefault(statuses);
		}
		return ItemRule.max(rules, statuses);
	}

	private BaseStatus calculateStatus(Item item) {
		var statusByChild = calculateStatusByChild(item);
		var statusByEvents = BaseStatus.max(item.getEventsStatus().values());
		return statusByChild.max(statusByEvents);
	}

	private Stream<Map.Entry<Item, ItemFilter>> findFiltersByEqualFields(Map<String, String> fields) {
		return fields.entrySet().stream()
				.map(e -> entityCache.keySetIndexEq(Item.FIELD_FILTERS_EQL, e))
				.map(this::findByIds)
				.flatMap(List::stream)
				.filter(Item::isNotDeleted)
				.map(item -> {
					return item.getFilters().entrySet().stream()
							.filter(flt -> fields.entrySet().containsAll(flt.getValue().getEqualFields().entrySet()))
							.findFirst()
							.map(e -> Map.entry(item, e.getValue()));
				})
				.filter(Optional::isPresent)
				.map(Optional::get);
	}

	private Set<String> findParentIdsById(String itemId) {
		return entityCache.keySetIndexEq(Item.FIELD_PARENTS, itemId);
	}

	private Set<String> findParentIds(Item item) {
		return findParentIdsById(item.getId());
	}

	public Optional<Stream<Item>> findParentsById(String itemId) {
		return findById(itemId)
				.map(this::findParentIds)
				.map(this::findByIds)
				.map(List::stream);
	}

	public Stream<Item> findChildren(Item parent) {
		return findByIds(parent.getChildrenIds()).stream()
				.filter(Item::isNotDeleted);	
	}

	public Stream<Item> findParents(Item child) {
		return findByIds(findParentIds(child)).stream()
				.filter(Item::isNotDeleted);	
	}

	public Optional<Stream<Item>> findChildrenById(String id) {
		return findById(id)
				.map(this::findChildren);
	}

	private Set<String> findAllChildrenById(String parentId) {
		var queue = new LinkedList<String>();
		var history = new HashSet<String>();
		queue.add(parentId);
		while(!queue.isEmpty()) {
			var id = queue.poll();
			if(history.contains(id)) {
				continue;
			}
			findById(id)
					.filter(Item::isNotDeleted)
					.ifPresent(parent -> {
						history.add(id);
						queue.addAll(parent.getChildrenIds());
					});
		}
		return history;
	}

	public Stream<Event> findAllEventsById(String id) {
		 var ids = findByIds(findAllChildrenById(id)).stream()
				.map(Item::getEventsStatus)
				.map(Map::keySet)
				.flatMap(Set::stream)
				.collect(Collectors.toSet());
		
		 return eventService.findByIds(ids).stream()
				 .filter(Event::isNotDeleted);
	}

	@Override
	public Comparator<Item> getSortComparator(QuerySort sort) {
		final int mult = sort.getOperator() == Operator.SORTDESC ? -1 : 1;
		switch (sort.getName()) {
		case Item.FIELD_NAME:
			return (e1, e2) -> e1.getName().compareTo(e2.getName()) * mult;
		}
		return super.getSortComparator(sort);
	}

}
