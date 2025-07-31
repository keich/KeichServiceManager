package ru.keich.mon.servicemanager.item;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.QueueInfo;
import ru.keich.mon.servicemanager.entity.EntityService;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.event.EventService;
import ru.keich.mon.servicemanager.query.predicates.Predicates;
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
@Log
public class ItemService extends EntityService<String, Item> {
	
	private final EventService eventService;
	private final ItemHistoryService itemHistoryService;
	
	public ItemService(@Value("${replication.nodename}") String nodeName
			,EventService eventService
			,ItemHistoryService itemHistoryService
			,MeterRegistry registry
			,@Value("${item.thread.count:2}") Integer threadCount) {
		super(nodeName, registry, threadCount);

		entityCache.addIndex(Item.FIELD_FILTERS_EQL, IndexType.EQUAL, Item::getFiltersForIndex);
		entityCache.addIndex(Item.FIELD_PARENTS, IndexType.EQUAL, Item::getParentsForIndex);
		
		entityCache.addIndex(Item.FIELD_NAME, IndexType.EQUAL, Item::getNameForIndex);
		
		entityCache.addIndex(Item.FIELD_EVENTIDS, IndexType.EQUAL, Item::getEventsIdsForIndex);
		
		entityCache.addQueryField(Item.FIELD_AGGSTATUS, Item::getAggStatusForQuery);
		entityCache.addIndex(Item.FIELD_STATUS, IndexType.STATUS, Item::getStatusForIndex);
		
		this.eventService = eventService;
		this.itemHistoryService = itemHistoryService;
		eventService.setItemService(this);
	}

	@Override
	public void addOrUpdate(Item item) {
		entityCache.compute(item.getId(), () -> {
			entityChangedQueue.add(new QueueInfo<String>(item.getId(), QueueInfo.QueueInfoType.UPDATE));
			return new Item.Builder(item)
					.status(BaseStatus.CLEAR)
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
					.deletedOn(item.isDeleted() ? Instant.now() : null)
					.build();
		}, oldItem -> {
			if (item.isDeleted() && oldItem.isDeleted()) {
				return oldItem;
			}
			entityChangedQueue.add(new QueueInfo<String>(item.getId(), QueueInfo.QueueInfoType.UPDATE));
			return new Item.Builder(item)
					.eventsStatus(oldItem.getEventsStatus())
					.status(oldItem.getStatus())
					.aggStatus(oldItem.getAggStatus())
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
					.createdOn(oldItem.getCreatedOn())
					.updatedOn(Instant.now())
					.deletedOn(item.isDeleted() ? Instant.now() : null)
					.build();
		});

	}
	
	@Override
	public Optional<Item> deleteById(String itemId) {
		return entityCache.compute(itemId, () -> null, oldItem -> {
			if (oldItem.isDeleted()) {
				return oldItem;
			}
			entityChangedQueue.add(new QueueInfo<String>(itemId, QueueInfo.QueueInfoType.UPDATE));
			return new Item.Builder(oldItem)
					.version(getNextVersion())
					.fromHistory(Collections.singleton(nodeName))
					.updatedOn(Instant.now())
					.deletedOn(Instant.now())
					.build();
		});
	}
	
	@Override
	protected void queueRead(QueueInfo<String> info) {
		switch(info.getType()) {
		case UPDATE:
			entityCache.compute(info.getId(), () -> null, oldItem -> {
				var newStatus = BaseStatus.CLEAR;
				if(oldItem.isNotDeleted()) {
					newStatus = calculateStatus(oldItem);
				}
				if(oldItem.getStatus().equals(newStatus)) {
					return oldItem;
				}
				entityChangedQueue.add(new QueueInfo<String>(info.getId(), QueueInfo.QueueInfoType.UPDATED));
				return new Item.Builder(oldItem).status(newStatus).build();
			});
			break;
		case UPDATED:
			entityCache.compute(info.getId(), () -> null, item -> {
				itemHistoryService.add(item);
				findParentsById(info.getId()).stream()
						.filter(Item::isNotDeleted)
						.map(Item::getId)
						.forEach(parentId -> {
							entityChangedQueue.add(new QueueInfo<String>(parentId, QueueInfo.QueueInfoType.UPDATE));
						});
				return item;
			});
			break;
		default:
			break;
		}
	}
	
	public void itemUpdateEventsStatus(String itemId, Consumer<Map<String, BaseStatus>> s) {
		entityCache.compute(itemId, () -> null, item -> {
			entityChangedQueue.add(new QueueInfo<String>(itemId, QueueInfo.QueueInfoType.UPDATE));
			return new Item.Builder(item).eventsStatusUpdate(s).build();
		});
	}

	public void eventRemoved(Event event) {
		var predicate = Predicates.equal(Item.FIELD_EVENTIDS, event.getId());
		entityCache.keySet(predicate, -1).stream()
				.forEach(itemId -> itemUpdateEventsStatus(itemId, m -> m.remove(event.getId())));
	}

	public void eventChanged(Event event) {
		findFiltersByEqualFields(event.getFields())
				.forEach(itft -> {
					var item = itft.getKey();
					var filter = itft.getValue();
					itemUpdateEventsStatus(item.getId(), m -> m.put(event.getId(), filter.getStatus(event)));
				});
	}
	
	private BaseStatus calculateStatusByChild(Item parent) {	
		var rules = parent.getRules().values();
		var statuses = findChildren(parent)
				.filter(Item::isNotDeleted)
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
	
	private Stream<Map.Entry<Item, ItemFilter>> findFiltersByEqualFields(Map<String, String> fields){
		return fields.entrySet().stream()
				.map(e -> Predicates.equal(Item.FIELD_FILTERS_EQL, e))
				.flatMap(p -> entityCache.keySet(p, -1).stream())
				.distinct()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get)
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
	
	private Stream<Item> findChildren(Item parent) {
		return parent.getChildrenIds().stream()
				.distinct()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get);
	}
	
	public List<Item> findChildrenById(String id) {
		return findById(id)
				.map(this::findChildren)
				.map(Stream::toList)
				.orElse(Collections.emptyList());
	}
	
	public List<Item> findParentsById(String itemId) {
		var predicate = Predicates.equal(Item.FIELD_PARENTS, itemId);
		return entityCache.keySet(predicate, -1).stream()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.toList();
	}
	
	private Stream<Event> findEventsByItem(Item item) {
		return item.getEventsStatus().keySet().stream()
				.map(eventService::findById)
				.filter(Optional::isPresent)
				.map(Optional::get);
	}
	
	private void findAllItemsById(String parentId, Set<Item> out, Set<String> history) {
		findById(parentId).ifPresent(parent -> {
			if (parent.isDeleted()) {
				return;
			}
			history.add(parentId);
			out.add(parent);
			parent.getChildrenIds().forEach(childId -> {
				if (history.contains(childId)) {
					log.warning("findAllEventsById: circle found from " + parentId + " to " + childId);
				} else {
					findAllItemsById(childId, out, history);
				}
			});
			history.remove(parentId);
		});
	}

	public List<Event> findAllEventsById(String id) {
		var items = new HashSet<Item>();
		var history = new HashSet<String>();
		findAllItemsById(id, items, history);
		return items.stream()
				.flatMap(this::findEventsByItem)
				.distinct()
				.collect(Collectors.toList());

	}
	
	public List<Event> findAllHistoryEventsById(String id, Instant from, Instant to) {
		var items = new HashSet<Item>();
		var history = new HashSet<String>();
		findAllItemsById(id, items, history);
		items.add(new Item.Builder(id).build());
		var ietmIds = items.stream().map(Item::getId).toList();
		return itemHistoryService.getEventsByItemId(ietmIds, from, to);
	}
	
	@Scheduled(fixedRateString = "${item.history.all.fixedrate:60}", timeUnit = TimeUnit.SECONDS)
	public void historyByFixedRate() {
		var predicate = Predicates.greaterEqual(Item.FIELD_VERSION, 0L);
		 entityCache.keySet(predicate, -1).stream()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.forEach(itemHistoryService::add);
		
	}
	
}
