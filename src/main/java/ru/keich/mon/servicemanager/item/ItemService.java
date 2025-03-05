package ru.keich.mon.servicemanager.item;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
		entityCache.addIndex(Item.FIELD_STATUS, IndexType.SORTED, Item::getStatusForIndex);
		
		this.eventService = eventService;
		this.itemHistoryService = itemHistoryService;
		eventService.setItemService(this);
	}

	public void addOrUpdate(Item item) {
		entityCache.compute(item.getId(), () -> {
			entityChangedQueue.add(new QueueInfo<String>(item.getId(), QueueInfo.QueueInfoType.UPDATE));
			return new Item.Builder(item)
					.status(BaseStatus.CLEAR)
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
					.deletedOn(Objects.nonNull(item.getDeletedOn()) ? Instant.now() : null)
					.build();
		}, oldItem -> {
			if(Objects.nonNull(item.getDeletedOn()) && Objects.nonNull(oldItem.getDeletedOn())) {
				return null;
			}
			entityChangedQueue.add(new QueueInfo<String>(item.getId(), QueueInfo.QueueInfoType.UPDATE));
			return new Item.Builder(item)
					.status(oldItem.getStatus())
					.aggStatus(oldItem.getAggStatus())
					.eventsStatus(oldItem.getEventsStatus())
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
					.createdOn(oldItem.getCreatedOn())
					.updatedOn(Instant.now())
					.deletedOn(Objects.nonNull(item.getDeletedOn()) ? Instant.now() : null)
					.build();
		});

	}
	
	@Override
	public Optional<Item> deleteById(String itemId) {
		return entityCache.computeIfPresent(itemId, oldItem -> {
			if(Objects.nonNull(oldItem.getDeletedOn())) {
				return null;
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
			entityCache.computeIfPresent(info.getId(), oldItem -> {
				var item = calculateStatus(new Item.Builder(oldItem));
				if (item.isChanged()) {
					entityChangedQueue.add(new QueueInfo<String>(info.getId(), QueueInfo.QueueInfoType.UPDATED));
					return item.build();
				}
				return null;
			});
			break;
		case UPDATED:
			entityCache.computeIfPresent(info.getId(), item -> {
				itemHistoryService.add(item);
				findParentsById(info.getId()).stream()
				.filter(parent -> Objects.isNull(parent.getDeletedOn()))
				.map(Item::getId)
				.forEach(parentId -> {
					entityChangedQueue.add(new QueueInfo<String>(parentId, QueueInfo.QueueInfoType.UPDATE));
				});
				return null;
			});
			break;
		default:
			break;
		}
	}
	
	public void itemUpdateEventsStatus(String itemId, Consumer<Map<String, BaseStatus>> s) {
		entityCache.computeIfPresent(itemId, item -> {
			entityChangedQueue.add(new QueueInfo<String>(itemId, QueueInfo.QueueInfoType.UPDATE));
			return new Item.Builder(item).eventsStatusUpdate(s).build();
		});
	}

	private void eventRemoved(Event event) {
		var predicate = Predicates.equal(Item.FIELD_EVENTIDS, event.getId());
		entityCache.keySet(predicate, -1).stream()
				.forEach(itemId -> itemUpdateEventsStatus(itemId, m -> m.remove(event.getId())));
	}

	public void eventChanged(Event event) {
		if(Objects.nonNull(event.getDeletedOn())) {
			eventRemoved(event);
			return;
		}
		findFiltersByEqualFields(event.getFields())
				.forEach(itft -> {
					var item = itft.getKey();
					var filter = itft.getValue();
					itemUpdateEventsStatus(item.getId(), m -> m.put(event.getId(), filter.getStatus(event)));
				});
	}

	private int calculateEntityStatusAsCluster(Set<String> childrenIds, ItemRule rule) {
		var listStatus = childrenIds.stream()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.filter(child -> Objects.isNull(child.getDeletedOn()))
				.mapToInt(child -> child.getStatus().ordinal())
				.boxed()
				.filter(i -> i >= rule.getStatusThreshold().ordinal())
				.toList();
		var percent = 100 * listStatus.size() / childrenIds.size();
		if (percent >= rule.getValueThreshold()) {
			if (rule.isUsingResultStatus()) {
				return rule.getResultStatus().ordinal();
			} else {
				var min = listStatus.stream().mapToInt(i -> i).min().orElse(0);
				return min;
			}
		}
		return 0;
	}

	private int calculateEntityStatusDefault(Set<String> childrenIds) {
		return childrenIds.stream()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.filter(child -> Objects.isNull(child.getDeletedOn()))
				.mapToInt(child -> child.getStatus().ordinal())
				.max().orElse(0);
	}
	
	private int calculateStatusByChild(Collection<ItemRule> rules, Set<String> childrenIds) {
		return rules.stream().mapToInt(rule -> {
			switch (rule.getType()) {
			case CLUSTER:
				return calculateEntityStatusAsCluster(childrenIds, rule);
			default:
				return calculateEntityStatusDefault(childrenIds);
			}
		}).max().orElse(calculateEntityStatusDefault(childrenIds));
	}
	
	private Item.Builder calculateStatus(Item.Builder item) {
		if(Objects.nonNull(item.getDeletedOn())) {
			return item.status(BaseStatus.CLEAR);
		}
		var rules = item.getRules().values();
		var childrenIds = item.getChildrenIds();
		var statusByChild = childrenIds.isEmpty() ? BaseStatus.CLEAR : BaseStatus.fromInteger(calculateStatusByChild(rules, childrenIds));
		var statusByEvents = item.getEventsStatus().values().stream().mapToInt(BaseStatus::ordinal).max().orElse(0);
		return item.status(statusByChild.max(BaseStatus.fromInteger(statusByEvents)));
	}
	
	private List<Map.Entry<Item, ItemFilter>> findFiltersByEqualFields(Map<String, String> fields){
		return fields.entrySet().stream()
				.map(e -> Predicates.equal(Item.FIELD_FILTERS_EQL, e))
				.flatMap(p -> entityCache.keySet(p, -1).stream())
				.distinct()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.filter(item -> Objects.isNull(item.getDeletedOn()))
				.map(item -> {
					return item.getFilters().entrySet().stream()
							.filter(flt -> fields.entrySet().containsAll(flt.getValue().getEqualFields().entrySet()))
							.findFirst()
							.map(e -> Map.entry(item, e.getValue()));
				})
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
	}
	
	public List<Item> findChildrenById(String id) {
		return findById(id).stream()
				.map(Item::getChildrenIds)
				.flatMap(Set::stream)
				.distinct()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.toList();
	}
	
	public List<Item> findParentsById(String itemId) {
		var predicate = Predicates.equal(Item.FIELD_PARENTS, itemId);
		return entityCache.keySet(predicate, -1).stream()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.toList();
	}
	
	private List<Event> findEventsByItem(Item item) {
		return item.getEventsStatus().keySet().stream()
				.map(id -> eventService.findById(id))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
	}
	
	private void findAllItemsById(String parentId, Set<Item> out, Set<String> history) {
		findById(parentId).ifPresent(parent -> {
			if(Objects.nonNull(parent.getDeletedOn())) {
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
		return items.stream().flatMap(item -> findEventsByItem(item).stream())
				.distinct()
				.collect(Collectors.toList());

	}
	
	public List<Event> findAllHistoryEventsById(String id, Instant from, Instant to) {
		var items = new HashSet<Item>();
		var history = new HashSet<String>();
		findAllItemsById(id, items, history);
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
