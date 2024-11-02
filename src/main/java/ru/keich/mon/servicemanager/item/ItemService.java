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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.QueueThreadReader;
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
	final protected QueueThreadReader<String> itemNeedUpdateParentsQueue;

	static final String INDEX_NAME_FILTERS_EQL = "filter_equal";
	static final String INDEX_NAME_PARENTS = "parents";
	
	static final public String INDEX_NAME_NAME_UPPERCASE = "name";
	static final public String INDEX_NAME_EVENTIDS = "eventIds";
	
	public ItemService(@Value("${replication.nodename}") String nodeName, EventService eventService, MeterRegistry registry) {
		super(nodeName, registry);

		entityCache.createIndex(INDEX_NAME_FILTERS_EQL, IndexType.EQUAL, Item::getFiltersForIndex);
		entityCache.createIndex(INDEX_NAME_PARENTS, IndexType.EQUAL, Item::getParentsForIndex);
		
		entityCache.createIndex(INDEX_NAME_NAME_UPPERCASE, IndexType.EQUAL, Item::getNameUpperCaseForIndex);
		
		entityCache.createIndex(INDEX_NAME_EVENTIDS, IndexType.EQUAL, Item::getEventsIndex);
		
		this.eventService = eventService;
		eventService.setItemService(this);

		itemNeedUpdateParentsQueue = new QueueThreadReader<String>(this.getClass().getSimpleName() + "-itemNeedUpdateParentsQueue", 4, this::needUpdateParentsChanged);
	}

	public void addOrUpdate(Item item) {
		var t  = new Item.Builder(item)
				.version(getNextVersion())
				.fromHistoryAdd(nodeName)
				.status(BaseStatus.CLEAR);
		t.build();
		entityCache.compute(item.getId(), () -> {
			entityChangedQueue.add(item.getId());
			return new Item.Builder(item)
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
					.status(BaseStatus.CLEAR)
					.build();
		}, oldItem -> {
			entityChangedQueue.add(item.getId());
			return new Item.Builder(item)
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
					.createdOn(oldItem.getCreatedOn())
					.updatedOn(Instant.now())
					.status(oldItem.getStatus())
					.eventsStatus(oldItem.getEventsStatus())
					.build();
		});

	}
	
	@Override
	public Optional<Item> deleteById(String itemId) {
		return entityCache.computeIfPresent(itemId, oldItem -> {
			if(Objects.nonNull(oldItem.getDeletedOn())) {
				entityChangedQueue.add(itemId);
				return null;
			}
			entityChangedQueue.add(itemId);
			return new Item.Builder(oldItem)
					.version(getNextVersion())
					.fromHistory(Collections.singleton(nodeName))
					.updatedOn(Instant.now())
					.deletedOn(Instant.now())
					.build();
		});
	}
	
	@Override
	protected void entityChanged(String itemId) {
		entityCache.computeIfPresent(itemId, oldItem -> {
			var item = calculateStatus(new Item.Builder(oldItem));
			if (item.isChanged()) {
				itemNeedUpdateParentsQueue.add(itemId);
				return item.build();
			}
			return oldItem;
		});
	}
	
	public void needUpdateParentsChanged(String itemId) {
		entityCache.get(itemId).ifPresent(item -> {
			findParents(item).stream()
			.filter(parent -> Objects.isNull(parent.getDeletedOn()))
			.forEach(parent -> {
				entityChangedQueue.add(parent.getId());
			});
		});
	}
	
	
	public void itemUpdateEventsStatus(String itemId, Consumer<Map<String, BaseStatus>> s) {
		entityCache.computeIfPresent(itemId, item -> {
			itemNeedUpdateParentsQueue.add(itemId);
			return calculateStatus(new Item.Builder(item).eventsStatusUpdate(s)).build();
		});
	}
	
	public void eventChanged(Event event) {
		if(Objects.nonNull(event.getDeletedOn())) {
			var predicate = Predicates.equal(INDEX_NAME_EVENTIDS, event.getId());
			entityCache.keySet(predicate, -1).stream()
					.forEach(itemId -> itemUpdateEventsStatus(itemId, m -> m.remove(event.getId())));
			return;
		}
		findFiltersByEqualFields(event.getFields())
				.forEach(itft -> {
					var item = itft.getKey();
					var filter = itft.getValue();
					BaseStatus status;
					if(filter.isUsingResultStatus()) {
						status =  filter.getResultStatus();
					} else {
						status = event.getStatus();
					}
					itemUpdateEventsStatus(item.getId(), m -> m.put(event.getId(), status));
				});
	}

	private int calculateEntityStatusAsCluster(Set<String> childrenIds, ItemRule rule) {
		var overal = childrenIds.size();
		if (overal <= 0) {
			return 0;
		}
		var listStatus = childrenIds.stream()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.filter(child -> Objects.isNull(child.getDeletedOn()))
				.mapToInt(child -> child.getStatus().ordinal())
				.boxed()
				.filter(i -> i >= rule.getStatusThreshold().ordinal())
				.toList();
		var percent = 100 * listStatus.size() / overal;
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
		var rules = item.getRules().values();
		var childrenIds = item.getChildrenIds();
		var childStatus = BaseStatus.fromInteger(calculateStatusByChild(rules, childrenIds));
		var max = item.getEventsStatus().values().stream().mapToInt(BaseStatus::ordinal).max().orElse(0);
		var eventStatusMax = BaseStatus.fromInteger(max);
		var overalStatus = childStatus.max(eventStatusMax);
		item.status(overalStatus);
		return item;
	}
	
	private List<Map.Entry<Item, ItemFilter>> findFiltersByEqualFields(Map<String, String> fields){
		return fields.entrySet().stream()
				.map(e -> Predicates.equal(INDEX_NAME_FILTERS_EQL, e))
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

	public List<Item> findChildren(Item item) {
		return item.getChildrenIds().stream()
				.distinct()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
	}
	
	public List<Item> findChildrenById(String id) {
		return findById(id)
				.map(this::findChildren)
				.orElse(Collections.emptyList());
	}
	
	public List<Item> findParents(Item item) {
		return findParentsById(item.getId());
	}
	
	public List<Item> findParentsById(String itemId) {
		var predicate = Predicates.equal(INDEX_NAME_PARENTS, itemId);
		return entityCache.keySet(predicate, -1).stream()
				.map(this::findById)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList());
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
	
}
