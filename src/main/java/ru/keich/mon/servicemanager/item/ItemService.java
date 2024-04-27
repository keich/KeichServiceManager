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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.entity.EntityService;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.event.EventService;
import ru.keich.mon.servicemanager.eventrelation.EventRelationService;
import ru.keich.mon.servicemanager.store.IndexedHashMap.IndexType;


@Service
@Log
public class ItemService extends EntityService<String, Item> {
	
	private final EventService eventService;
	private final EventRelationService eventRelationService;

	static final String INDEX_NAME_FILTERS_EQL = "filter_equal";
	static final String INDEX_NAME_PARENTS = "parents";
	
	public ItemService(@Value("${replication.nodename}") String nodeName, EventService eventService,
			EventRelationService eventRelationService) {
		super(nodeName);
		entityCache.createIndex(INDEX_NAME_FILTERS_EQL, IndexType.EQUAL, Item::getFiltersForIndex);
		entityCache.createIndex(INDEX_NAME_PARENTS, IndexType.EQUAL, Item::getParentsForIndex);
		this.eventService = eventService;
		this.eventRelationService = eventRelationService;
		eventService.setItemService(this);
	}
	
	@Override
	protected void entityRemoved(Item item) {
		super.entityRemoved(item);
		eventRelationService.itemRemoved(item);
		item.getChildren().stream()
		.map(itemId -> findById(itemId))
		.filter(opt -> opt.isPresent())
		.map(opt -> opt.get())		
		.forEach(parent ->{
			calculateStatusStart(parent);
		});
	}

	
	@Override
	protected void beforeInsert(Item item) {
		super.beforeInsert(item);
		item.setStatus(BaseStatus.CLEAR);
	}

	@Override
	protected void afterInsert(Item item) {
		super.afterInsert(item);
		calculateStatusStart(item);
	}
	

	@Override
	protected void afterInsertUnLock(Item item) {
		super.afterInsertUnLock(item);
		eventService.itemAdded(item);
	}

	@Override
	protected boolean insertExist(Item old, Item item) {
		if(!super.insertExist(old, item)) {
			return false;
		}
		item.setStatus(old.getStatus());
		return true;
	}

	private int calculateEntityStatusAsCluster(Item item, ItemRule rule) {
		var overal = item.getChildren().size();
		if (overal <= 0) {
			return 0;
		}
		var listStatus = item.getChildren().stream()
				.map(cid -> findById(cid))
				.filter(o -> o.isPresent())
				.map(o -> o.get())
				.mapToInt(child -> child.getStatus().ordinal())
				.boxed()
				.filter(i -> i >= rule.getStatusThreshold().ordinal())
				.collect(Collectors.toList());
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

	private int calculateEntityStatusDefault(Item item) {
		return item.getChildren().stream()
			.map(cid -> findById(cid))
			.filter(o -> o.isPresent())
			.map(o -> o.get())
			.mapToInt(child -> child.getStatus().ordinal())
			.max().orElse(0);
	}
	
	private int calculateStatusByChild(Item item) {
		var rules = item.getRules();
		return rules.entrySet().stream().mapToInt(e -> {
			var rule = e.getValue();
			switch (rule.getType()) {
			case CLUSTER:
				return calculateEntityStatusAsCluster(item, rule);
			default:
				return calculateEntityStatusDefault(item);
			}
		}).max().orElse(calculateEntityStatusDefault(item));
	}
	
	private void calculateStatus(Item item, HashSet<String> history) {//TODO
		BaseStatus maxStatus = BaseStatus.fromInteger(calculateStatusByChild(item));
		BaseStatus eventStatusMax = eventRelationService.getMaxStatus(item);
		maxStatus = maxStatus.max(eventStatusMax);
		if(maxStatus != item.getStatus()) {
			history.add(item.getId());
			item.setStatus(maxStatus);
			findParents(item.getId()).forEach(parent ->{
				if (history.contains(parent.getId())) {
					log.warning("calculateEntityStatusDeeper: circle found from " + item.getId() + " to " + parent.getId());
				}else {
					calculateStatus(parent, history);
				}
			});
			history.remove(item.getId());
		}
	}
	
	private void calculateStatusStart(Item item) {
		var history = new HashSet<String>();
		calculateStatus(item, history);
	}
	
	public List<Map.Entry<Item, ItemFilter>> findFiltersByEqualFields(Map<String, String> fields){
		return fields.entrySet().stream()
				.flatMap(k -> entityCache.indexGet(INDEX_NAME_FILTERS_EQL,  k).stream())
				.distinct()
				.map(id -> findById(id))
				.filter(o -> o.isPresent())
				.map(o -> o.get())
				.map(item -> {
					return item.getFilters().entrySet().stream()
					.filter(flt -> fields.entrySet().containsAll(flt.getValue().getEqualFields().entrySet()))
					.findFirst()
					.map(e -> Map.entry(item, e.getValue()));
				})
				.filter(o -> o.isPresent())
				.map(o -> o.get())
				.collect(Collectors.toList());
	}
	
	public void eventAdded(Event event) {
		entityCache.transaction(() -> {
			findFiltersByEqualFields(event.getFields())
			.forEach(itft -> {
				var item = itft.getKey();
				var filter = itft.getValue();
				var status = event.getStatus();
				if(filter.isUsingResultStatus()) {
					status =  filter.getResultStatus();
				}
				eventRelationService.add(item, event, status);
				calculateStatusStart(item);
			});
			return null;
		});
	}
	
	public void eventRemoved(Event event) {
		entityCache.transaction(() -> {
			var itemIds = eventRelationService.getItemIds(event);
			eventRelationService.eventRemoved(event);
			itemIds.stream()
			.map(itemId -> findById(itemId))
			.filter(opt -> opt.isPresent())
			.map(opt -> opt.get())
			.forEach(item -> calculateStatusStart(item));
			return null;
		});
	}

	public List<Item> findChildren(String itemId) {
		return findById(itemId).map( item -> item.getChildren()).stream()
			.flatMap(set -> set.stream())
			.map(cid -> findById(cid))
			.filter(o -> o.isPresent())
			.map(o -> o.get())
			.collect(Collectors.toList());
	}
	
	public List<Item> findParents(String itemId) {
		return entityCache.indexGet(INDEX_NAME_PARENTS, itemId).stream()
		.map(parentId -> findById(parentId))
		.filter(opt -> opt.isPresent())
		.map(opt -> opt.get())
		.collect(Collectors.toList());
	}
	
	private List<Event> findEventsByItem(Item item){
		return eventRelationService.getEventIds(item).stream()
		.map(id -> eventService.findById(id))
		.filter(opt -> opt.isPresent())
		.map(opt -> opt.get())
		.collect(Collectors.toList());
	}
	
	private void findAllItemsById(String parentId, Set<Item> out, Set<String> history) {
		findById(parentId).ifPresent(parent -> {
			history.add(parentId);
			out.add(parent);
			parent.getChildren().forEach(childId -> {
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
		return entityCache.transaction(() -> {
			var items = new HashSet<Item>();
			var history = new HashSet<String>();
			findAllItemsById(id, items, history);
			return items.stream().flatMap(item -> findEventsByItem(item).stream())
					.distinct()
					.collect(Collectors.toList());
		});
	}
	
}
