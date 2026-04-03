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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.KSearchLexer;
import ru.keich.mon.servicemanager.KSearchParser;
import ru.keich.mon.servicemanager.QueueInfo;
import ru.keich.mon.servicemanager.entity.EntityErrorListener;
import ru.keich.mon.servicemanager.entity.EntitySearchListener;
import ru.keich.mon.servicemanager.entity.EntitySearchListener.ServiceType;
import ru.keich.mon.servicemanager.entity.EntitySearchResult;
import ru.keich.mon.servicemanager.entity.EntityService;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.event.EventService;
import ru.keich.mon.servicemanager.query.Operator;
import ru.keich.mon.servicemanager.query.QueryParamsParser;
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
		this.eventService = eventService;
		eventService.setItemService(this);
		queryValueMapper.put(Item.FIELD_NAME, Item::getNameForQuery);
		queryValueMapper.put(Item.FIELD_AGGSTATUS, Item::getAggStatusForQuery);
		registerIndexMetrics();
	}

	@Override
	public void addOrUpdate(Item item) {
		entityCache.compute(item.getId(), (eventId, oldItem) -> {
			Item.Builder builder;
			if(oldItem != null) {
				builder = new Item.Builder(oldItem);
				builder.updatedOn(Instant.now());
			} else {
				builder = Item.Builder.getDefault(eventId);
				if(item.getCreatedOn() != null) {
					builder.createdOn(item.getCreatedOn());
				}
			}
			if(item.getFields() != null) {
				var fields = item.getFields().entrySet().stream()
						.collect(Collectors.toMap(e -> e.getKey().intern(), e -> e.getValue().intern()));
				builder.fields(Collections.unmodifiableMap(fields));
			}
			Set<String> fromHistory = new HashSet<String>();
			fromHistory.add(nodeName);
			if(item.getFromHistory() != null) {
				fromHistory.addAll(item.getFromHistory().stream().map(String::intern).toList());
			}
			builder.fromHistory(fromHistory);
			if(item.isDeleted()) {
				builder.deletedOn(Instant.now());
			} else {
				builder.deletedOn(null);
			}
			if(item.getName() != null) {
				builder.name(item.getName());
			}
			if(item.getRules() != null) {
				builder.rules(Collections.unmodifiableMap(item.getRules()));
			}
			if(item.getFilters() != null) {
				builder.filters(Collections.unmodifiableMap(item.getFilters()));
			}
			if(item.getChildrenIds() != null) {
				builder.childrenIds(Collections.unmodifiableSet(item.getChildrenIds()));
			}
			if(item.getMaintenance()  != null) {
				builder.maintenance(item.getMaintenance());
			}
			if(item.getSourceType()  != null) {
				builder.sourceType(item.getSourceType());
			}
			entityChangedQueue.add(new QueueInfo<String>(item.getId(), QueueInfo.QueueInfoType.UPDATE));
			return builder
					.version(getNextVersion())
					.source(item.getSource())
					.sourceKey(item.getSourceKey())
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
					return new Item.Builder(item)
							.version(getNextVersion())
							.status(newStatus)
							.build();
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
			return new Item.Builder(item)
					.version(getNextVersion())
					.eventsStatusUpdate(s)
					.build();
		});
	}

	public void eventRemoved(Event event) {
		entityCache.keySetIndexEq(Item.FIELD_EVENTIDS, event.getId())
				.forEach(itemId -> itemUpdateEventsStatus(itemId, m -> m.remove(event.getId())));
	}

	public Set<String> eventChanged(Event event) {
		return findFiltersByEqualFields(event.getFields())
				.map(itft -> {
					var itemId = itft.getKey().getId();
					var newStatus = itft.getValue().getStatus(event);
					itemUpdateEventsStatus(itemId, m -> m.put(event.getId(), newStatus));
					return itemId;
				})
				.collect(Collectors.toSet());
	}

	private Stream<BaseStatus> calculateRulesStatus(Item parent) {	
		if(parent.getRules().isEmpty()) {
			return Stream.of(ItemRule.doDefault(findChildren(parent)));
		}
		return parent.getRules()
				.entrySet()
				.stream()
				.map(e -> e.getValue().calculate(findChildren(parent)));
	}

	private BaseStatus calculateStatus(Item item) {
		var rulesStatus = calculateRulesStatus(item);
		var eventsStatus = item.getEventsStatus().values().stream();
		return BaseStatus.max(rulesStatus, eventsStatus);
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

	public Set<String> findParentIds(Item item) {
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
	
	public Set<String> findAllEventIdsById(String id) {
		 return findByIds(findAllChildrenById(id)).stream()
				.map(Item::getEventsStatus)
				.map(Map::keySet)
				.flatMap(Set::stream)
				.collect(Collectors.toSet());
	}

	public Stream<Event> findAllEventsById(String id) {		
		 return eventService.findByIds(findAllEventIdsById(id)).stream().filter(Event::isNotDeleted);
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

	@Override
	public Object fieldValueOf(String fieldName, String str) {
		return Item.fieldValueOf(fieldName, str);
	}

	public Set<String> findItemIdsByEvent(Event event) {
		return entityCache.keySetIndexEq(Item.FIELD_EVENTIDS, event.getId());
	}

	// TODO move code from getTree
	@Override
	protected Stream<Item> enrich(Stream<Item> data, QueryParamsParser qp) {
		if(qp.getEnrich().contains(Item.FIELD_EVENTS)) {
			return data.map(this::fillEvents);
		}
		return data;
	}

	Item fillEvents(Item item) {
		return new Item.Builder(item)
				.events(findAllEventsById(item.getId()).toList())
				.build();
	}

	@Override
	protected EntitySearchResult<String> getEntitySearchResult(String search) {
		var lexer = new KSearchLexer(CharStreams.fromString(search));
		lexer.removeErrorListeners();
		lexer.addErrorListener(new EntityErrorListener());
		var tokens = new CommonTokenStream(lexer);
		var parser = new KSearchParser(tokens);
		var tree = parser.parse();
		var walker = new ParseTreeWalker();
		var q = new EntitySearchListener(eventService, this, ServiceType.ITEM);
		walker.walk(q, tree);
		return q;
	}

}
