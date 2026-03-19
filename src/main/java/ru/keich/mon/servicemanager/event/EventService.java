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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.MeterRegistry;
import ru.keich.mon.servicemanager.QueueInfo;
import ru.keich.mon.servicemanager.entity.EntityService;
import ru.keich.mon.servicemanager.item.ItemService;
import ru.keich.mon.servicemanager.query.Operator;
import ru.keich.mon.servicemanager.query.QueryParamsParser;
import ru.keich.mon.servicemanager.query.QuerySort;

@Service
public class EventService extends EntityService<String, Event>{

	private ItemService itemService;

	public void setItemService(ItemService itemService) {
		this.itemService = itemService;
		entityCache.addIndexSorted(Event.FIELD_ENDSON, Event::getEndsOnForIndex);
	}

	public EventService(@Value("${replication.nodename}") String nodeName
			,MeterRegistry registry
			,@Value("${event.thread.count:2}") Integer threadCount) {
		super(nodeName, registry, threadCount);
		queryValueMapper.put(Event.FIELD_NODE, Event::getNodeForQuery);
		queryValueMapper.put(Event.FIELD_SUMMARY, Event::getSummaryForQuery);
		entityCache.addIndexSmallInt(Event.FIELD_CALCULATED, 2, Event::getCalculatedForIndex);
		registerIndexMetrics();
	}

	@Override
	public void addOrUpdate(Event event) {
		entityCache.compute(event.getId(), (eventId, oldEvent) -> {
			Event.Builder builder;
			if(oldEvent != null) {
				builder = new Event.Builder(oldEvent);
				builder.updatedOn(Instant.now());
			} else {
				builder = Event.Builder.getDefault(eventId);
				if(event.getCreatedOn() != null) {
					builder.createdOn(event.getCreatedOn());
				}
			}
			if(event.getFields() != null) {
				var fields = event.getFields().entrySet().stream()
						.collect(Collectors.toMap(e -> e.getKey().intern(), e -> e.getValue().intern()));
				builder.fields(Collections.unmodifiableMap(fields));
			}
			Set<String> fromHistory = new HashSet<String>();
			fromHistory.add(nodeName);
			if(event.getFromHistory() != null) {
				fromHistory.addAll(event.getFromHistory().stream().map(String::intern).toList());
			}
			builder.fromHistory(fromHistory);
			if(event.isDeleted()) {
				builder.deletedOn(Instant.now()).calculated(true);
			} else {
				builder.deletedOn(null).calculated(false);
			}
			if (event.getNode() == null) {
				if(event.getFields() != null) {
					var fieldsNode = event.getFields().get("node");
					builder.node(fieldsNode != null ? fieldsNode.intern() : "");
				}
			} else {
				builder.node(event.getNode().intern());
			}
			if (event.getSummary() == null) {
				if(event.getFields() != null) {
					var fieldsSummary = event.getFields().get("summary");
					builder.summary(fieldsSummary != null ? fieldsSummary : "");
				}
			} else {
				builder.summary(event.getSummary());
			}
			if(event.getSourceType() != null) {
				builder.sourceType(event.getSourceType());
			}
			entityChangedQueue.add(new QueueInfo<String>(event.getId(), QueueInfo.QueueInfoType.UPDATE));
			return builder
					.source(event.getSource())
					.sourceKey(event.getSourceKey())
					.status(event.getStatus())
					.type(event.getType())
					.endsOn(event.getEndsOn())
					.version(getNextVersion())
					.build();
		});
	}

	@Override
	public Optional<Event> deleteById(String eventId) {
		return Optional.ofNullable(entityCache.computeIfPresent(eventId, (k, oldEvent) -> {
			if (oldEvent.isDeleted()) {
				return oldEvent;
			}
			entityChangedQueue.add(new QueueInfo<String>(eventId, QueueInfo.QueueInfoType.UPDATE));
			return new Event.Builder(oldEvent)
					.version(getNextVersion())
					.calculated(true)
					.fromHistory(Collections.singleton(nodeName))
					.deletedOn(Instant.now())
					.build();
		}));
	}

	@Override
	protected void queueRead(QueueInfo<String> info) {
		switch (info.getType()) {
		case UPDATE:
			entityCache.computeIfPresent(info.getId(), (k, event) -> {
				if (event.isDeleted()) {
					itemService.eventRemoved(event);
				} else {
					var itemIds = itemService.eventChanged(event);
					return new Event.Builder(event)
							.calculated(true)
							.version(getNextVersion())
							.itemIdsUpdate(s-> s.addAll(itemIds))
							.build();
				}
				return event;
			});
		case UPDATED:
			// not used
			break;
		}
	}

	@Scheduled(fixedRateString = "1", timeUnit = TimeUnit.SECONDS)
	public void deleteEndsOnScheduled() {
		entityCache.keySetIndexGetBefore(Event.FIELD_ENDSON, Instant.now()).forEach(this::deleteById);
	}

	@Override
	public Comparator<Event> getSortComparator(QuerySort sort) {
		final int mult = sort.getOperator() == Operator.SORTDESC ? -1 : 1;
		switch (sort.getName()) {
		case Event.FIELD_ENDSON:
			return (e1, e2) -> e1.getEndsOn().compareTo(e2.getEndsOn()) * mult;
		case Event.FIELD_NODE:
			return (e1, e2) -> e1.getNode().compareTo(e2.getNode()) * mult;
		case Event.FIELD_SUMMARY:
			return (e1, e2) -> e1.getSummary().compareTo(e2.getSummary()) * mult;
		}
		return super.getSortComparator(sort);
	}

	public Object fieldValueOf(String fieldName, String str) {
		return Event.fieldValueOf(fieldName, str);
	}

	public Set<String> findItemIdsByEvent(Event event) {
		return itemService.findItemIdsByEvent(event);
	}

	@Override
	protected Stream<Event> enrich(Stream<Event> data, QueryParamsParser qp) {
		return data;
	}

}
