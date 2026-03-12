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
	}

	@Override
	public void addOrUpdate(Event event) {
		entityCache.compute(event.getId(), (k, oldEvent) -> {
			final Instant createdOn = oldEvent == null ? event.getCreatedOn() : oldEvent.getCreatedOn();
			final Instant deletedOn = event.isDeleted() ? Instant.now() : null;
			final var fromHistory = new HashSet<String>();
			fromHistory.add(nodeName);
			entityChangedQueue.add(new QueueInfo<String>(event.getId(), QueueInfo.QueueInfoType.UPDATE));
			return new Event.Builder(event)
					.calculated(false)
					.version(getNextVersion())
					.fromHistory(fromHistory)
					.createdOn(createdOn)
					.updatedOn(Instant.now())
					.deletedOn(deletedOn)
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
					itemService.eventChanged(event);
				}
				return new Event.Builder(event)
						.calculated(true)
						.version(getNextVersion())
						.build();
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
		if(qp.getEnrich().contains(Event.FIELD_ITEMIDS)) {
			return data.map(this::fillItemIds);
		}
		return data;
	}

	Event fillItemIds(Event event) {
		return new Event.Builder(event).itemIds(findItemIdsByEvent(event)).build();
	}

}
