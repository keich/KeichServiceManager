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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.MeterRegistry;
import ru.keich.mon.indexedhashmap.IndexedHashMap.IndexType;
import ru.keich.mon.indexedhashmap.query.Operator;
import ru.keich.mon.indexedhashmap.query.predicates.Predicates;
import ru.keich.mon.servicemanager.query.QuerySort;
import ru.keich.mon.servicemanager.QueueInfo;
import ru.keich.mon.servicemanager.entity.EntityService;
import ru.keich.mon.servicemanager.item.ItemService;

@Service
public class EventService extends EntityService<String, Event>{
	
	private ItemService itemService;
	
	public void setItemService(ItemService itemService) {
		this.itemService = itemService;
		entityCache.addIndex(Event.FIELD_ENDSON, IndexType.SORTED, Event::getEndsOnForIndex);
		entityCache.addQueryField(Event.FIELD_NODE, Event::getNodeForQuery);
		entityCache.addQueryField(Event.FIELD_SUMMARY, Event::getSummaryForQuery);
	}

	public EventService(@Value("${replication.nodename}") String nodeName
			,MeterRegistry registry
			,@Value("${event.thread.count:2}") Integer threadCount) {
		super(nodeName, registry, threadCount, Event::fieldValueOf);
	}
	
	@Override
	public void addOrUpdate(Event event) {
		entityCache.compute(event.getId(), (k, oldEvent) -> {
			var createdOn = event.getCreatedOn();
			Instant deletedOn = event.isDeleted() ? Instant.now() : null;
			if(oldEvent != null) {
				createdOn = oldEvent.getCreatedOn();
			}
			entityChangedQueue.add(new QueueInfo<String>(event.getId(), QueueInfo.QueueInfoType.UPDATE));
			return new Event.Builder(event)
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
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
					.updatedOn(Instant.now())
					.deletedOn(Instant.now())
					.build();
		}));
	}

	@Override
	protected void queueRead(QueueInfo<String> info) {
		entityCache.computeIfPresent(info.getId(), (k, event) -> {
			if (event.isDeleted()) {
				itemService.eventRemoved(event);
			} else {
				itemService.eventChanged(event);
			}
			return event;
		});
	}
	
	@Scheduled(fixedRateString = "1", timeUnit = TimeUnit.SECONDS)
	public void deleteEndsOnScheduled() {
		var predicate = Predicates.lessThan(Event.FIELD_ENDSON, Instant.now());
		entityCache.keySet(predicate).forEach(this::deleteById);
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
	
}
