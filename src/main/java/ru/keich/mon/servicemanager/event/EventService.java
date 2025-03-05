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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.MeterRegistry;
import ru.keich.mon.servicemanager.QueueInfo;
import ru.keich.mon.servicemanager.entity.EntityService;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.item.ItemService;
import ru.keich.mon.servicemanager.query.predicates.Predicates;
import ru.keich.mon.servicemanager.store.IndexedHashMap.IndexType;

@Service
public class EventService extends EntityService<String, Event>{
	
	private ItemService itemService;
	private final EventHistoryService eventHistoryService;
	private final Integer historyEventSearchLimit = 1000;
	
	public void setItemService(ItemService itemService) {
		this.itemService = itemService;
		entityCache.addIndex(Event.FIELD_ENDSON, IndexType.SORTED, Event::getEndsOnForIndex);
		entityCache.addIndex(Event.FIELD_NODE, IndexType.EQUAL, Event::getEndsOnForIndex);
		entityCache.addIndex(Item.FIELD_STATUS, IndexType.SORTED, Event::getStatusForIndex);
	}

	public EventService(@Value("${replication.nodename}") String nodeName
			,EventHistoryService eventHistoryService
			,MeterRegistry registry
			,@Value("${event.thread.count:2}") Integer threadCount) {
		super(nodeName, registry, threadCount);
		this.eventHistoryService = eventHistoryService;
	}
	
	@Override
	public void addOrUpdate(Event event) {
		entityCache.compute(event.getId(), () -> {
			entityChangedQueue.add(new QueueInfo<String>(event.getId(), QueueInfo.QueueInfoType.UPDATE));
			return new Event.Builder(event)
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
					.deletedOn(Objects.nonNull(event.getDeletedOn()) ? Instant.now() : null)
					.build();	
		}, oldEvent -> {
			if(Objects.nonNull(event.getDeletedOn()) && Objects.nonNull(oldEvent.getDeletedOn())) {
				return null;
			}
			entityChangedQueue.add(new QueueInfo<String>(oldEvent.getId(), QueueInfo.QueueInfoType.UPDATE));
			return new Event.Builder(event)
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
					.createdOn(oldEvent.getCreatedOn())
					.updatedOn(Instant.now())
					.deletedOn(Objects.nonNull(event.getDeletedOn())? Instant.now() : null)
					.build();
		});
	}

	@Override
	public Optional<Event> deleteById(String eventId) {
		return entityCache.computeIfPresent(eventId, oldEvent -> {
			if(Objects.nonNull(oldEvent.getDeletedOn())) {
				return null;
			}
			entityChangedQueue.add(new QueueInfo<String>(eventId, QueueInfo.QueueInfoType.UPDATE));
			return new Event.Builder(oldEvent)
					.version(getNextVersion())
					.fromHistory(Collections.singleton(nodeName))
					.updatedOn(Instant.now())
					.deletedOn(Instant.now())
					.build();
		});
	}

	@Override
	protected void queueRead(QueueInfo<String> info) {
		entityCache.computeIfPresent(info.getId(), event -> {
			eventHistoryService.add(event);
			itemService.eventChanged(event);
			return event;
		});
	}
	
	@Scheduled(fixedRateString = "1", timeUnit = TimeUnit.SECONDS)
	public void deleteEndsOnScheduled() {
		var predicate = Predicates.lessThan(Event.FIELD_ENDSON, Instant.now());
		entityCache.keySet(predicate, -1).stream()
				.forEach(this::deleteById);
	}
	
	public Optional<Event> findByIdHistory(String id) {
		return Optional.ofNullable(eventHistoryService.getEventsByIds(Collections.singletonList(id), historyEventSearchLimit).get(id));
	}
	
}
