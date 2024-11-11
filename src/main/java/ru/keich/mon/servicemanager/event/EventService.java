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
import ru.keich.mon.servicemanager.item.ItemService;
import ru.keich.mon.servicemanager.query.predicates.Predicates;
import ru.keich.mon.servicemanager.store.IndexedHashMap.IndexType;

@Service
public class EventService extends EntityService<String, Event>{
	
	static final public String INDEX_NAME_ENDS_ON = "endson";
	
	private ItemService itemService;
	
	public void setItemService(ItemService itemService) {
		this.itemService = itemService;
		entityCache.addIndex(INDEX_NAME_ENDS_ON, IndexType.SORTED, Event::getEndsOnForIndex);
	}

	public EventService(@Value("${replication.nodename}") String nodeName
			,MeterRegistry registry
			,@Value("${event.thread.count:2}") Integer threadCount) {
		super(nodeName, registry, threadCount);
	}
	
	@Override
	public void addOrUpdate(Event event) {
		entityCache.compute(event.getId(), () -> {
			entityChangedQueue.add(new QueueInfo<String>(event.getId(), QueueInfo.QueueInfoType.UPDATE));
			return new Event.Builder(event)
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
					.build();	
		}, oldEvent -> {
			entityChangedQueue.add(new QueueInfo<String>(event.getId(), QueueInfo.QueueInfoType.UPDATE));
			return new Event.Builder(event)
					.version(getNextVersion())
					.fromHistoryAdd(nodeName)
					.createdOn(oldEvent.getCreatedOn())
					.updatedOn(Instant.now())
					.build();
		});
	}

	@Override
	public Optional<Event> deleteById(String eventId) {
		return entityCache.computeIfPresent(eventId, oldEvent -> {
			if(Objects.nonNull(oldEvent.getDeletedOn())) {
				return null;
			}
			entityChangedQueue.add(new QueueInfo<String>(eventId, QueueInfo.QueueInfoType.REMOVED));
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
		switch(info.getType()) {
		case UPDATE:
			entityCache.computeIfPresent(info.getId(), event -> {
				itemService.eventChanged(event);
				return event;
			});
			break;
		case REMOVED:
			entityCache.computeIfPresent(info.getId(), event -> {
				itemService.eventRemoved(info.getId());
				return event;
			});
			break;
		default:
			break;
		}
	}
	
	@Scheduled(fixedRateString = "1", timeUnit = TimeUnit.SECONDS)
	public void deleteEndsOnScheduled() {
		var predicate = Predicates.lessThan(INDEX_NAME_ENDS_ON, Instant.now());
		entityCache.keySet(predicate, -1).stream()
				.forEach(this::deleteById);
	}
	
}
