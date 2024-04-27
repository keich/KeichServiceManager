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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import ru.keich.mon.servicemanager.entity.EntityService;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.item.ItemService;
import ru.keich.mon.servicemanager.store.IndexedHashMap.IndexType;

@Service
public class EventService extends EntityService<String, Event>{

	private ItemService itemService;
	static final String INDEX_NAME_FIELDS = "fields";
	
	public void setItemService(ItemService itemService) {
		this.itemService = itemService;
	}

	public EventService(@Value("${replication.nodename}") String nodeName) {
		super(nodeName);
		entityCache.createIndex(INDEX_NAME_FIELDS, IndexType.EQUAL, Event::getFieldsForIndex);
	}
	
	@Override
	protected void afterInsert(Event event) {
		super.afterInsert(event);
		itemService.eventAdded(event);
	}

	@Override
	protected void entityRemoved(Event event) {
		super.entityRemoved(event);
		itemService.eventRemoved(event);
	}

	public void itemAdded(Item item){
		entityCache.transaction(() -> {
			item.getFilters().entrySet().stream()
				.flatMap(e -> e.getValue().getEqualFields().entrySet().stream())
				.flatMap(k -> entityCache.indexGet(INDEX_NAME_FIELDS, k).stream())
				.distinct()
				.map(id -> findById(id))
				.filter(o -> o.isPresent())
				.map(o -> o.get())	
				.filter(event -> item.getFilters().entrySet().stream()
					.filter(flt -> event.getFields().entrySet().containsAll(flt.getValue().getEqualFields().entrySet()))
					.findFirst()
					.isPresent()
				)
				.forEach(event -> itemService.eventAdded(event));
			return null;
		});
	}
}
