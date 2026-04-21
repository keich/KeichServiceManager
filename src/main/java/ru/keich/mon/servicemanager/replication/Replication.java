package ru.keich.mon.servicemanager.replication;

import java.util.concurrent.TimeUnit;

import org.springframework.scheduling.annotation.Scheduled;

import ru.keich.mon.servicemanager.entity.EntityReplication;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.item.Item;

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

public class Replication {
	final private EntityReplication<String, Event> eventReplication;
	final private EntityReplication<String, Item> itemReplication;
	
	public Replication(EntityReplication<String, Event> eventReplication, EntityReplication<String, Item> itemReplication) {
		super();
		this.eventReplication = eventReplication;
		this.itemReplication = itemReplication;
	}
	
	//TODO to params
	@Scheduled(fixedRate = 5, timeUnit = TimeUnit.SECONDS)
	public void replicationScheduled() {
		itemReplication.doReplication(() -> eventReplication.doReplication());
	}
}
