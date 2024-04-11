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

import javax.net.ssl.SSLException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import ru.keich.mon.servicemanager.entity.EntitySchedule;

@Component
public class EventSchedule extends EntitySchedule<String, Event> {
	public EventSchedule(EventService eventService, 
			@Value("${replication.nodename}") String nodeName,
			@Value("${replication.neighbor.host:none}") String replicationNeighborHost,
			@Value("${replication.neighbor.port:8443}") Integer replicationNeighborPort) throws SSLException {
		super(eventService, nodeName, replicationNeighborHost, replicationNeighborPort, "/api/v1/event", Event.class);
	}

}
