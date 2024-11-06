package ru.keich.mon.servicemanager.alert;

import java.time.Instant;
import java.util.HashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.event.Event.EventType;
import ru.keich.mon.servicemanager.event.EventService;

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
public class AlertService {

	public static String SOURCE = "Alerts";
	public static String SOURCEKEY = "AlertsInternal";
	public static String FIELDS_ANNOTATIONS_PREFIX = "annotations.";
	public static String FIELDS_ALERTS_PREFIX = "alert.";
	public static String ANNOTATION_NODE = "node";
	public static String ANNOTATION_SEVERITY = "severity";
	public static String ANNOTATION_DESCRIPTION = "description";
	public static String ANNOTATION_ALERT_ID = "alert_id";
	
	@Autowired 
	EventService eventService;
	
	public void addOrUpdate(Alert alert) {
		eventService.addOrUpdate(alertToEvent(alert));
	}

	public Event alertToEvent(Alert alert) {
		var fields = new HashMap<String, String>();
		fields.putAll(alert.getLabels());
		alert.getAnnotations().entrySet().forEach(e -> {
			fields.put(FIELDS_ANNOTATIONS_PREFIX + e.getKey(), e.getValue());
		});
		fields.put(FIELDS_ALERTS_PREFIX + "starts_at", alert.getStartsAt());
		fields.put(FIELDS_ALERTS_PREFIX + "ends_at", alert.getEndsAt());
		fields.put(FIELDS_ALERTS_PREFIX + "generator_url", alert.getGeneratorURL());
		return new Event.Builder(alert.getAnnotations().getOrDefault(ANNOTATION_ALERT_ID, ""))
				.node(alert.getAnnotations().getOrDefault(ANNOTATION_NODE, ""))
				.endsOn(Instant.parse(alert.getEndsAt()))
				.status(BaseStatus.fromString(alert.getAnnotations().getOrDefault(ANNOTATION_SEVERITY, "1")))
				.type(EventType.PROBLEM)
				.summary(alert.getAnnotations().getOrDefault(ANNOTATION_DESCRIPTION, ""))
				.source(SOURCE)
				.sourceKey(SOURCEKEY)
				.fields(fields)
				.build();
	}
	
}
