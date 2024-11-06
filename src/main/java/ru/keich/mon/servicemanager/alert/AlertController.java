package ru.keich.mon.servicemanager.alert;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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

@RestController
public class AlertController {

	@Autowired 
	AlertService alertService;
	
	@Autowired 
	EventService eventService;
	
	@PostMapping("/api/v2/alerts")
	public List<String> alerts(@RequestBody List<Alert> alerts) {
		alerts.stream()
				.map(alertService::alertToEvent)
				.filter(event -> { 
					if(Objects.nonNull(event.getEndsOn())) {
						return Instant.now().isBefore(event.getEndsOn());
					}
					return true;
				}).forEach(eventService::addOrUpdate);
		return Collections.emptyList();
	}

	@GetMapping("/api/v2/alerts")
	public List<String> getAlerts(@RequestParam MultiValueMap<String, String> params) {
		return Collections.emptyList();
	}

	@GetMapping("/api/v1/status/buildinfo")
	public BuildInfo buildinfo() {
		return new BuildInfo();
	}

	@GetMapping("/api/v2/status")
	public AlertmanagerStatus status() {
		return new AlertmanagerStatus();
	}

	@GetMapping("/api/v2/alerts/groups")
	public List<String> groups() {
		return Collections.emptyList();
	}

	@GetMapping("/api/v2/silences")
	public List<String> getSilences() {
		return Collections.emptyList();
	}

	@PostMapping("/api/v2/silences")
	public Silence silences(@RequestBody String body) {
		var ret = new Silence();
		ret.setSilenceID("sdfsd");
		return ret;
	}
		
}
