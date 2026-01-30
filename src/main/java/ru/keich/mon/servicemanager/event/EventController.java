package ru.keich.mon.servicemanager.event;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import ru.keich.mon.servicemanager.entity.EntityController;

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
@RequestMapping("/api/v1")
public class EventController extends EntityController<String, Event> {
	final EventService eventService;
	
	public EventController(EventService eventService) {
		super(eventService);
		this.eventService = eventService;
	}

	@Override
	@PostMapping("/event")
	public ResponseEntity<String> addOrUpdate(@RequestBody List<Event> events) {
		return super.addOrUpdate(events);
	}
	
	@Override
	@GetMapping("/event")
	@CrossOrigin(origins = "*")
	public ResponseEntity<MappingJacksonValue> find(@RequestParam MultiValueMap<String, String> reqParam) {
		return super.find(reqParam);
	}
	
	@GetMapping("/event/{id}")
	@CrossOrigin(origins = "*")
	public ResponseEntity<MappingJacksonValue> findById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		return super.findById(id, reqParam);
	}
	
	@Override
	@DeleteMapping("/event")
	@CrossOrigin(origins = "*")
	public ResponseEntity<Integer> deleteByFilter(@RequestBody(required = false) List<String> ids, @RequestParam Map<String, String> reqParam) {
		return super.deleteByFilter(ids, reqParam);
	}
	
}
