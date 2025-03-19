package ru.keich.mon.servicemanager.item;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import lombok.extern.java.Log;
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
@Log
public class ItemController extends EntityController<String, Item> {

	public static final String QUERY_CHILDREN = "children";
	public static final String QUERY_PARENTS = "parents";
	
	public static final String JSON_FILTER_FIELD = "parents";
	
	private final ItemService itemService;
	
	public ItemController(ItemService itemService) {
		super(itemService, new SimpleFilterProvider()
				.addFilter(FILTER_NAME, SimpleBeanPropertyFilter
						.serializeAllExcept(Set.of(Item.FIELD_EVENTSSTATUS, Item.FIELD_EVENTS))));
		this.itemService = itemService;
	}
	
	@Override
	protected SimpleFilterProvider getJsonFilter(MultiValueMap<String, String> reqParam){
		if(reqParam.containsKey(QUERY_PROPERTY)) {
			var properties = reqParam.get(QUERY_PROPERTY).stream().collect(Collectors.toSet());
			properties.add(QUERY_ID);
			return new SimpleFilterProvider().addFilter(FILTER_NAME, SimpleBeanPropertyFilter.filterOutAllExcept(properties));
		}
		return jsonDefaultFilter;
	}

	@Override
	@PostMapping("/item")
	public ResponseEntity<String> addOrUpdate(@RequestBody List<Item> items) {
		return super.addOrUpdate(items);
	}

	@Override
	@GetMapping("/item")
	@CrossOrigin(origins = "*")
	public ResponseEntity<MappingJacksonValue> find(@RequestParam MultiValueMap<String, String> reqParam) {
		return super.find(reqParam, Item::fieldValueOf);
	}

	boolean needEvents(List<String> property) {
		return Optional.ofNullable(property)
				.map(p -> p.stream().collect(Collectors.toSet()).contains(Item.FIELD_EVENTS))
				.orElse(false);
	}
	
	Item fillEvents(Item item) {
		var outItem = new Item.Builder(item);
		outItem.setEvents(itemService.findAllEventsById(item.getId()));
		return outItem.build();
	}
	
	List<Item> fillEvents(List<Item> items) {
		return items.stream().map(this::fillEvents).toList();
	}
	
	@Override
	@GetMapping("/item/{id}")
	@CrossOrigin(origins = "*")
	public ResponseEntity<MappingJacksonValue> findById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		if(needEvents(reqParam.get(QUERY_PROPERTY))) {
			return itemService.findById(id)
					.map(this::fillEvents)
					.map(MappingJacksonValue::new)
					.map(value -> applyFilter(value, reqParam))
					.orElse(ResponseEntity.notFound().build());
		}
		return super.findById(id, reqParam);
	}

	@GetMapping("/item/{id}/children")
	@CrossOrigin(origins = "*")
	ResponseEntity<MappingJacksonValue> findChildrenById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		var items = itemService.findChildrenById(id);
		if(needEvents(reqParam.get(QUERY_PROPERTY))) {
			items = fillEvents(items);
		}
		return applyFilter(new MappingJacksonValue(items), reqParam);
	}
	
	@GetMapping("/item/{id}/parents")
	@CrossOrigin(origins = "*")
	ResponseEntity<MappingJacksonValue> findParentsById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		var items = itemService.findParentsById(id);
		if(needEvents(reqParam.get(QUERY_PROPERTY))) {
			items = fillEvents(items);
		}
		return applyFilter(new MappingJacksonValue(itemService.findParentsById(id)), reqParam);
	}

	@Override
	@DeleteMapping("/item")
	@CrossOrigin(origins = "*")
	public ResponseEntity<Integer> deleteByFilter(@RequestBody(required = false) List<String> ids, @RequestParam Map<String, String> reqParam) {
		return super.deleteByFilter(ids, reqParam);
	}
	
	@GetMapping("/item/{id}/events")
	@CrossOrigin(origins = "*")
	public ResponseEntity<MappingJacksonValue> findAllEventsById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		return applyFilter(new MappingJacksonValue(itemService.findAllEventsById(id)), reqParam);
	}
	
	@GetMapping("/item/{id}/events/history")
	@CrossOrigin(origins = "*")
	public ResponseEntity<MappingJacksonValue> findAllHistoryEventsById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		if(!reqParam.containsKey("from") || !reqParam.containsKey("to")) {
			return ResponseEntity.badRequest().build();
		}
		var from = Instant.parse(reqParam.get("from").get(0));
		var to = Instant.parse(reqParam.get("to").get(0));

		return applyFilter(new MappingJacksonValue(itemService.findAllHistoryEventsById(id, from, to)), reqParam);
	}

	@GetMapping("/item/{id}/tree")
	@CrossOrigin(origins = "*")
	// TODO rename children/tree
	ResponseEntity<MappingJacksonValue> getTree(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		if(reqParam.containsKey(QUERY_PROPERTY)) {
			reqParam.add(QUERY_PROPERTY, QUERY_CHILDREN);
		}
		return itemService.findById(id)
				.map(parent -> setChildren(parent, new HashSet<String>()))
				.map(parent -> applyFilter(new MappingJacksonValue(parent), reqParam))
				.orElse(ResponseEntity.notFound().build());
	}
	
	private Item setChildren(Item parent, HashSet<String> history) {
		var parentId = parent.getId();
		history.add(parentId);
		var outItem = new Item.Builder(parent);
		var l = parent.getChildrenIds().stream()
				.filter(childId -> {
					if (history.contains(childId)) {
						log.warning("setChildren: circle found from " + parentId + " to " + childId);
						return false;
					}
					return true;
				})
				.map(itemService::findById)
				.filter(Optional::isPresent)
				.map(opt -> setChildren(opt.get(), history))
				.toList();
		outItem.setChildren(l);
		history.remove(parentId);
		return outItem.build();
	}
	
	@GetMapping("/item/{id}/parents/tree")
	@CrossOrigin(origins = "*")
	ResponseEntity<MappingJacksonValue> findParentsTreeById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		if(reqParam.containsKey(QUERY_PROPERTY)) {
			reqParam.add(QUERY_PROPERTY, QUERY_PARENTS);
		}
		return itemService.findById(id)
				.map(child -> setParents(child, new HashSet<String>()))
				.map(child -> applyFilter(new MappingJacksonValue(child), reqParam))
				.orElse(ResponseEntity.notFound().build());
	}
	
	private Item setParents(Item child, HashSet<String> history) {
		var childId = child.getId();
		history.add(childId);
		var outItem = new Item.Builder(child);
		var l = itemService.findParentsById(childId).stream()
				.filter(parent -> {
					if (history.contains(parent.getId())) {
						log.warning("setParents: circle found from " + parent.getId() + " to " + childId);
						return false;
					}
					return true;
				})
				.map(parent -> setParents(parent, history))
				.toList();
		outItem.setParents(l);
		history.remove(childId);
		return outItem.build();
	}
	
}
