package ru.keich.mon.servicemanager.item;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import ru.keich.mon.servicemanager.event.EventService;
import ru.keich.mon.servicemanager.query.QueryParamsParser;

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
	private final EventService eventService;
	
	public ItemController(ItemService itemService, EventService eventService) {
		super(itemService,
				new SimpleFilterProvider().addFilter(FILTER_NAME,
						SimpleBeanPropertyFilter
								.serializeAllExcept(Set.of(Item.FIELD_EVENTSSTATUS, Item.FIELD_EVENTS))));
		this.itemService = itemService;
		this.eventService = eventService;
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
		return super.find(reqParam);
	}
	
	Item fillEvents(Item item) { //TODO sort?
		return new Item.Builder(item)
				.setEvents(itemService.findAllEventsById(item.getId()).toList())
				.build();
	}
	
	List<Item> fillEvents(List<Item> items) {
		return items.stream()
				.map(this::fillEvents)
				.toList();
	}
	
	@GetMapping("/item/{id}")
	@CrossOrigin(origins = "*")
	public ResponseEntity<MappingJacksonValue> findById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		var qp = new QueryParamsParser(reqParam);	
		return itemService.findById(id)
				.map(item -> {
					if(qp.isNeedEvents()) {
						return fillEvents(item);
					}
					return item;
				})
				.map(MappingJacksonValue::new)
				.map(value -> applyFilter(value, qp.getProperties()))
				.orElse(ResponseEntity.notFound().build());
	}

	@GetMapping("/item/{id}/children")
	@CrossOrigin(origins = "*")
	ResponseEntity<MappingJacksonValue> findChildrenById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		var opt = itemService.findChildrenById(id);
		if(opt.isEmpty()) {
			return ResponseEntity.notFound().build();
		}
		var qp = new QueryParamsParser(reqParam);
		var data = itemService.sortAndLimit(opt.get(), qp.getSorts(), qp.getLimit());
		if(qp.isNeedEvents()) {
			data = data.map(this::fillEvents);
		}
		return applyFilter(new MappingJacksonValue(data.collect(Collectors.toList())), qp.getProperties());
	}
	
	@GetMapping("/item/{id}/parents")
	@CrossOrigin(origins = "*")
	ResponseEntity<MappingJacksonValue> findParentsById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		var opt = itemService.findParentsById(id);
		if(opt.isEmpty()) {
			return ResponseEntity.notFound().build();
		}
		var qp = new QueryParamsParser(reqParam);
		var data = itemService.sortAndLimit(opt.get(), qp.getSorts(), qp.getLimit());
		if(qp.isNeedEvents()) {
			data = data.map(this::fillEvents);
		}
		return applyFilter(new MappingJacksonValue(data.collect(Collectors.toList())), qp.getProperties());
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
		var qp = new QueryParamsParser(reqParam);
		var data = itemService.findAllEventsById(id);
		data = eventService.sortAndLimit(data, qp.getSorts(), qp.getLimit());
		return applyFilter(new MappingJacksonValue(data), qp.getProperties());
	}
	
	@GetMapping("/item/{id}/tree")
	@CrossOrigin(origins = "*")
	// TODO rename children/tree
	ResponseEntity<MappingJacksonValue> getTree(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		var qp = new QueryParamsParser(reqParam);
		return itemService.findById(id)
				.map(parent -> setChildren(parent, new HashSet<String>()))
				.map(parent -> applyFilter(new MappingJacksonValue(parent), qp.getPropertiesWith(QUERY_CHILDREN)))
				.orElse(ResponseEntity.notFound().build());
	}
	
	private Item setChildren(Item parent, HashSet<String> history) {
		var parentId = parent.getId();
		history.add(parentId);
		var outItem = new Item.Builder(parent);
		var l = itemService.findChildren(parent)
				.filter(child -> {
					if (history.contains(child.getId())) {
						log.warning("setChildren: circle found from " + parentId + " to " + child.getId());
						return false;
					}
					return true;
				})
				.map(child -> setChildren(child, history))
				.toList();
		outItem.setChildren(l);
		history.remove(parentId);
		return outItem.build();
	}
	
	@GetMapping("/item/{id}/parents/tree")
	@CrossOrigin(origins = "*")
	ResponseEntity<MappingJacksonValue> findParentsTreeById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		var qp = new QueryParamsParser(reqParam);
		return itemService.findById(id)
				.map(child -> setParents(child, new HashSet<String>()))
				.map(child -> applyFilter(new MappingJacksonValue(child), qp.getPropertiesWith(QUERY_PARENTS)))
				.orElse(ResponseEntity.notFound().build());
	}
	
	private Item setParents(Item child, HashSet<String> history) {
		var childId = child.getId();
		history.add(childId);
		var outItem = new Item.Builder(child);
		itemService.findParentsById(childId).ifPresent(stream -> {
			var l = stream.filter(parent -> {
				if (history.contains(parent.getId())) {
					log.warning("setParents: circle found from " + parent.getId() + " to " + childId);
					return false;
				}
				return true;
			})
			.map(parent -> setParents(parent, history))
			.toList();
			outItem.setParents(l);
		});
		history.remove(childId);
		return outItem.build();
	}

}
