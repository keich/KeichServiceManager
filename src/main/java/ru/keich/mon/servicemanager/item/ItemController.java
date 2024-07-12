package ru.keich.mon.servicemanager.item;

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

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
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

import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.entity.EntityController;

@RestController
@RequestMapping("/api/v1")
@Log
public class ItemController extends EntityController<String, Item> {

	public static final String QUERY_CHILDREN = "children";
	public static final String QUERY_PARENTS = "parents";
	
	private final ItemService itemService;
	
	public ItemController(@Autowired  ItemService itemService) {
		super(itemService);
		this.itemService = itemService;
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

	@Override
	@GetMapping("/item/{id}")
	@CrossOrigin(origins = "*")
	public ResponseEntity<Item> findById(@PathVariable String id) {
		return super.findById(id);
	}

	@GetMapping("/item/{id}/children")
	@CrossOrigin(origins = "*")
	ResponseEntity<List<Item>> findChildrenById(@PathVariable String id) {
		return ResponseEntity.ok(itemService.findChildrenById(id));
	}
	
	@GetMapping("/item/{id}/parents")
	@CrossOrigin(origins = "*")
	ResponseEntity<List<Item>> findParentsById(@PathVariable String id) {
		return ResponseEntity.ok(itemService.findParentsById(id));
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
		var events = itemService.findAllEventsById(id);
		final SimpleFilterProvider jsonFilter = getJsonFilter(reqParam);
		var value = new MappingJacksonValue(events);
		value.setFilters(jsonFilter);
		return ResponseEntity.ok(value);
	}

	@GetMapping("/item/{id}/tree")
	@CrossOrigin(origins = "*")
	// TODO rename children/tree
	ResponseEntity<MappingJacksonValue> getTree(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		var history = new HashSet<String>();
		return itemService.findById(id)
		.map(parent -> {
			var parentDTO = new ItemDTO(parent);
			setChildren(parentDTO, history);
			if(reqParam.containsKey(QUERY_PROPERTY)) {
				reqParam.add(QUERY_PROPERTY, QUERY_CHILDREN);
			}
			final SimpleFilterProvider jsonFilter = getJsonFilter(reqParam);
			var value = new MappingJacksonValue(parentDTO);
			value.setFilters(jsonFilter);
			return ResponseEntity.ok(value);
		})
		.orElse(ResponseEntity.notFound().build());
	}
	
	private ItemDTO setChildren(ItemDTO parent, HashSet<String> history) {
		var children = itemService.findChildrenById(parent.getId()).stream()
		.map(child -> new ItemDTO(child))
		.toList();
		
		history.add(parent.getId());
		children.forEach(child -> {
			if (history.contains(child.getId())) {
				log.warning("setChildren: circle found from " + parent.getId() + " to " + child.getId());
			} else {
				setChildren(child, history);
			}
		});
		parent.setChildren(children);
		history.remove(parent.getId());
		return parent;
	}
	
	@GetMapping("/item/{id}/parents/tree")
	@CrossOrigin(origins = "*")
	ResponseEntity<MappingJacksonValue> findParentsTreeById(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		var history = new HashSet<String>();
		return itemService.findById(id)
		.map(child -> {
			var childTDO = new ItemDTO(child);
			setParents(childTDO, history);
			if(reqParam.containsKey(QUERY_PROPERTY)) {
				reqParam.add(QUERY_PROPERTY, QUERY_PARENTS);
			}
			final SimpleFilterProvider jsonFilter = getJsonFilter(reqParam);
			var value = new MappingJacksonValue(childTDO);
			value.setFilters(jsonFilter);
			return ResponseEntity.ok(value);
		})
		.orElse(ResponseEntity.notFound().build());
	}
	
	private ItemDTO setParents(ItemDTO child, HashSet<String> history) {
		var parents = itemService.findParentsById(child.getId()).stream()
		.map(parent -> new ItemDTO(parent))
		.toList();
		
		history.add(child.getId());
		parents.forEach(parent -> {
			if (history.contains(parent.getId())) {
				log.warning("setParents: circle found from " + parent.getId() + " to " + child.getId());
			} else {
				setParents(parent, history);
			}
		});
		child.setParents(parents);
		history.remove(child.getId());
		return child;
	}
	
}
