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

	public static final String QUERY_CHILDREN= "children";
	
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
		return ResponseEntity.ok(itemService.findChildren(id));
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
	ResponseEntity<MappingJacksonValue> getTree(@PathVariable String id, @RequestParam MultiValueMap<String, String> reqParam) {
		var history = new HashSet<String>();
		return itemService.findById(id).map(item -> {
			var root = new ItemDTO(item);
			return setChildren(root, history);
		})
		.map(item -> {
			if(reqParam.containsKey(QUERY_PROPERTY)) {
				reqParam.add(QUERY_PROPERTY, QUERY_CHILDREN);
			}
			final SimpleFilterProvider jsonFilter = getJsonFilter(reqParam);
			var value = new MappingJacksonValue(item);
			value.setFilters(jsonFilter);
			return ResponseEntity.ok(value);
		})
		.orElse(ResponseEntity.notFound().build());
	}
	
	private ItemDTO setChildren(ItemDTO root, HashSet<String> history) {
		var children = root.getChildrenIds().stream()
		.map(childId -> itemService.findById(childId))
		.filter(opt -> opt.isPresent())
		.map(opt -> new ItemDTO(opt.get()))
		.toList();
		
		history.add(root.getId());
		children.forEach(child -> {
			if (history.contains(child.getId())) {
				log.warning("setChildren: circle found from " + root.getId() + " to " + child.getId());
			} else {
				setChildren(child, history);
			}
		});
		root.setChildren(children);
		history.remove(root.getId());
		return root;
	}
}
