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
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.java.Log;
import ru.keich.mon.servicemanager.entity.EntityController;
import ru.keich.mon.servicemanager.event.Event;

@RestController
@RequestMapping("/api/v1")
@Log
public class ItemController extends EntityController<String, Item> {

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
	public ResponseEntity<List<Item>> findByVersionGreaterThan(@RequestParam Map<String, String> reqParam) {
		return super.findByVersionGreaterThan(reqParam);
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
	public ResponseEntity<List<Event>> findAllEventsById(@PathVariable String id) {
		return ResponseEntity.ok(itemService.findAllEventsById(id));
	}

	@GetMapping("/item/{id}/tree")
	@CrossOrigin(origins = "*")
	ResponseEntity<ItemDTO> getTree(@PathVariable String id) {
		var history = new HashSet<String>();
		return itemService.findById(id).map(item -> {
			var root = new ItemDTO(item);
			return setChildren(root, history);
		}).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}
	
	private ItemDTO setChildren(ItemDTO root, HashSet<String> history) {
		var children = root.getChildrenIds().stream()
		.map(childId -> itemService.findById(childId))
		.filter(opt -> opt.isPresent())
		.map(opt -> new ItemDTO(opt.get()))
		.collect(Collectors.toList());
		
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
