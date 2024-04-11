package ru.keich.mon.servicemanager.entity;

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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

public class EntityController<K, T extends Entity<K>> {

	private EntityService<K, T> entityService;
	
	public EntityController(EntityService<K, T> entityService) {
		super();
		this.entityService = entityService;
	}
	
	public ResponseEntity<String> addOrUpdate(@RequestBody List<T> objs) {
		objs.forEach(e -> entityService.addOrUpdate(e));
		return ResponseEntity.ok("ok");
	}
	
	public ResponseEntity<List<T>> findByVersionGreaterThan(@RequestParam Map<String, String> reqParam) {
		if (reqParam.containsKey("version") && reqParam.containsKey("ignoreNodeName")) {
			Long version = Long.valueOf(reqParam.get("version"));
			String ignoreNodeName = reqParam.get("ignoreNodeName");
			var entyties = entityService.findByVersionGreaterThan(version).stream()
					.filter(e -> !e.getFromHistory().contains(ignoreNodeName))
					.collect(Collectors.toList());
			return ResponseEntity.ok(entyties);
		}
		return ResponseEntity.notFound().build();
	}
	
	public ResponseEntity<T> findById(@PathVariable K id) {
		return entityService.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
	}

	public ResponseEntity<Integer> deleteByFilter(@RequestBody(required = false) List<K> enties, @RequestParam Map<String, String> reqParam) {
		if (enties != null) {
			var ret = entityService.deleteByIds(enties);
			return ResponseEntity.ok(ret.size());
		}

		if (reqParam.containsKey("query") && reqParam.containsKey("source")
				&& reqParam.containsKey("sourceKey")) {
			var query = reqParam.get("query");
			var source = reqParam.get("source");
			var sourceKey = reqParam.get("sourceKey");

			if ("bySourceAndSourceKeyNot".equalsIgnoreCase(query)) {
				var ret =  entityService.deleteBySourceAndSourceKeyNot(source, sourceKey)
				.size();
				return ResponseEntity.ok(ret);
			}
		}
		return ResponseEntity.notFound().build();
	}
}
