package ru.keich.mon.servicemanager.entity;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import ru.keich.mon.servicemanager.query.QueryParamsParser;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.ser.std.SimpleBeanPropertyFilter;
import tools.jackson.databind.ser.std.SimpleFilterProvider;


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

public class EntityController<K, T extends Entity<K>> {

	private EntityService<K, T> entityService;

	public static final String FILTER_NAME = "propertiesFilter";

	protected final SimpleFilterProvider jsonDefaultFilter;

	public EntityController(EntityService<K, T> entityService) {
		super();
		this.entityService = entityService;
		this.jsonDefaultFilter = new SimpleFilterProvider().addFilter(FILTER_NAME, SimpleBeanPropertyFilter.serializeAll()).setFailOnUnknownId(false);
	}

	public ResponseEntity<String> addOrUpdate(@RequestBody List<T> objs) {
		objs.forEach(entityService::addOrUpdate);
		return ResponseEntity.ok("ok");
	}

	protected SimpleFilterProvider getJsonFilter(Set<String> properties){
		if(!properties.isEmpty()) {
			return new SimpleFilterProvider().addFilter(FILTER_NAME, SimpleBeanPropertyFilter.filterOutAllExcept(properties)).setFailOnUnknownId(false);
		}
		return jsonDefaultFilter;
	}

	protected ResponseEntity<String> applyFilter(Object obj, QueryParamsParser qp) {
		var jsonFilter = getJsonFilter(qp.getProperties());
		var mapper = JsonMapper.builder().filterProvider(jsonFilter).build();
		return ResponseEntity.ok(mapper.writeValueAsString(obj));
	}

	public ResponseEntity<String> find(MultiValueMap<String, String> reqParam) {
		return entityService.sortAndLimitEnrich(reqParam, entityService::find, (s, qp) -> applyFilter(s.toList(), qp));
	}

	public ResponseEntity<String> findById(K id, MultiValueMap<String, String> reqParam) {
		return entityService.sortAndLimitEnrich(reqParam, qp -> entityService.findById(id).stream(), (s, qp) -> { 
			var opt = s.findFirst();
			if(opt.isEmpty()) {
				return ResponseEntity.notFound().build();
			}
			return applyFilter(opt.get(), qp);
		});
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
				var ret =  entityService.deleteBySourceAndSourceKeyNot(source, sourceKey).size();
				return ResponseEntity.ok(ret);
			}
		}
		return ResponseEntity.notFound().build();
	}

}
