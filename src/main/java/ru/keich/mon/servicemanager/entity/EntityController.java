package ru.keich.mon.servicemanager.entity;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import lombok.Getter;
import ru.keich.mon.indexedhashmap.query.Operator;
import ru.keich.mon.indexedhashmap.query.QuerySort;
import ru.keich.mon.indexedhashmap.query.predicates.Predicates;
import ru.keich.mon.indexedhashmap.query.predicates.QueryPredicate;

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
	public static final String QUERY_PROPERTY = "property";
	public static final String QUERY_LIMIT = "limit";
	public static final String QUERY_ID = "id";
	public static final String FILTER_NAME = "propertiesFilter";
	
	protected final SimpleFilterProvider jsonDefaultFilter;
	
	public EntityController(EntityService<K, T> entityService, SimpleFilterProvider jsonDefaultFilter) {
		super();
		this.entityService = entityService;
		this.jsonDefaultFilter = jsonDefaultFilter;
	}
	
	public EntityController(EntityService<K, T> entityService) {
		super();
		this.entityService = entityService;
		this.jsonDefaultFilter = new SimpleFilterProvider().addFilter(FILTER_NAME, SimpleBeanPropertyFilter.serializeAll());
	}
	
	public ResponseEntity<String> addOrUpdate(@RequestBody List<T> objs) {
		objs.forEach(entityService::addOrUpdate);
		return ResponseEntity.ok("ok");
	}
	
	protected SimpleFilterProvider getJsonFilter(Set<String> properties){
		if(!properties.isEmpty()) {
			return new SimpleFilterProvider().addFilter(FILTER_NAME, SimpleBeanPropertyFilter.filterOutAllExcept(properties));
		}
		return jsonDefaultFilter;
	}
	
	protected ResponseEntity<MappingJacksonValue> applyFilter(MappingJacksonValue data, Set<String> properties) {
		final SimpleFilterProvider jsonFilter = getJsonFilter(properties);
		data.setFilters(jsonFilter);
		return ResponseEntity.ok(data);
	}
	
	public ResponseEntity<MappingJacksonValue> find(@RequestParam MultiValueMap<String, String> reqParam) {
		return find(reqParam, Entity::fieldValueOf);
	}
	
	public <C> ResponseEntity<MappingJacksonValue> find(MultiValueMap<String, String> reqParam, BiFunction<String, String, Object> valueConverter) {
		var qp = new QueryParse(reqParam, valueConverter);		
		var data = entityService.sortAndLimit(entityService.query(qp.getFilters()), qp.getSorts(), qp.getLimit())
				.collect(Collectors.toList());
		return applyFilter(new MappingJacksonValue(data), qp.getProperties());
	}

	public ResponseEntity<MappingJacksonValue> findById(@PathVariable K id, @RequestParam MultiValueMap<String, String> reqParam, BiFunction<String, String, Object> valueConverter) {
		var qp = new QueryParse(reqParam, valueConverter);	
		return entityService.findById(id)
				.map(MappingJacksonValue::new)
				.map(value -> applyFilter(value, qp.getProperties()))
				.orElse(ResponseEntity.notFound().build());
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
	
	@Getter
	public static class QueryParse {
		private final List<QueryPredicate> filters;
		private final List<QuerySort> sorts;
		private final Set<String> properties;
		private final long limit;
		
		public QueryParse(MultiValueMap<String, String> reqParam, BiFunction<String, String, Object> valueConverter) {	
			if (reqParam.containsKey(QUERY_LIMIT)) {
				limit = Long.valueOf(reqParam.get(QUERY_LIMIT).get(0));
			} else {
				limit = -1;
			}
			
			if(reqParam.containsKey(QUERY_PROPERTY)) {
				properties = reqParam.get(QUERY_PROPERTY).stream().collect(Collectors.toSet());
				properties.add(QUERY_ID);
			} else {
				properties = Collections.emptySet();
			}
			
			var filteredPrams = reqParam.entrySet().stream()
					.filter(p -> !p.getKey().toLowerCase().equals(QUERY_PROPERTY))
					.filter(p -> !p.getKey().toLowerCase().equals(QUERY_LIMIT))
					.collect(Collectors.toList());
			
			this.filters = filteredPrams.stream()
					.flatMap(param -> {
						return param.getValue().stream()
								.map(value -> Predicates.fromParam(param.getKey(), value, valueConverter));
					})
					.filter(p -> p.getOperator() != Operator.ERROR)
					.collect(Collectors.toList());
			
			this.sorts = filteredPrams.stream()
					.flatMap(param -> {
						return param.getValue().stream().map(value -> QuerySort.fromParam(param.getKey(), value));
					})
					.filter(s -> s.getOperator() != Operator.ERROR)
					.sorted(QuerySort::compareTo)
					.collect(Collectors.toList());
		}
		
	}
	
}
