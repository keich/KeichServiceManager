package ru.keich.mon.servicemanager.entity;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import ru.keich.mon.servicemanager.KSearchLexer;
import ru.keich.mon.servicemanager.KSearchParser;
import ru.keich.mon.servicemanager.query.QueryParamsParser;
import ru.keich.mon.servicemanager.query.QueryPredicate;
import ru.keich.mon.servicemanager.search.SearchListener;


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

	protected Stream<T> findByPredicates(List<QueryPredicate> predicates) {
		return predicates.stream()
				.map(p -> entityService.find(p))
				.reduce((result, el) -> { 
					result.retainAll(el);
					return result;
				})
				.orElse(Collections.emptySet())
				.stream()
				.map(entityService::findById)
				.filter(Optional::isPresent)
				.map(Optional::get);
	}

	protected Stream<T> findBySearch(String search) {
		var lexer = new KSearchLexer(CharStreams.fromString(search));
		var tokens = new CommonTokenStream(lexer);
		var parser = new KSearchParser(tokens);
		var tree = parser.parse();
		var walker = new ParseTreeWalker();
		var q = new SearchListener<K, T>(entityService);
		walker.walk(q, tree);
		return q.getResult()
				.stream()
				.map(entityService::findById)
				.filter(Optional::isPresent)
				.map(Optional::get);
	}
	
	protected Stream<T> find(QueryParamsParser qp) {
		if(qp.isHasSearch()) {
			return findBySearch(qp.getSearch());
		} else {
			return findByPredicates(qp.getPredicates(entityService::fieldValueOf));
		}
	}

	public ResponseEntity<MappingJacksonValue> find(MultiValueMap<String, String> reqParam) {
		var qp = new QueryParamsParser(reqParam);	
		var data = entityService.sortAndLimit(find(qp), qp.getSorts(), qp.getLimit()).collect(Collectors.toList());
		return applyFilter(new MappingJacksonValue(data), qp.getProperties());
	}

	public ResponseEntity<MappingJacksonValue> findById(@PathVariable K id, @RequestParam MultiValueMap<String, String> reqParam) {
		var qp = new QueryParamsParser(reqParam);	
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

}
