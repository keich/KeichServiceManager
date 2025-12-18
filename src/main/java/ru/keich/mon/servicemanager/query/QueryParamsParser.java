package ru.keich.mon.servicemanager.query;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.springframework.util.MultiValueMap;

import lombok.Getter;
import ru.keich.mon.indexedhashmap.query.Operator;
import ru.keich.mon.indexedhashmap.query.predicates.Predicates;
import ru.keich.mon.indexedhashmap.query.predicates.QueryPredicate;
import ru.keich.mon.servicemanager.item.Item;

/*
 * Copyright 2025 the original author or authors.
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

@Getter
public class QueryParamsParser {
	public static final String QUERY_SEARCH = "search";
	public static final String QUERY_PROPERTY = "property";
	public static final String QUERY_LIMIT = "limit";
	public static final String QUERY_ID = "id";
	private final List<QuerySort> sorts;
	private final Set<String> properties;
	private final long limit;
	private final String search;
	private final boolean needEvents;
	private final boolean hasSearch;
	private final List<Entry<String, List<String>>> filteredPrams;
	
	public QueryParamsParser(MultiValueMap<String, String> reqParam) {	
		if (reqParam.containsKey(QUERY_LIMIT)) {
			limit = Long.valueOf(reqParam.get(QUERY_LIMIT).get(0));
		} else {
			limit = -1;
		}
		
		if(reqParam.containsKey(QUERY_SEARCH)) {
			search = reqParam.get(QUERY_SEARCH).get(0);
			hasSearch = true;
		} else {
			search = "";
			hasSearch = false;
		}
		
		if(reqParam.containsKey(QUERY_PROPERTY)) {
			properties = reqParam.get(QUERY_PROPERTY).stream().collect(Collectors.toSet());
			properties.add(QUERY_ID);
		} else {
			properties = Collections.emptySet();
		}
		
		if (this.getProperties().contains(Item.FIELD_EVENTS)) {
			this.needEvents = true;
		} else {
			this.needEvents = false;
		}
		
		this.filteredPrams = reqParam.entrySet().stream()
				.filter(p -> !p.getKey().toLowerCase().equals(QUERY_PROPERTY))
				.filter(p -> !p.getKey().toLowerCase().equals(QUERY_LIMIT))
				.filter(p -> !p.getKey().toLowerCase().equals(QUERY_SEARCH))
				.collect(Collectors.toList());
		
		this.sorts = filteredPrams.stream()
				.flatMap(param -> {
					return param.getValue().stream().map(value -> QuerySort.fromParam(param.getKey(), value));
				})
				.filter(s -> s.getOperator() != Operator.ERROR)
				.sorted(QuerySort::compareTo)
				.collect(Collectors.toList());
	}
	
	public Set<String> getPropertiesWith(String name) {
		if (!this.getProperties().isEmpty()) {
			var ret = new HashSet<String>(this.getProperties());
			ret.add(name);
			return ret;
		}
		return this.getProperties();
	}
	
	public List<QueryPredicate> getPredicates(BiFunction<String, String, Object> valueConverter) {
		return filteredPrams.stream()
				.flatMap(param -> {
					return param.getValue().stream()
							.map(value -> Predicates.fromParam(param.getKey(), value, valueConverter));
				})
				.filter(p -> p.getOperator() != Operator.ERROR)
				.collect(Collectors.toList());
	}

}
