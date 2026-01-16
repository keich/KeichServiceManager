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
	
	public static class ErrorParsePredicateException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public ErrorParsePredicateException(String errorMessage) {
	        super(errorMessage);
	    }
		
	}
	
	public static <C> QueryPredicate getPredicatesFromParam(String name, String value,
			BiFunction<String, String, Object> valueConverter) {
		var arr = value.split(":", 2);
		if (arr.length == 2) {
			var operator = Operator.fromString(arr[0]);
			switch (operator) {
			case EQ:
				return QueryPredicate.equal(name, valueConverter.apply(name, arr[1]));
			case NE:
				return QueryPredicate.notEqual(name, valueConverter.apply(name, arr[1]));
			case LT:
				return QueryPredicate.lessThan(name, valueConverter.apply(name, arr[1]));
			case GT:
				return QueryPredicate.greaterThan(name, valueConverter.apply(name, arr[1]));
			case GE:
				return QueryPredicate.greaterEqual(name, valueConverter.apply(name, arr[1]));
			case CO:
				return QueryPredicate.contain(name, valueConverter.apply(name, arr[1]));
			case NC:
				return QueryPredicate.notContain(name, valueConverter.apply(name, arr[1]));
			case NI:
				return QueryPredicate.notInclude(name, valueConverter.apply(name, arr[1]));
			default:
				throw new ErrorParsePredicateException("Wrong operator: " + arr[0]);
			}
		} else {
			return QueryPredicate.equal(name, valueConverter.apply(name, value));
		}
	}
	
	public List<QueryPredicate> getPredicates(BiFunction<String, String, Object> valueConverter) {
		return filteredPrams.stream()
				.flatMap(param -> {
					return param.getValue().stream()
							.map(value -> getPredicatesFromParam(param.getKey(), value, valueConverter));
				})
				.collect(Collectors.toList());
	}

}
