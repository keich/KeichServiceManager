package ru.keich.mon.servicemanager.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

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
	private final List<QuerySort> sorts = new ArrayList<QuerySort>();
	private final Set<String> properties = new HashSet<String>();
	private long limit = -1;
	private String search = "";
	private boolean needEvents = false;
	private boolean hasSearch = false;
	private final List<QueryPredicate> predicates = new ArrayList<QueryPredicate>();
	//private final List<Entry<String, List<String>>> filteredPrams;
	
	public QueryParamsParser(MultiValueMap<String, String> reqParam, BiFunction<String, String, Object> valueConverter) {	
		reqParam.entrySet().forEach(p -> {
			var param = p.getKey();
			var lowerParam = param.toLowerCase();
			var value = p.getValue();
			var size = value.size();
			
			switch (lowerParam) {
			case QUERY_LIMIT:
				if (size == 0) {
					throw new ErrorParsePredicateException("Param " + lowerParam + " is empty");
				} else if (size > 1) {
					throw new ErrorParsePredicateException("Param " + lowerParam + " has multiple values");
				}
				limit = Long.valueOf(value.get(0));
				break;
			case QUERY_SEARCH:
				if (size == 0) {
					throw new ErrorParsePredicateException("Param " + lowerParam + " is empty");
				} else if (size > 1) {
					throw new ErrorParsePredicateException("Param " + lowerParam + " has multiple values");
				}
				search = value.get(0);
				hasSearch = true;
				break;
			case QUERY_PROPERTY:
				if(size == 0) {
					throw new ErrorParsePredicateException("Param " + lowerParam + " is empty");
				}
				value.forEach(properties::add);
				properties.add(QUERY_ID);
				break;
			default:
				value.forEach(v -> {
					var qparam = getQueryParams(param, v, valueConverter);
					switch(qparam.getType()) {
					case SORT:
						sorts.add(qparam.getSort());
						break;
					case PREDICATE:
						predicates.add(qparam.getPredocate());
						break;
					}
				});
				break;
			}			
		});
		needEvents = properties.contains(Item.FIELD_EVENTS);
	}
	
	public void addProperty(String name) {
		properties.add(name);
	}
	
	public static class ErrorParsePredicateException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public ErrorParsePredicateException(String errorMessage) {
	        super(errorMessage);
	    }
		
	}
	
	public static QueryParam getQueryParams(String name, String value, BiFunction<String, String, Object> valueConverter) {
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
			case SORT:
				return new QuerySort(name, operator, Integer.valueOf(arr[1]));
			case SORTDESC:
				return new QuerySort(name, operator, Integer.valueOf(arr[1]));
			default:
				throw new ErrorParsePredicateException("Wrong operator: " + arr[0]);
			}
		} else {
			return QueryPredicate.equal(name, valueConverter.apply(name, value));
		}
	}

}
