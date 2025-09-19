package ru.keich.mon.servicemanager.query;

import lombok.Getter;

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
public class QuerySort implements Comparable<QuerySort> {
	
	final String name;
	final Operator operator;
	final int order;

	public static QuerySort fromParam(String name, String value) {
		var arr = value.split(":", 2);
		if (arr.length == 2) {
			var operator = Operator.fromString(arr[0]);
			if (operator == Operator.SORT || operator == Operator.SORTDESC) {
				var order = Integer.valueOf(arr[1]);
				return new QuerySort(name, operator, order);
			}
		}
		return error(name, value);
	}
	
	public QuerySort(String name, Operator operator, int order) {
		this.name = name;
		this.operator = operator;
		this.order = order;
	}

	public static  QuerySort error(String name, Object value) {
		return new QuerySort(name, Operator.ERROR, 0);
	}


	@Override
	public int compareTo(QuerySort o) {
		return this.getOrder() - o.getOrder();
	}

	@Override
	public String toString() {
		return "QuerySort [name=" + name + ", operator=" + operator + ", order=" + order + "]";
	}
	
}
