package ru.keich.mon.servicemanager.query;

import java.util.Map.Entry;
import java.util.function.Predicate;

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

public class QueryPredicate {

	public static enum Operator {
		NE, EQ, LT, GT, GE, CO, NC, NI;
	}
	
	final String name;
	final Operator operator;
	final Object value;
	final Predicate<Object> predicate;

	public QueryPredicate(String name, Operator operator, Object value, Predicate<Object> predicate) {
		this.value = value;
		this.name = name;
		this.operator = operator;
		this.predicate = predicate;
	}

	public String getName() {
		return name;
	}

	public Object getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "QueryPredicate [name=" + name + ", operator=" + operator + ", value=" + value + "]";
	}

	public Operator getOperator() {
		return operator;
	}
	
	public Predicate<Object> getPredicate() {
		return predicate;
	}

	public static QueryPredicate equal(String name, Object value) {
		return new QueryPredicate(name, Operator.EQ, value, v -> value.equals(v));
	}

	public static QueryPredicate notEqual(String name, Object value) {
		return new QueryPredicate(name, Operator.NE, value, v -> !value.equals(v));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static QueryPredicate greaterEqual(String name, Object value) {
		var c = (Comparable) value;
		return new QueryPredicate(name, Operator.GE, value, v -> c.compareTo(v) <= 0);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static QueryPredicate greaterThan(String name, Object value) {
		var c = (Comparable) value;
		return new QueryPredicate(name, Operator.GT, value, v -> c.compareTo(v) < 0);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static QueryPredicate lessThan(String name, Object value) {
		var c = (Comparable) value;
		return new QueryPredicate(name, Operator.LT, value, v -> c.compareTo(v) > 0);
	}

	@SuppressWarnings("rawtypes")
	public static QueryPredicate contain(String name, Object value) {
		final Predicate<Object> predicate;
		if (value instanceof Entry) {
			predicate = (o) -> {
				var entry1 = (Entry) o;
				var entry2 = (Entry) value;
				if (entry1.getKey().equals(entry2.getKey())) {
					return entry1.getValue().toString().contains(entry2.getValue().toString());
				}
				return false;
			};
		} else {
			predicate = (t) -> t.toString().contains(value.toString());
		}
		return new QueryPredicate(name, Operator.CO, value, predicate);
	}

	@SuppressWarnings("rawtypes")
	public static QueryPredicate notContain(String name, Object value) {
		final Predicate<Object> predicate;
		if (value instanceof Entry) {
			predicate = (o) -> {
				var entry1 = (Entry) o;
				var entry2 = (Entry) value;
				if (entry1.getKey().equals(entry2.getKey())) {
					return !entry1.getValue().toString().contains(entry2.getValue().toString());
				}
				return false;
			};
		} else {
			predicate = (t) -> !t.toString().contains(value.toString());
		}
		return new QueryPredicate(name, Operator.NC, value, predicate);
	}

	public static QueryPredicate notInclude(String name, Object value) {
		final Predicate<Object> predicate =  v -> !v.toString().contains(value.toString());
		return new QueryPredicate(name, Operator.NI, value, predicate);
	}

}
