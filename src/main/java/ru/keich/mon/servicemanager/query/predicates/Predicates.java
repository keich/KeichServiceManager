package ru.keich.mon.servicemanager.query.predicates;

import ru.keich.mon.servicemanager.query.Operator;

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

public class Predicates {
	
	public static QueryPredicate<?> fromParam(String name, String value) {
		var arr = value.split(":", 2);
		if (arr.length == 2) {
			value = arr[1];
			var operator = Operator.fromString(value);
			switch (operator) {
			case NE:
				return notEqual(name, value);
			case LT:
				return lessThan(name, arr[1]);
			case GT:
				return greaterEqual(name, arr[1]);
			case CO:
				return contain(name, arr[1]);
			case NC:
				return notContain(name, arr[1]);
			default:
				return equal(name, value);
			}
		} else {
			return equal(name, value);
		}
	}
	
	public static <T extends Comparable<T>> QueryPredicate<T> equal(String name, T value) {
		return new EqualPredicate<T>(name, value);
	}
	
	public static <T extends Comparable<T>> QueryPredicate<T> notEqual(String name, T value) {
		return new NotEqualPredicate<T>(name, value);
	}
	
	public static <T extends Comparable<T>> QueryPredicate<T> greaterEqual(String name, T value) {
		return new GreaterEqualPredicate<T>(name, value);
	}
	
	public static <T extends Comparable<T>> QueryPredicate<T> lessThan(String name, T value) {
		return new LessThanPredicate<T>(name, value);
	}
	
	public static <T extends Comparable<T>> QueryPredicate<T> contain(String name, T value) {
		return new ContainPredicate<T>(name, value);
	}
	
	public static <T extends Comparable<T>> QueryPredicate<T> notContain(String name, T value) {
		return new NotContainPredicate<T>(name, value);
	}
	
}
