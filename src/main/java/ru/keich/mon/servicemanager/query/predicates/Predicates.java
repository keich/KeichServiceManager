package ru.keich.mon.servicemanager.query.predicates;

import java.util.function.BiFunction;

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
	
	public static <C> QueryPredicate fromParam(String name, String value, BiFunction<String, String, Object> valueConverter) {
		var arr = value.split(":", 2);
		if (arr.length == 2) {
			var operator = Operator.fromString(arr[0]);
			switch (operator) {
			case EQ:
				return equal(name, valueConverter.apply(name, arr[1]));
			case NE:
				return notEqual(name, valueConverter.apply(name, arr[1]));
			case LT:
				return lessThan(name, valueConverter.apply(name, arr[1]));
			case GT:
				return greaterThan(name, valueConverter.apply(name, arr[1]));
			case GE:
				return greaterEqual(name, valueConverter.apply(name, arr[1]));
			case CO:
				return contain(name, valueConverter.apply(name, arr[1]));
			case NC:
				return notContain(name, valueConverter.apply(name, arr[1]));
			case NI:
				return notInclude(name, valueConverter.apply(name, arr[1]));
			default:
				return error(name, value);
			}
		} else {
			return equal(name, valueConverter.apply(name, value));
		}
	}
	
	public static QueryPredicate error(String name, Object value) {
		return new ErrorPredicate(name, value);
	}
	
	public static QueryPredicate equal(String name, Object value) {
		return new EqualPredicate(name, value);
	}
	
	public static QueryPredicate notEqual(String name, Object value) {
		return new NotEqualPredicate(name, value);
	}
	
	public static QueryPredicate greaterEqual(String name, Object value) {
		return new GreaterEqualPredicate(name, value);
	}
	
	public static QueryPredicate greaterThan(String name, Object value) {
		return new GreaterThanPredicate(name, value);
	}
	
	public static QueryPredicate lessThan(String name, Object value) {
		return new LessThanPredicate(name, value);
	}
	
	public static QueryPredicate contain(String name, Object value) {
		return new ContainPredicate(name, value);
	}
	
	public static QueryPredicate notContain(String name, Object value) {
		return new NotContainPredicate(name, value);
	}
	
	public static QueryPredicate notInclude(String name, Object value) {
		return new NotIncludePredicate(name, value);
	}
	
}
