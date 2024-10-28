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
	
	public static QueryPredicate fromParam(String name, String value) {
		var arr = value.split(":", 2);
		if (arr.length == 2) {
			var operator = Operator.fromString(arr[0]);
			value = arr[1];
			switch (operator) {
			case NE:
				return notEqual(name, value);
			case LT:
				return lessThan(name, Long.valueOf(arr[1]));
			case GT:
				return greaterEqual(name, Long.valueOf(arr[1]));
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
	
	public static  QueryPredicate equal(String name, Object value) {
		return new EqualPredicate(name, value);
	}
	
	public static QueryPredicate notEqual(String name, Object value) {
		return new NotEqualPredicate(name, value);
	}
	
	public static  QueryPredicate greaterEqual(String name, Object value) {
		return new GreaterEqualPredicate(name, value);
	}
	
	public static  QueryPredicate lessThan(String name, Object value) {
		return new LessThanPredicate(name, value);
	}
	
	public static  QueryPredicate contain(String name, Object value) {
		return new ContainPredicate(name, value);
	}
	
	public static QueryPredicate notContain(String name, Object value) {
		return new NotContainPredicate(name, value);
	}
	
}
