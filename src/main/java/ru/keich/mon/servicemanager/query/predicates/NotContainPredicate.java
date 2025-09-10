package ru.keich.mon.servicemanager.query.predicates;

import java.util.Map.Entry;
import java.util.function.Predicate;

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

public class NotContainPredicate extends QueryPredicate {

	private final Predicate<Object> func;

	public NotContainPredicate(String name, Object value) {
		super(name, Operator.NC, value);
		if (value instanceof Entry) {
			func = (o) -> {
				var entry1 = (Entry) o;
				var entry2 = (Entry) value;
				if (entry1.getKey().equals(entry2.getKey())) {
					return !entry1.getValue().toString().contains(entry2.getValue().toString());
				}
				return false;
			};
		} else {
			func = (t) -> !t.toString().contains(value.toString());
		}
	}

	@Override
	public boolean test(Object o) {
		return func.test(o);
	}
	
}
