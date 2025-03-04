package ru.keich.mon.servicemanager.query.predicates;

import java.util.Set;

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

	public NotContainPredicate(String name, Object value) {
		super(name, Operator.NC, value);
	}

	@Override
	public boolean test(Object t) {
		if (t instanceof Set) {
			var set = (Set) t;
			return !set.contains(value);
		}
		return !t.toString().contains(value.toString());
	}
}
