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

public class GreaterThanPredicate extends QueryPredicate {

	public GreaterThanPredicate(String name, Object value) {
		super(name, Operator.GT, value);
	}

	@Override
	public boolean test(Object t) {
		var v = (Comparable) value;
		return v.compareTo(t) < 0;
	}

}
