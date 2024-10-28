package ru.keich.mon.servicemanager.query.predicates;

import java.util.function.Predicate;

import lombok.Getter;
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

@Getter
public abstract class QueryPredicate implements Predicate<Object> {

	final String name;
	final Operator operator;
	final Object value;

	public QueryPredicate(String name, Operator operator, Object value) {
		this.value = value;
		this.name = name;
		this.operator = operator;
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

}
