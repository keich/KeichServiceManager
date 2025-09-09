package ru.keich.mon.servicemanager.store;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

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

public class QueryLong<T> implements Query<T> {
	private final Function<T, Long> mapper;

	public QueryLong(Function<T, Long> mapper) {
		super();
		this.mapper = mapper;
	}

	@Override
	public Set<Object> apply(T o) {
		return Collections.singleton(mapper.apply(o));
	}

}
