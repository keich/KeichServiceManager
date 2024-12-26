package ru.keich.mon.servicemanager.store;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

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

public interface Index <K, T> {	
	public void append(T entity);
	public void remove(T entity);
	public Set<K> get(Object key);
	public Set<K> getBefore(Object key);
	public Set<K> getAfterEqual(Object key);
	public Set<K> getAfterFirst(Object key);
	public Set<K> findByKey(long limit, Predicate<Object> predicate);
	public Set<K> valueSet();
	public AtomicInteger getMetricObjectsSize();
}
