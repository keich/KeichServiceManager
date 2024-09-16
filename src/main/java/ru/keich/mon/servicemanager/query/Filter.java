package ru.keich.mon.servicemanager.query;

import lombok.Getter;

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
public class Filter {
	final QueryId id;
	final String value;
	
	public Filter(String id, String value) {
		var arr = value.split(":", 2);
		if(arr.length == 2) {
			var operator = Operator.fromString(arr[0]);
			this.value = arr[1];
			this.id = new QueryId(id, operator);
		} else {
			this.value = value;
			this.id = new QueryId(id, Operator.ERROR);
		}
	}
	
	public Filter(QueryId id, String value) {
		this.value = value;
		this.id = id;
	}
	
	@Override
	public String toString() {
		return "Filter [id=" + id + ", value=" + value + "]";
	}	
	
}
