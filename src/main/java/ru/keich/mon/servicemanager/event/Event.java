package ru.keich.mon.servicemanager.event;

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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.entity.Entity;

@Getter
public class Event extends Entity<String> {
	public enum EventType {
		NOTSET, PROBLEM, RESOLUTION, INFORMATION
	}
	
	private final EventType type;
	private final BaseStatus status;
	
	@JsonCreator
	public Event(@JsonProperty(value = "id", required = true) String id,
			@JsonProperty(value = "source", required = true) String source,
			@JsonProperty(value = "sourceKey", required = true) String sourceKey,
			@JsonProperty(value = "type", required = true) EventType type,
			@JsonProperty(value = "status", required = true) BaseStatus status,
			@JsonProperty("fields") Map<String, String> fields) {
		super(id.intern(), source, sourceKey, fields);
		this.type = type;
		this.status = status;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode() + prime * fields.size();
		result = prime * result + prime * status.hashCode() +  prime * type.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!super.equals(obj))
			return false;
		Event other = (Event) obj;
		if(fields.size() != other.fields.size()) {
			return false;
		}
		return super.equals(other) && status == other.status && type == other.type && fields.equals(other.fields);
	}

	@Override
	public String toString() {
		return "Event [id=" + getId() + ", type=" + type + ", status=" + status +
				", createdOn=" + getCreatedOn() + ", createdOn=" + getUpdatedOn() + ", deletedOn=" + getDeletedOn() +
				", fields=" + fields + "]";
	}
	
	public static Set<Object> getFieldsForIndex(Event event) {
		return event.fields.entrySet().stream().collect(Collectors.toSet());
	}
	

}
