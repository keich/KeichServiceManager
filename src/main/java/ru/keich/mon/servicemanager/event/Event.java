package ru.keich.mon.servicemanager.event;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.SourceType;
import ru.keich.mon.servicemanager.entity.Entity;

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
public class Event extends Entity<String> {
	
	public static final String FIELD_ENDSON = "endsOn";
	public static final String FIELD_NODE = "node";
	public static final String FIELD_SUMMARY = "summary";
	
	public enum EventType {
		NOTSET, PROBLEM, RESOLUTION, INFORMATION
	}
	
	private final EventType type;
	private final String node;
	private final String summary;
	private final Instant endsOn;
	
	@JsonCreator
	public Event(@JsonProperty(value = "id", required = true) String id,
			@JsonProperty(value = "version", required = false) Long version,
			@JsonProperty(value = "source", required = true) String source,
			@JsonProperty(value = "sourceKey", required = true) String sourceKey,
			@JsonProperty(value = "sourceType", required = false) SourceType sourceType,
			@JsonProperty(value = "node") String node,
			@JsonProperty(value = "summary") String summary,
			@JsonProperty(value = "type", required = true) EventType type,
			@JsonProperty(value = "status", required = true) BaseStatus status,
			@JsonProperty("fields") Map<String, String> fields,
			@JsonProperty("fromHistory") Set<String> fromHistory,
			@JsonProperty(value = "createdOn") Instant createdOn,
			@JsonProperty(value = "updatedOn") Instant updatedOn,
			@JsonProperty(value = "deletedOn") Instant deletedOn,
			@JsonProperty(value = "endsOn") Instant endsOn) {
		super(id, version, source, sourceKey, sourceType, fields, fromHistory, createdOn, updatedOn, deletedOn, status);
		this.type = type;
		this.node = node == null ? "" : node;
		this.summary = summary == null ? "" : summary;
		this.endsOn = endsOn;
	}

	public static Set<Object> getEndsOnForIndex(Event event) {
		return event.endsOn == null ? Collections.emptySet() : Collections.singleton(event.endsOn);
	}

	public static Set<Object> getNodeForQuery(Event event) {
		return Collections.singleton(event.getNode());
	}

	public static Set<Object> getSummaryForQuery(Event event) {
		return Collections.singleton(event.getSummary());
	}
	
	@Override
	public String toString() {
		return "Event [id=" + getId() + ", type=" + type + ", status=" + getStatus() +
				", createdOn=" + getCreatedOn() + ", createdOn=" + getUpdatedOn() + ", deletedOn=" + getDeletedOn() +
				", fields=" + getFields() + "]";
	}
	
	@Getter
	public static class Builder extends Entity.Builder<String, Event> {
		protected String node;
		protected String summary;
		protected EventType type;
		protected BaseStatus status;
		protected Instant endsOn;

		public Builder(String id) {
			super(id);
		}
		
		public Builder(Event event) {
			super(event);
			this.summary = event.getSummary();
			this.type = event.getType();
			this.status = event.getStatus();
			this.endsOn = event.getEndsOn();

			this.node = event.getNode();
			if (this.node == null) {
				var fieldsNode = event.getFields().get("node");
				if (fieldsNode != null) {
					this.node = fieldsNode;
				}
			}

			this.summary = event.getSummary();
			if (summary == null) {
				var fieldsSummary = event.getFields().get("summary");
				if (fieldsSummary != null) {
					this.summary = fieldsSummary;
				}
			}
		}
		
		@Override
		public Event build() {
			return new Event(this.id,
			this.version,
			this.source,
			this.sourceKey,
			this.sourceType,
			this.node,
			this.summary,
			this.type,
			this.status,
			this.fields,
			this.fromHistory,
			this.createdOn,
			this.updatedOn,
			this.deletedOn,
			this.endsOn);
		}

		public Builder type(EventType type) {
			this.type = type;
			this.changed = true;
			return this;
		}
		
		public Builder status(BaseStatus status) {
			this.status = status;
			this.changed = true;
			return this;
		}
		
		public Builder summary(String summary) {
			this.summary = summary;
			this.changed = true;
			return this;
		}
		
		public Builder node(String node) {
			this.node = node;
			this.changed = true;
			return this;
		}
		
		public Builder endsOn(Instant endsOn) {
			this.endsOn = endsOn;
			this.changed = true;
			return this;
		}

		@Override
		public Builder source(String source) {
			super.source(source);
			return this;
		}

		@Override
		public Builder sourceKey(String sourceKey) {
			super.sourceKey(sourceKey);
			return this;
		}

		@Override
		public Builder sourceType(SourceType sourceType) {
			super.sourceType(sourceType);
			return this;
		}

		@Override
		public Builder version(Long version) {
			super.version(version);
			return this;
		}

		@Override
		public Builder createdOn(Instant createdOn) {
			super.createdOn(createdOn);
			return this;
		}

		@Override
		public Builder updatedOn(Instant updatedOn) {
			super.updatedOn(updatedOn);
			return this;
		}

		@Override
		public Builder deletedOn(Instant deletedOn) {
			super.deletedOn(deletedOn);
			return this;
		}

		@Override
		public Builder fromHistory(Set<String> fromHistory) {
			super.fromHistory(fromHistory);
			return this;
		}

		@Override
		public Builder fromHistoryAdd(String value) {
			super.fromHistoryAdd(value);
			return this;
		}

		@Override
		public Builder fields(Map<String, String> fields) {
			super.fields(fields);
			return this;
		}
		
	}
	
	public static Object fieldValueOf(String fieldName, String str) {
		switch (fieldName) {
		case FIELD_ENDSON:
			return Instant.parse(str);
		}
		return Entity.fieldValueOf(fieldName, str);
	}
	
	
}
