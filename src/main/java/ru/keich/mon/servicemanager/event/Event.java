package ru.keich.mon.servicemanager.event;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import ru.keich.mon.servicemanager.BaseStatus;
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
	public enum EventType {
		NOTSET, PROBLEM, RESOLUTION, INFORMATION
	}
	
	private final EventType type;
	private final BaseStatus status;
	private final String node;
	private final String summary;
	
	@JsonCreator
	public Event(@JsonProperty(value = "id", required = true) String id,
			@JsonProperty(value = "version", required = false) Long version,
			@JsonProperty(value = "source", required = true) String source,
			@JsonProperty(value = "sourceKey", required = true) String sourceKey,
			@JsonProperty(value = "node") String node,
			@JsonProperty(value = "summary") String summary,
			@JsonProperty(value = "type", required = true) EventType type,
			@JsonProperty(value = "status", required = true) BaseStatus status,
			@JsonProperty("fields") Map<String, String> fields,
			@JsonProperty("fromHistory") Set<String> fromHistory,
			@JsonProperty(value = "createdOn") Instant createdOn,
			@JsonProperty(value = "updatedOn") Instant updatedOn,
			@JsonProperty(value = "deletedOn") Instant deletedOn) {
		super(id, version, source, sourceKey, fields, fromHistory, createdOn, updatedOn, deletedOn);
		this.type = type;
		this.status = status;
		this.node = node;
		this.summary = summary;
	}

	@Override
	public String toString() {
		return "Event [id=" + getId() + ", type=" + type + ", status=" + status +
				", createdOn=" + getCreatedOn() + ", createdOn=" + getUpdatedOn() + ", deletedOn=" + getDeletedOn() +
				", fields=" + getFields() + "]";
	}

	public static class Builder {
		protected final String id;
		protected Long version;
		protected String source;
		protected String sourceKey;
		protected Instant createdOn;
		protected Instant updatedOn;
		protected Instant deletedOn;
		protected Set<String> fromHistory;
		protected Map<String, String> fields;
		protected String node;
		String summary;
		EventType type;
		BaseStatus status;

		public Builder(String id) {
			this.id = id;
		}
		
		public Builder(Event event) {
			this.id = event.getId();
			this.version = event.getVersion();
			this.source = event.getSource();
			this.sourceKey = event.getSourceKey();
			this.fields = event.getFields();
			this.fromHistory = event.getFromHistory();
			this.createdOn = event.getCreatedOn();
			this.updatedOn = event.getUpdatedOn();
			this.deletedOn = event.getDeletedOn();
			this.summary = event.getSummary();
			this.type = event.getType();
			this.status = event.getStatus();

			this.node = event.getNode();
			if (Objects.isNull(this.node)) {
				var fieldsNode = event.getFields().get("node");
				if (Objects.nonNull(fieldsNode)) {
					this.node = fieldsNode;
				}
			}

			this.summary = event.getSummary();
			if (Objects.isNull(summary)) {
				var fieldsSummary = event.getFields().get("summary");
				if (Objects.nonNull(fieldsSummary)) {
					this.summary = fieldsSummary;
				}
			}
		}
		
		public Event build() {
			return new Event(this.id,
			this.version,
			this.source,
			this.sourceKey,
			this.node,
			this.summary,
			this.type,
			this.status,
			this.fields,
			this.fromHistory,
			this.createdOn,
			this.updatedOn,
			this.deletedOn);
		}

		public Builder type(EventType type) {
			this.type = type;
			return this;
		}
		
		public Builder status(BaseStatus status) {
			this.status = status;
			return this;
		}
		
		public Builder source(String source) {
			this.source = source;
			return this;
		}
		
		public Builder sourceKey(String sourceKey) {
			this.sourceKey = sourceKey;
			return this;
		}
		
		public Builder version(Long version) {
			this.version = version;
			return this;
		}
		
		public Builder summary(String summary) {
			this.summary = summary;
			return this;
		}
		
		public Builder node(String node) {
			this.node = node;
			return this;
		}

		public Builder createdOn(Instant createdOn) {
			this.createdOn = createdOn;
			return this;
		}
		
		public Builder updatedOn(Instant updatedOn) {
			this.updatedOn = updatedOn;
			return this;
		}

		public Builder deletedOn(Instant deletedOn) {
			this.deletedOn = deletedOn;
			return this;
		}

		public Builder fromHistory(Set<String> fromHistory) {
			this.fromHistory = fromHistory;
			return this;
		}
		
	}
}
