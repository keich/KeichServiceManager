package ru.keich.mon.servicemanager.event;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.entity.Entity;
import ru.keich.mon.servicemanager.query.predicates.QueryPredicate;

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
	private final BaseStatus status;
	private final String node;
	private final String summary;
	private final Instant endsOn;
	
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
			@JsonProperty(value = "deletedOn") Instant deletedOn,
			@JsonProperty(value = "endsOn") Instant endsOn) {
		super(id, version, source, sourceKey, fields, fromHistory, createdOn, updatedOn, deletedOn);
		this.type = type;
		this.status = status;
		this.node = node;
		this.summary = summary;
		this.endsOn = endsOn;
	}

	public static Set<Object> getEndsOnForIndex(Event event) {
		return Optional.ofNullable((Object)event.endsOn).map(Collections::singleton).orElse(Collections.emptySet());
	}
	
	@Override
	public boolean testQueryPredicate(QueryPredicate predicate) {
		var fieldName = predicate.getName();
		switch (fieldName) {
		case FIELD_ENDSON:
			return predicate.test(endsOn);
		case FIELD_NODE:
			return predicate.test(node);
		case FIELD_SUMMARY:
			return predicate.test(summary);
		}
		return super.testQueryPredicate(predicate);
	}
	
	@Override
	public String toString() {
		return "Event [id=" + getId() + ", type=" + type + ", status=" + status +
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
		
		@Override
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
			this.deletedOn,
			this.endsOn);
		}

		public Builder type(EventType type) {
			this.type = type;
			return this;
		}
		
		public Builder status(BaseStatus status) {
			this.status = status;
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
		
		public Builder endsOn(Instant endsOn) {
			this.endsOn = endsOn;
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
