package ru.keich.mon.servicemanager.event;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

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
	public static final String FIELD_ITEMIDS = "itemIds";
	public static final String FIELD_CALCULATED = "calculated";
	
	public enum EventType {
		NOTSET, PROBLEM, RESOLUTION, INFORMATION
	}
	
	private final EventType type;
	private final String node;
	private final String summary;
	private final Instant endsOn;
	private final Set<String> itemIds;
	private final Boolean calculated;
	
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
			@JsonProperty(value = "endsOn") Instant endsOn,
			@JsonProperty(value = "itemIds") Set<String> itemIds,
			@JsonProperty(value = "calculated") Boolean calculated) {
		super(id, version, source, sourceKey, sourceType, fields, fromHistory, createdOn, updatedOn, deletedOn, status);
		this.type = type;
		this.node = node;
		this.summary = summary;
		this.endsOn = endsOn;
		this.itemIds = itemIds;
		this.calculated = calculated;
	}

	public static Set<Object> getEndsOnForIndex(Event event) {
		return event.endsOn == null ? Collections.emptySet() : Collections.singleton(event.endsOn);
	}
	
	public static Integer getCalculatedForIndex(Event event) {
		return event.calculated ? 1 : 0;
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
				", createdOn=" + getCreatedOn() + ", updatedOn=" + getUpdatedOn() + ", deletedOn=" + getDeletedOn() +
				", fields=" + getFields() + "]";
	}
	
	@Getter
	public static class Builder extends Entity.Builder<String, Event> {
		protected String node;
		protected String summary;
		protected EventType type;
		protected BaseStatus status;
		protected Instant endsOn;
		protected Set<String> itemIds;
		protected Boolean calculated;

		public Builder(String id) {
			super(id);
		}

		public Builder(Event event) {
			super(event);
			summary = event.getSummary();
			type = event.getType();
			status = event.getStatus();
			endsOn = event.getEndsOn();
			itemIds = event.getItemIds();
			node = event.getNode();
			summary = event.getSummary();
		}

		@Override
		public Event build() {
			return new Event(this.id,
			version,
			source,
			sourceKey,
			sourceType,
			node,
			summary,
			type,
			status,
			fields,
			fromHistory,
			createdOn,
			updatedOn,
			deletedOn,
			endsOn,
			itemIds,
			calculated);
		}

		public static Event.Builder getDefault(String id) {
			return new Event.Builder(id)
					.version(0L)
					.source("")
					.sourceKey("")
					.sourceType(SourceType.OTHER)
					.status(BaseStatus.CLEAR)
					.node("")
					.summary("")
					.type(EventType.PROBLEM)
					.status(BaseStatus.CRITICAL)
					.fields(Collections.emptyMap())
					.fromHistory(Collections.emptySet())
					.createdOn(Instant.now())
					.updatedOn(Instant.now())
					.itemIds(Collections.emptySet())
					.calculated(false);
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
			this.endsOn = endsOn;;
			return this;
		}

		public Builder itemIds(Set<String> itemIds) {
			this.itemIds = itemIds;
			return this;
		}
		
		public Builder itemIdsUpdate(Consumer<Set<String>> s) {
			itemIds = new HashSet<>(itemIds);
			s.accept(itemIds);
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
		public Builder fields(Map<String, String> fields) {
			super.fields(fields);
			return this;
		}
		
		public Builder calculated(Boolean calculated) {
			this.calculated = calculated;
			return this;
		}

	}

	public static Object fieldValueOf(String fieldName, String str) {
		switch (fieldName) {
		case FIELD_ENDSON:
			return Instant.parse(str);
		case FIELD_CALCULATED:
			return Boolean.valueOf(str) ? 1 : 0;
		}
		return Entity.fieldValueOf(fieldName, str);
	}

}
