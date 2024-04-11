package ru.keich.mon.servicemanager.eventrelation;

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

import lombok.Getter;

@Getter
public class EventRelationId {
	private final String itemId;
	private final String eventId;
	
	public EventRelationId(String itemId, String eventId) {
		super();
		this.itemId = itemId.intern();
		this.eventId = eventId.intern();
	}
	
	@Override
	public int hashCode() {
		return eventId.hashCode() +  itemId.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EventRelationId other = (EventRelationId) obj;
		return eventId.equals(other.eventId) && itemId.equals(other.itemId);
	}

	@Override
	public String toString() {
		return "EventRelationId [itemId=" + itemId + ", eventId=" + eventId + "]";
	}
	
}
