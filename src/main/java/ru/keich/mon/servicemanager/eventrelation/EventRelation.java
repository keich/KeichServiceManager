package ru.keich.mon.servicemanager.eventrelation;

import java.util.Collections;
import java.util.Set;

import lombok.Getter;
import ru.keich.mon.servicemanager.BaseStatus;
import ru.keich.mon.servicemanager.store.BaseEntity;

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
public class EventRelation extends BaseEntity<EventRelationId> implements Comparable<EventRelation> {

	private final BaseStatus status;
	
	public EventRelation(EventRelationId id, BaseStatus status) {
		super(id);
		this.status = status;
	}
	
	public EventRelation(String itemId, String eventId, BaseStatus status) {
		super(new EventRelationId(itemId, eventId));
		this.status = status;
	}

	public static Set<Object> getEventIdsForCache(EventRelation relation) {
		return Collections.singleton(relation.getId().getEventId());
	}
	
	public static Set<Object> getItemIdsForCache(EventRelation relation) {
		return Collections.singleton(relation.getId().getItemId());
	}
	
	public static Set<Object> getItemStatusForCache(EventRelation relation) {
		return Collections.singleton(relation);
	}

	@Override
	public int compareTo(EventRelation rel) {
		var ret = getId().getItemId().compareTo(rel.getId().getItemId());
		if(ret == 0) {
			ret = -getStatus().compareTo(rel.getStatus()); 
			if(ret == 0) {
				return getId().getEventId().compareTo(rel.getId().getEventId());
			}
		}
		return ret;
	}
	
	@Override
	public String toString() {
		return "EventRelation [status=" + status + ", getId()=" + getId() + "]";
	}
}
