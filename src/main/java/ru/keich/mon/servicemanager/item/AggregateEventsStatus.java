package ru.keich.mon.servicemanager.item;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import ru.keich.mon.servicemanager.BaseStatus;

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

public class AggregateEventsStatus {

	private class ResultStatus {
		BaseStatus status;
		Instant deletedOn = null;

		public ResultStatus(BaseStatus status) {
			super();
			this.status = status;
		}

		public ResultStatus(BaseStatus status, Instant deletedOn) {
			super();
			this.status = status;
			this.deletedOn = deletedOn;
		}

		public boolean isDeleted() {
			return deletedOn != null;
		}

	}

	private final BaseStatus maxStatus;
	private final Set<String> activeEventsIds;
	private final Map<String, ResultStatus> statuses;

	public AggregateEventsStatus(Map<String, ResultStatus> statuses, BaseStatus maxStatus, Set<String> activeEventsIds) {
		super();
		this.statuses = Collections.unmodifiableMap(statuses);
		this.maxStatus = maxStatus;
		this.activeEventsIds = Collections.unmodifiableSet(activeEventsIds);
	}

	public AggregateEventsStatus() {
		this.maxStatus = BaseStatus.CLEAR;
		this.statuses = Collections.emptyMap();
		this.activeEventsIds = Collections.emptySet();
	}

	public Set<String> getEventsIds() {
		return activeEventsIds;
	}

	private class updateContext {
		Map<String, ResultStatus> newStatuses = new HashMap<>();
		Instant nowTime = Instant.now();
		Instant ninutesAgoTime = Instant.now().minusSeconds(300); // TODO params?
		BaseStatus newMaxStatus = BaseStatus.CLEAR;
		Set<String> eventsIds = new HashSet<>();
	}

	private AggregateEventsStatus update(String eventId, Consumer<updateContext> c) {
		var ctx = new updateContext();
		for (var e : statuses.entrySet()) {
			if (e.getKey().equals(eventId)) {
				continue;
			}
			if (e.getValue().isDeleted()) {
				if (e.getValue().deletedOn.compareTo(ctx.ninutesAgoTime) < 0) {
					continue;
				}
			} else {
				if (ctx.newMaxStatus.lessThen(e.getValue().status)) {
					ctx.newMaxStatus = e.getValue().status;
				}
				ctx.eventsIds.add(e.getKey());
			}
			ctx.newStatuses.put(e.getKey(), e.getValue());
		}
		c.accept(ctx);
		return new AggregateEventsStatus(ctx.newStatuses, ctx.newMaxStatus, ctx.eventsIds);
	}

	public AggregateEventsStatus addEventStatus(String eventId, BaseStatus status) {
		return update(eventId, ctx -> {
			if (ctx.newMaxStatus.lessThen(status)) {
				ctx.newMaxStatus = status;
			}
			ctx.newStatuses.put(eventId, new ResultStatus(status));
			ctx.eventsIds.add(eventId);
		});
	}

	public AggregateEventsStatus removeEvent(String eventId) {
		return update(eventId, ctx -> {
			var es = statuses.get(eventId);
			ctx.newStatuses.put(eventId, new ResultStatus(es.status, ctx.nowTime));
		});
	}

	public BaseStatus getMaxStatus() {
		return this.maxStatus;
	}

	public Map<String, BaseStatus> getAggEventsStatus() {
		var out = new HashMap<String, BaseStatus>();
		Instant ninutesAgoTime = Instant.now().minusSeconds(300);
		for (var e : statuses.entrySet()) {
			if (e.getValue().isDeleted() && e.getValue().deletedOn.compareTo(ninutesAgoTime) < 0) {
				continue;
			}
			out.put(e.getKey(), e.getValue().status);
		}
		return out;
	}

}
