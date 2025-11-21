package ru.keich.mon.servicemanager.entity;

import java.time.Instant;

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

public class EntityReplicationState {
	private volatile boolean active = false;
	private volatile boolean first = true;
	private String neighborStartTime = "";
	private Long maxVersion = 0L;
	private Long minVersion = Long.MAX_VALUE;
	private Long added = 0L;
	private Long deleted = 0L;
	private Instant startTime = Instant.now();
	private Instant endTime = Instant.now();
	
	public EntityReplicationState() {
		setFirstRunTrue();
	}
	
	public void setNeighborStartTime(String time) {
		neighborStartTime = time;
	}
	
	public String getNeighborStartTime() {
		return neighborStartTime;
	}
	
	public void setFirstRunTrue() {
		first = true;
		neighborStartTime = "";
		maxVersion = 0L;
		minVersion = Long.MAX_VALUE;
		reset();
	}
	
	public void setFirstRunFalse() {
		first = false;
	}
	
	public boolean isFirstRun() {
		return first;
	}

	public boolean isActive() {
		return active;
	}

	public void setActiveTrue() {
		this.active = true;
		startTime = Instant.now();
	}

	public void setActiveFalse() {
		this.active = false;
		endTime = Instant.now();
	}
	
	public void updateVersion(Long version) {
		if (minVersion > version) {
			minVersion = version;
		}
		if (maxVersion < version) {
			maxVersion = version;
		}
	}
	
	public Long getMaxVersion() {
		return maxVersion;
	}
	
	public Long getMinVersion() {
		return minVersion;
	}
	
	public Long getAdded() {
		return added;
	}
	
	public Long getDeleted() {
		return deleted;
	}
	
	public void incrementCounters(Instant deletedOn) {
		if(deletedOn == null) {
			added++;
		} else {
			deleted++;
		}
	}

	public void reset() {
		minVersion = Long.MAX_VALUE;
		added = 0L;
		deleted = 0L;
	}
	
	@Override
	public String toString() {
		final Instant time;
		if(active) {
			time = Instant.now();
		} else {
			time = endTime;
		}
		return " minVersion: " + minVersion + " maxVersion: " + maxVersion + " added: " + added + " deleted: "
				+ deleted + " time spent " + (time.toEpochMilli() - startTime.toEpochMilli()) + " milliseconds";
	}
	
}
