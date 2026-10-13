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
	private String neighborStartTime = "";
	private Long maxVersion = 0L;
	private Long added = 0L;
	private Long deleted = 0L;
	private Instant startTime = Instant.now();

	public void setNeighborStartTime(String time) {
		neighborStartTime = time;
	}

	public String getNeighborStartTime() {
		return neighborStartTime;
	}

	public void firstRun() {
		neighborStartTime = "";
		maxVersion = 0L;
		reset();
	}

	public boolean isFirstRun() {
		return maxVersion == 0L;
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
	}

	public Long getMaxVersion() {
		return maxVersion;
	}

	public void setMaxVersion(Long maxVersion) {
		this.maxVersion = maxVersion;
	}

	public Long getAdded() {
		return added;
	}

	public Long getDeleted() {
		return deleted;
	}

	public void incrementCounters(Instant deletedOn) {
		if (deletedOn == null) {
			added++;
		} else {
			deleted++;
		}
	}

	public void reset() {
		added = 0L;
		deleted = 0L;
	}

	@Override
	public String toString() {
		return " maxVersion: " + maxVersion + " added: " + added + " deleted: " + deleted + " time spent "
				+ (Instant.now().toEpochMilli() - startTime.toEpochMilli()) + " milliseconds";
	}

}
