package ru.keich.mon.servicemanager.entity;

import java.time.Instant;
import java.util.Optional;

public class EntityReplicationState {
	private volatile boolean active = false;
	private volatile boolean first = true;
	private String neighborStartTime = "";
	private Long maxVersion = 0L;
	private Long minVersion = Long.MAX_VALUE;
	private Long added = 0L;
	private Long deleted = 0L;
	
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
	}

	public void setActiveFalse() {
		this.active = false;
	}
	
	public void updateVersion(Long version) {
		if(minVersion > version) {
			minVersion = version;
		}
		if(maxVersion < version) {
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
		Optional.ofNullable(deletedOn)
		.ifPresentOrElse(d -> deleted++, () -> added++);
	}

	public void reset() {
		minVersion = Long.MAX_VALUE;
		added = 0L;
		deleted = 0L;
	}
	
	@Override
	public String toString() {
		return " minVersion: " + minVersion + " maxVersion: " + maxVersion + " added: " + added + " deleted: "
				+ deleted;
	}
	
}
