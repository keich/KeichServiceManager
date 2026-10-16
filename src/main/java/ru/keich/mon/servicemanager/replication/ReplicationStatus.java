package ru.keich.mon.servicemanager.replication;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ReplicationStatus {

	private final boolean enable;
	private final boolean firstReplicationComplete;

}
