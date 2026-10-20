package ru.keich.mon.servicemanager.replication;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ReplicationStatus {

	private final boolean enable;
	private boolean firstReplicationComplete;

	public void firstReplicationComplete() {
		firstReplicationComplete = true;
		synchronized (this) {
			this.notifyAll();
		}
	}

	public void waitFirstReplicationComplete() throws InterruptedException {
		synchronized (this) {
			this.wait();
		}
	}

}
