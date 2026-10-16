package ru.keich.mon.servicemanager.replication;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;


@Service
@ConditionalOnProperty(name = "replication.neighbor", havingValue = "null", matchIfMissing = true)
public class ReplicationServiceDummy implements ReplicationService {

	private final ReplicationStatus status = new ReplicationStatus(false, true);

	@Override
	public ReplicationStatus getStatus() {
		return status;
	}

}
