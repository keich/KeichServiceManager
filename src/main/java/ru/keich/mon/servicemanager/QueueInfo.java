package ru.keich.mon.servicemanager;

import lombok.Getter;

@Getter
public class QueueInfo<K> {
	
	K id;
	QueueInfoType type;
	
	public QueueInfo(K id, QueueInfoType type) {
		this.id = id;
		this.type = type;
	}
	
	public enum QueueInfoType {
		UPDATE, UPDATED, REMOVED
	}
	
}
