package ru.keich.mon.servicemanager;

import lombok.Getter;

@Getter
public class QueueInfo {
	
	String id;
	QueueInfoType type;
	
	public QueueInfo(String id, QueueInfoType type) {
		this.id = id;
		this.type = type;
	}
	
	public enum QueueInfoType {
		UPDATE, UPDATED
	}
	
}
