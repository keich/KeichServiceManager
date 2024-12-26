package ru.keich.mon.servicemanager.entity;

public class ChangedNeighborStartTimeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public ChangedNeighborStartTimeException(String errorMessage) {
        super(errorMessage);
    }
	
}
