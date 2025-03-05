package ru.keich.mon.servicemanager.history;

import java.util.List;
import java.util.function.Consumer;

public class HistoryQueueDummyImp<T> implements HistoryQueue<T> {

	@Override
	public void poll(Consumer<List<T>> consumer) {

	}

	@Override
	public void add(T value) {

	}

}
