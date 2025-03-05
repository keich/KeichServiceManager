package ru.keich.mon.servicemanager.history;

import java.util.List;
import java.util.function.Consumer;

public interface HistoryQueue<T> {
	public void poll(Consumer<List<T>> consumer);
	public void add(T value);
}
