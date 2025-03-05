package ru.keich.mon.servicemanager.history;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import io.micrometer.core.instrument.Counter;
import ru.keich.mon.servicemanager.store.IndexedHashMap.EmptyCounter;

public class HistoryQueueImp<T> implements HistoryQueue<T> {
	private BlockingQueue<T> queue = new LinkedBlockingDeque<T>();
	private Counter metricAdded = EmptyCounter.EMPTY;
	private Counter metricRemoved = EmptyCounter.EMPTY;
	private final Integer limit;
	
	public HistoryQueueImp(Integer limit) {
		this.limit = limit;
	}
	
	@Override
	public void add(T value) {
		metricAdded.increment();
		queue.add(value);
	}

	@Override
	public void poll(Consumer<List<T>> consumer) {
		List<T> list = new ArrayList<T>();
		try {
			var element = queue.poll();
			while(element != null) {
				list.add(element);
				metricRemoved.increment();
				if(list.size() >= limit) {
					consumer.accept(list);
					list = new ArrayList<T>();
				}
				element = queue.poll();
			}
			if(list.size() > 0) {
				consumer.accept(list);
			}
		} catch (Exception v) {
			v.printStackTrace();
		}
	}
	
}
