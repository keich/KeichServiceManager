package ru.keich.mon.servicemanager;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import ru.keich.mon.servicemanager.store.IndexedHashMap.EmptyCounter;

/*
 * Copyright 2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class QueueThreadReader<K> {
	
	static final public String METRIC_NAME_QUEUE = "queueThreadReader_";
	static final public String METRIC_NAME_OPERATION = "operation";
	static final public String METRIC_NAME_SERVICENAME = "servicename";
	static final public String METRIC_NAME_ADDED = "added";
	static final public String METRIC_NAME_REMOVED = "removed";
	static final Integer POLL_SECONDS = 30;
	private final Consumer<K> consumer;
	private BlockingQueue<K> queue = new LinkedBlockingDeque<K>();
	private Counter metricAdded = EmptyCounter.EMPTY;
	private Counter metricRemoved = EmptyCounter.EMPTY;

	public QueueThreadReader(MeterRegistry registry, String name, int  number, Consumer<K> consumer) {
		super();
		if(registry != null) {
			metricAdded = registry.counter(METRIC_NAME_QUEUE + METRIC_NAME_OPERATION, METRIC_NAME_OPERATION,
					METRIC_NAME_ADDED, METRIC_NAME_SERVICENAME, name);
	
			metricRemoved = registry.counter(METRIC_NAME_QUEUE + METRIC_NAME_OPERATION, METRIC_NAME_OPERATION,
					METRIC_NAME_REMOVED, METRIC_NAME_SERVICENAME, name);
		}
		this.consumer = consumer;
		for (int i = 0; i < number; i++) {
			runThread(name + "-queue-" + i);
		}
	}

	private Thread runThread(String name) {
		var thread = new Thread(() -> {
			try {
				while (true) {
					Optional.ofNullable(queue.poll(POLL_SECONDS, TimeUnit.SECONDS)).ifPresent(this::remove);
				}
			} catch (Exception v) {
				v.printStackTrace();
			}
		}, name);
		thread.start();
		return thread;
	}

	public void add(K value) {
		metricAdded.increment();
		queue.add(value);
	}

	public void remove(K value) {
		metricRemoved.increment();
		consumer.accept(value);
	}

}
