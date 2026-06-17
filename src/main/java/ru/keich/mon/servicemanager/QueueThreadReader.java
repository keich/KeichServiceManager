package ru.keich.mon.servicemanager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
	private BlockingQueue<K> queue = new LinkedBlockingDeque<K>();

	public QueueThreadReader(Consumer<K> consumer) {
		super();
		Thread.startVirtualThread(() -> {
			while (true) {
				try {
					var info = queue.poll(POLL_SECONDS, TimeUnit.SECONDS);
					if (info != null) Thread.startVirtualThread(() -> consumer.accept(info));
				} catch (Exception v) {
					v.printStackTrace();
				}
			}
		});
	}

	public void add(K value) {
		queue.add(value);
	}

}
