package ru.keich.mon.servicemanager.item;

import java.time.Instant;

import lombok.Getter;
import ru.keich.mon.servicemanager.BaseStatus;

/*
 * Copyright 2025 the original author or authors.
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

@Getter
public class AggregateStatus {
	public static final AggregateStatus EMPTY = new AggregateStatus();
	protected final Instant[] statuses;
	private final int lastStatus;
	private static long seconds = 10;

	public AggregateStatus(AggregateStatus obj, BaseStatus status) {
		statuses = obj.statuses.clone();
		lastStatus = status.getInt();
		statuses[lastStatus] = Instant.now();
	}

	private AggregateStatus() {
		lastStatus = 0;
		statuses = new Instant[BaseStatus.length];
		statuses[lastStatus] = Instant.now();
	}

	public BaseStatus getMax() {
		Instant now = Instant.now().minusSeconds(seconds);
		int maxStatus = lastStatus;
		for (int i = maxStatus; i < BaseStatus.length; i++) {
			if (statuses[i] != null && statuses[i].compareTo(now) >= 0) {
				maxStatus = i;
			}
		}
		return BaseStatus.fromInteger(maxStatus);
	}
	
	public static void setSeconds(long seconds) {
		AggregateStatus.seconds = seconds;
	}

}
