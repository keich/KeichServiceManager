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
	protected Instant[] statuses = new Instant[BaseStatus.length];
	private int lastStatus = 0;
	private static long seconds = 10;

	public AggregateStatus(AggregateStatus obj) {
		for (int i = 0; i < BaseStatus.length; i++) {
			this.statuses[i] = obj.statuses[i];
		}
		this.lastStatus = obj.lastStatus;
	}

	public AggregateStatus() {
		this.statuses[lastStatus] = Instant.now();
	}

	public void set(BaseStatus status) {
		lastStatus = status.getInt();
		statuses[lastStatus] = Instant.now();
	}

	public BaseStatus getMax() {
		Instant now = Instant.now().minusSeconds(seconds);
		int maxStatus = lastStatus;
		for (int i = 0; i < BaseStatus.length; i++) {
			var val = statuses[i];
			if (val == null) {
				continue;
			}
			if (val.compareTo(now) >= 0 && maxStatus < i) {
				maxStatus = i;
			}
		}
		return BaseStatus.fromInteger(maxStatus);
	}
	
	public static void setSeconds(long seconds) {
		AggregateStatus.seconds = seconds;
	}

}
