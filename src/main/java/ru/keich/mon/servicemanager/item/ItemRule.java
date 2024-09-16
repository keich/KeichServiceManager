package ru.keich.mon.servicemanager.item;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import ru.keich.mon.servicemanager.BaseStatus;

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

@Getter
public class ItemRule {

	private final BaseStatus resultStatus;

	private final boolean usingResultStatus;

	private final BaseStatus statusThreshold;

	private final Integer valueThreshold;

	private final RuleType type;

	public enum RuleType {
		DEFAULT, CLUSTER
	}

	@Override
	public String toString() {
		return "ItemRule [resultStatus=" + resultStatus + ", usingResultStatus=" + usingResultStatus
				+ ", statusThreshold=" + statusThreshold + ", valueThreshold=" + valueThreshold + ", type=" + type
				+ "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(resultStatus, statusThreshold, type, usingResultStatus, valueThreshold);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ItemRule other = (ItemRule) obj;
		return resultStatus == other.resultStatus
				&& statusThreshold == other.statusThreshold && type == other.type
				&& usingResultStatus == other.usingResultStatus && Objects.equals(valueThreshold, other.valueThreshold);
	}

	@JsonCreator()
	public ItemRule(
			@JsonProperty("resultStatus") BaseStatus resultStatus,
			@JsonProperty("usingResultStatus") boolean usingResultStatus,
			@JsonProperty("statusThreshold") BaseStatus statusThreshold,
			@JsonProperty("valueThreshold") Integer valueThreshold,
			@JsonProperty(value = "type", required = true) RuleType type) {
		super();

		this.resultStatus = Optional.ofNullable(resultStatus).orElse(BaseStatus.INDETERMINATE);
		this.usingResultStatus = Optional.ofNullable(usingResultStatus).orElse(false);
		this.statusThreshold = Optional.ofNullable(statusThreshold).orElse(BaseStatus.CLEAR);
		this.valueThreshold = Optional.ofNullable(valueThreshold).orElse(0);
		this.type = type;
	}

}
