package ru.keich.mon.servicemanager.item;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

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

		this.resultStatus = resultStatus == null ? BaseStatus.INDETERMINATE : resultStatus;
		this.usingResultStatus = usingResultStatus;
		this.statusThreshold = statusThreshold == null ? BaseStatus.CLEAR : statusThreshold;
		this.valueThreshold = valueThreshold == null ? 0 : valueThreshold;
		this.type = type;
	}
	
	public BaseStatus getStatus(List<BaseStatus> statuses) {
		switch(type) {
		case CLUSTER:
			return doCluster(statuses);
		default:
			return doDefault(statuses);
		}
	}
	
	private BaseStatus doCluster(List<BaseStatus> statuses) {
		if(statuses.isEmpty()) {
			return BaseStatus.CLEAR;
		}
		
		var minStatus = BaseStatus.CRITICAL;
		var size = 0;
		for(var status: statuses) {
			if(statusThreshold.lessThenOrEqual(status)) {
				minStatus = minStatus.min(status);
				size++;
			}
		}
		
		var percent = 100 * size / statuses.size();
		if (percent >= valueThreshold) {
			if (usingResultStatus) {
				return getResultStatus();
			}
			return minStatus;
		}
		
		return BaseStatus.CLEAR;
	}
	
	public static BaseStatus doDefault(List<BaseStatus> statuses) {
		return BaseStatus.max(statuses);
	}
	
	public static BaseStatus max(Collection<ItemRule> rules, List<BaseStatus> statuses) {
		var maxStatus = BaseStatus.CLEAR;
		for(var rule: rules) {
			maxStatus = maxStatus.max(rule.getStatus(statuses));
		}
		return maxStatus;
	}
	
	public static BaseStatus min(Collection<ItemRule> rules, List<BaseStatus> statuses) {
		var minStatus = BaseStatus.CRITICAL;
		for(var rule: rules) {
			minStatus = minStatus.min(rule.getStatus(statuses));
		}
		return minStatus;
	}

}
