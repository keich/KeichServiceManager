package ru.keich.mon.servicemanager.item;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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

	public static String DEFAULT_NAME = "DefaultRule";
	public static Map<String, ItemRule> DEFAULT = Map.of(DEFAULT_NAME, new ItemRule(null, false, null, null, RuleType.DEFAULT));

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
	
	public static BaseStatus calculateMax(Collection<ItemRule> rules, List<Item> items) {
		var maxStatus = BaseStatus.CLEAR;
		for (var rule : rules) {
			var status = rule.calculate(items);
			if (maxStatus.lessThen(status)) {
				maxStatus = status;
			}
			if (maxStatus.equals(BaseStatus.MAX)) {
				break;
			}
		}
		return maxStatus;
	}

	public BaseStatus calculate(List<Item> items) {
		switch(type) {
		case CLUSTER:
			return doCluster(items);
		default:
			return doDefault(items);
		}
	}

	private BaseStatus doCluster(List<Item> items) {	
		if (items.size() == 0) {
			return BaseStatus.CLEAR;
		}
		int filteredCount = 0;
		var minStatus = BaseStatus.MAX;
		for(var item: items) {
			var status = item.getStatus();
			if (statusThreshold.lessThenOrEqual(status)) {
				if (minStatus.moreThen(status)) {
					minStatus = status;
				}
				filteredCount++;
			}
		}
		if (100 * filteredCount / items.size() >= valueThreshold) {
			if (usingResultStatus) {
				return getResultStatus();
			}
			return minStatus;
		}
		return BaseStatus.CLEAR;
	}

	public static BaseStatus doDefault(List<Item> items) {
		var maxStatus = BaseStatus.CLEAR;
		for(var item: items) {
			var status = item.getStatus();
			if(maxStatus.lessThen(status)) {
				maxStatus = status;
			}
			if(maxStatus.equals(BaseStatus.MAX)) {
				break;
			}
		}
		return maxStatus;
	}

}
