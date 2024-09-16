package ru.keich.mon.servicemanager.item;

import java.util.Collections;
import java.util.Map;
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
public class ItemFilter {
	
	private final BaseStatus resultStatus;
	
	private final boolean usingResultStatus;
	
	private final Map<String, String> equalFields;

	@Override
	public String toString() {
		return "ItemFilter [resultStatus=" + resultStatus + ", usingResultStatus="
				+ usingResultStatus + ", equalFields=" + equalFields + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 71;
		int result = resultStatus.hashCode() + (usingResultStatus ? 435 : 913);
		result = prime * result;
		result = result + prime * equalFields.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ItemFilter other = (ItemFilter) obj;
		if(equalFields.size() != other.equalFields.size()) {
			return false;
		}
		return equalFields.equals(other.equalFields)
				&& resultStatus == other.resultStatus && usingResultStatus == other.usingResultStatus;
	}

	@JsonCreator
	public ItemFilter(
			@JsonProperty("resultStatus") BaseStatus resultStatus,
			@JsonProperty("usingResultStatus") boolean usingResultStatus,
			@JsonProperty(value = "equalFields", required = true) Map<String, String> equalFields) {
		super();
		
		this.resultStatus = Optional.ofNullable(resultStatus).orElse(BaseStatus.INDETERMINATE);
		this.usingResultStatus = Optional.ofNullable(usingResultStatus).orElse(false);
		this.equalFields = Optional.ofNullable(equalFields).map(Collections::unmodifiableMap).orElse(Collections.emptyMap());
	}

}
