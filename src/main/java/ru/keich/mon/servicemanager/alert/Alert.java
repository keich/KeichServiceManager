package ru.keich.mon.servicemanager.alert;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

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
@Setter
public class Alert {
	
	private String startsAt;
	private String generatorURL = "";
	private String endsAt;
	private Map<String, String> labels = new HashMap<String, String>();
	private Map<String, String> annotations = new HashMap<String, String>();
	
	@Override
	public String toString() {
		return "Alert [startsAt=" + startsAt + ", generatorURL=" + generatorURL + ", endsAt=" + endsAt + ", labels="
				+ labels + ", annotations=" + annotations + "]";
	}
	
}
