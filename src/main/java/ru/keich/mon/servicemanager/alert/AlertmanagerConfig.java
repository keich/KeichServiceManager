package ru.keich.mon.servicemanager.alert;

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
public class AlertmanagerConfig {
	String original = """
receivers:
  - name: 'KeichServiceManager'
route:
  group_by: [...]
  group_wait: 30s
  group_interval: 1m
  receiver: 'KeichServiceManager'
			""";
}