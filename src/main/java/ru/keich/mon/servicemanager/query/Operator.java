package ru.keich.mon.servicemanager.query;

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


public enum Operator {
	NE, EQ, LT, GT, CO, NC, ERROR, ALL;
	
	public static Operator fromString(String str) {
        switch(str.toUpperCase()) {
        case "NE":
            return NE;
        case "EQ":
            return EQ;
        case "LT":
            return LT;
        case "GT":
            return GT;
        case "CO":
            return CO;
        case "NC":
            return NC;
        }
        return ERROR;
	}
}
