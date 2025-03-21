package ru.keich.mon.servicemanager;

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

import java.util.Objects;

public enum BaseStatus {
	CLEAR(0), INDETERMINATE(1), INFORMATION(2), WARNING(3), MAJOR(4), CRITICAL(5);
	
	private Integer status;
	
	private BaseStatus(int status) {
    	this.status = status;
	}

	static final public int length = 6;
	
	public static BaseStatus fromInteger(int x) {
        switch(x) {
        case 0:
            return CLEAR;
        case 1:
            return INDETERMINATE;
        case 2:
            return INFORMATION;
        case 3:
            return WARNING;
        case 4:
            return MAJOR;
        case 5:
            return CRITICAL;
        }
        if(x < 0) {
        	return CLEAR;
        }
        return CRITICAL;
    }
	
	public static BaseStatus fromString(String sev) {
        switch(sev) {
        case "0":
        case "clear":
        case "CLEAR":
            return CLEAR;
        case "1":
        case "indeterminate":
        case "INDETERMINATE":
            return INDETERMINATE;
        case "2":
        case "information":
        case "INFORMATION":
            return INFORMATION;
        case "3":
        case "warning":
        case "WARNING":
            return WARNING;
        case "4":
        case "major":
        case "MAJOR":
            return MAJOR;
        case "5":
        case "critical":
        case "CRITICAL":
            return CRITICAL;
        }
        return CLEAR; // TODO
    }
	
	public BaseStatus max(BaseStatus other) {
		if(Objects.nonNull(other)) {
			if(this.status < other.status) {
				return other;
			}
		}
		return this;
	}
	public boolean lessThen(BaseStatus other) {
		if(this.status < other.status) {
			return true;
		}
		return false;
	}
	
	public boolean lessThenOrEqual(BaseStatus other) {
		if(this.status < other.status) {
			return true;
		}
		if(this.status.equals(other.status)) {
			return true;
		}
		return false;
	}
	
	public boolean moreThen(BaseStatus other) {
		if(this.status > other.status) {
			return true;
		}
		return false;
	}
}
