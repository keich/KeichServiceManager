package ru.keich.mon.query;

import lombok.Getter;

@Getter
public class Filter {
	final QueryId id;
	final String value;
	
	public Filter(String id, String value) {
		var arr = value.split(":", 2);
		if(arr.length == 2) {
			var operator = Operator.fromString(arr[0]);
			this.value = arr[1];
			this.id = new QueryId(id, operator);
		} else {
			this.value = value;
			this.id = new QueryId(id, Operator.ERROR);
		}
	}
	
	public Filter(QueryId id, String value) {
		this.value = value;
		this.id = id;
	}
	
	@Override
	public String toString() {
		return "Filter [id=" + id + ", value=" + value + "]";
	}	
	
}
