package ru.keich.mon.query;

import lombok.Getter;

@Getter
public class QueryId {
	private final String name;
	private final Operator operator;
	
	public QueryId(String name, Operator operator) {
		super();
		this.name = name;
		this.operator = operator;
	}

	@Override
	public int hashCode() {
		return name.hashCode() + operator.hashCode() * 9245287;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueryId other = (QueryId) obj;
		return name.equalsIgnoreCase(other.name) && operator == other.operator;
	}

	@Override
	public String toString() {
		return "QueryId [name=" + name + ", operator=" + operator + "]";
	}
	
}
