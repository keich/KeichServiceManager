package ru.keich.mon.servicemanager.query;

public interface QueryParam {

	static enum QueryParamType {
		SORT, PREDICATE
	}

	public default QueryPredicate getPredocate() {
		return null;
	}

	public default QuerySort getSort() {
		return null;
	}

	public QueryParamType getType();

}
