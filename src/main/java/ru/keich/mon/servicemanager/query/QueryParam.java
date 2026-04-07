package ru.keich.mon.servicemanager.query;

public interface QueryParam {

	static enum QueryParamType {
		SORT, PREDICATE
	}

	public default QueryPredicate getQueryPredicate() {
		return null;
	}

	public default QuerySort getSort() {
		return null;
	}

	public QueryParamType getType();

}
