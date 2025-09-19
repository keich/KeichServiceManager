package ru.keich.mon.servicemanager.query.predicates;

import ru.keich.mon.servicemanager.query.Operator;

public class ErrorPredicate extends QueryPredicate {

	public ErrorPredicate(String name, Object value) {
		super(name, Operator.ERROR, value);
	}

	@Override
	public boolean test(Object t) {
		return false;
	}

}
