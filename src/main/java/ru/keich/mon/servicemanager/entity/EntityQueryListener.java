package ru.keich.mon.servicemanager.entity;

import java.util.Set;
import java.util.Stack;
import java.util.function.BiFunction;

import ru.keich.mon.indexedhashmap.IndexedHashMap;
import ru.keich.mon.indexedhashmap.query.predicates.Predicates;
import ru.keich.mon.servicemanager.KQueryBaseListener;
import ru.keich.mon.servicemanager.KQueryParser.ExprANDContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprContainContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprEqualContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprGreaterEqualContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprGreaterThanContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprLessThanContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprNotContainContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprNotEqualContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprNotIncludeContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprORContext;

public class EntityQueryListener<K, T> extends KQueryBaseListener {

	private final IndexedHashMap<K, T> entityCache;
	private final Stack<Set<K>> stack = new Stack<>();
	private final BiFunction<String, String, Object> valueConverter;

	public EntityQueryListener(IndexedHashMap<K, T> entityCache, BiFunction<String, String, Object> valueConverter) {
		this.entityCache = entityCache;
		this.valueConverter = valueConverter;
	}

	@Override
	public void exitExprAND(ExprANDContext ctx) {
		var result = stack.pop();
		result.retainAll(stack.pop());
		stack.push(result);
	}

	@Override
	public void exitExprOR(ExprORContext ctx) {
		var result = stack.pop();
		result.addAll(stack.pop());
		stack.push(result);
	}

	@Override
	public void exitExprNotEqual(ExprNotEqualContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.notEqual(name, valueConverter.apply(name, value));
		var r = entityCache.keySet(p);
		stack.push(r);
	}

	@Override
	public void exitExprEqual(ExprEqualContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.equal(name, valueConverter.apply(name, value));
		var r = entityCache.keySet(p);
		stack.push(r);
	}

	@Override
	public void exitExprNotContain(ExprNotContainContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.notContain(name, valueConverter.apply(name, value));
		var r = entityCache.keySet(p);
		stack.push(r);
	}

	@Override
	public void exitExprNotInclude(ExprNotIncludeContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.notInclude(name, valueConverter.apply(name, value));
		var r = entityCache.keySet(p);
		stack.push(r);
	}

	@Override
	public void exitExprLessThan(ExprLessThanContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.lessThan(name, valueConverter.apply(name, value));
		var r = entityCache.keySet(p);
		stack.push(r);
	}

	@Override
	public void exitExprGreaterEqual(ExprGreaterEqualContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.greaterEqual(name, valueConverter.apply(name, value));
		var r = entityCache.keySet(p);
		stack.push(r);
	}

	@Override
	public void exitExprContain(ExprContainContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.contain(name, valueConverter.apply(name, value));
		var r = entityCache.keySet(p);
		stack.push(r);
	}

	@Override
	public void exitExprGreaterThan(ExprGreaterThanContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.greaterThan(name, valueConverter.apply(name, value));
		var r = entityCache.keySet(p);
		stack.push(r);
	}

	private String pullString(ExprContext ctx, int idx) {
		var value = ctx.getChild(idx).getText();
		if('"' == value.charAt(0)) {
			return value.substring(1, value.length()-1);
		}
		return value;
	}
	
	public Set<K> getResult() {
		return stack.pop();
	}

}
