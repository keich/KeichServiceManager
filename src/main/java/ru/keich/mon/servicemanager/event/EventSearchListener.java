package ru.keich.mon.servicemanager.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.antlr.v4.runtime.tree.ParseTree;

import ru.keich.mon.servicemanager.KEventSearchBaseListener;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprANDContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprContainContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprEqualContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprFieldsContainContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprFieldsEqualContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprFieldsInEqualContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprGreaterEqualContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprGreaterThanContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprInEqualContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprLessThanContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprNotContainContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprNotEqualContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprNotIncludeContext;
import ru.keich.mon.servicemanager.KEventSearchParser.ExprORContext;
import ru.keich.mon.servicemanager.entity.Entity;
import ru.keich.mon.servicemanager.entity.EntitySearchResult;
import ru.keich.mon.servicemanager.query.QueryPredicate;


public class EventSearchListener extends KEventSearchBaseListener implements EntitySearchResult<String> {
	private final EventService eventService;
	private final Stack<Set<String>> stack = new Stack<>();

	public EventSearchListener(EventService eventService) {
		this.eventService = eventService;
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
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.notEqual(name, eventService.fieldValueOf(name, value));
		var r = eventService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprEqual(ExprEqualContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.equal(name, eventService.fieldValueOf(name, value));
		var r = eventService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprNotContain(ExprNotContainContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 3);
		var p = QueryPredicate.notContain(name, eventService.fieldValueOf(name, value));
		var r = eventService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprNotInclude(ExprNotIncludeContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 3);
		var p = QueryPredicate.notInclude(name, eventService.fieldValueOf(name, value));
		var r = eventService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprLessThan(ExprLessThanContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.lessThan(name, eventService.fieldValueOf(name, value));
		var r = eventService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprGreaterEqual(ExprGreaterEqualContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.greaterEqual(name, eventService.fieldValueOf(name, value));
		var r = eventService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprContain(ExprContainContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.contain(name, eventService.fieldValueOf(name, value));
		var r = eventService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprGreaterThan(ExprGreaterThanContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.greaterThan(name, eventService.fieldValueOf(name, value));
		var r = eventService.find(p);
		stack.push(r);
	}

	private String getFieldName(ExprContext ctx, int idx) {
		return pullString(ctx, idx);
	}

	private String pullString(ExprContext ctx, int idx) {
		var value = ctx.getChild(idx).getText();
		if ('"' == value.charAt(0) || '\'' == value.charAt(0)) {
			return value.substring(1, value.length() - 1);
		}
		return value;
	}

	@Override
	public void exitExprFieldsInEqual(ExprFieldsInEqualContext ctx) {
		var key = pullString(ctx, 2);
		var list = new ArrayList<String>();
		childToList(ctx.getChild(5), list);
		var r = list.stream()
		.map(str -> Map.entry(key, str))
		.map(e -> QueryPredicate.equal(Event.FIELD_FIELDS, e))
		.map(p -> eventService.find(p))
		.reduce((result, el) -> {
			result.addAll(el);
			return result;
		}).orElse(Collections.emptySet());
		stack.push(r);
	}

	@Override
	public void exitExprFieldsContain(ExprFieldsContainContext ctx) {
		var key = pullString(ctx, 2);
		var value = pullString(ctx, 4);
		var entry = Map.entry(key, value);
		var p = QueryPredicate.contain(Entity.FIELD_FIELDS, entry);
		var r = eventService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprFieldsEqual(ExprFieldsEqualContext ctx) {
		var key = pullString(ctx, 2);
		var value = pullString(ctx, 4);
		var entry = Map.entry(key, value);
		var p = QueryPredicate.equal(Entity.FIELD_FIELDS, entry);
		var r = eventService.find(p);
		stack.push(r);
	}

	private void childToList(ParseTree child, List<String> out) {
		if (child.getChildCount() == 0) {
			var val = child.getText();
			if (!",".equals(val)) {
				if ('"' == val.charAt(0)) {
					val = val.substring(1, val.length() - 1);
				}
				out.add(val);
			}
			return;
		}
		for (int i = 0; i < child.getChildCount(); i++) {
			childToList(child.getChild(i), out);
		}
	}

	@Override
	public void exitExprInEqual(ExprInEqualContext ctx) {
		var name = getFieldName(ctx, 0);
		var list = new ArrayList<String>();
		childToList(ctx.getChild(3), list);
		var r = list.stream().map(value -> {
			var p = QueryPredicate.equal(name, eventService.fieldValueOf(name, value));
			return eventService.find(p);
		}).reduce((result, el) -> {
			result.addAll(el);
			return result;
		}).orElse(Collections.emptySet());

		stack.push(r);
	}

	public Set<String> getResult() {
		return stack.pop();
	}
}
