package ru.keich.mon.servicemanager.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.antlr.v4.runtime.tree.ParseTree;

import ru.keich.mon.servicemanager.KSearchBaseListener;
import ru.keich.mon.servicemanager.KSearchParser.ExprANDContext;
import ru.keich.mon.servicemanager.KSearchParser.ExprContainContext;
import ru.keich.mon.servicemanager.KSearchParser.ExprContext;
import ru.keich.mon.servicemanager.KSearchParser.ExprEqualContext;
import ru.keich.mon.servicemanager.KSearchParser.ExprGreaterEqualContext;
import ru.keich.mon.servicemanager.KSearchParser.ExprGreaterThanContext;
import ru.keich.mon.servicemanager.KSearchParser.ExprInEqualContext;
import ru.keich.mon.servicemanager.KSearchParser.ExprLessThanContext;
import ru.keich.mon.servicemanager.KSearchParser.ExprNotContainContext;
import ru.keich.mon.servicemanager.KSearchParser.ExprNotEqualContext;
import ru.keich.mon.servicemanager.KSearchParser.ExprNotIncludeContext;
import ru.keich.mon.servicemanager.KSearchParser.ExprORContext;
import ru.keich.mon.servicemanager.event.Event;
import ru.keich.mon.servicemanager.event.EventService;
import ru.keich.mon.servicemanager.item.Item;
import ru.keich.mon.servicemanager.item.ItemService;
import ru.keich.mon.servicemanager.query.QueryPredicate;


public class EntitySearchListener extends KSearchBaseListener implements EntitySearchResult<String> {
	private final EventService eventService;
	private final ItemService itemService;
	private final Stack<Set<String>> stack = new Stack<>();
	private final ServiceType serviceType;
	public static final String ITEM = "item";
	public static final String EVENT = "event";
	
	public static enum ServiceType {
		ITEM, EVENT
	}
	
	public static record FieldName(ServiceType type, String name, String subName, boolean isFields) {}

	public EntitySearchListener(EventService eventService, ItemService itemService,ServiceType serviceType) {
		this.eventService = eventService;
		this.itemService = itemService;
		this.serviceType = serviceType;
	}

	private void evaluate(FieldName field, List<String> strValues, BiFunction<String, Object, QueryPredicate> getPredicate) {
		final Function<QueryPredicate, Set<String>> find;
		final BiFunction<String, String, Object> fieldValueOf;
		Function<Set<String>, Set<String>> fill = Function.identity();
		if(field.type == ServiceType.ITEM) {
			fieldValueOf = itemService::fieldValueOf;
			find = itemService::find;
			if(serviceType == ServiceType.EVENT) {
				fill = s -> itemService.findByIds(s).stream()
						.map(Item::getEventsStatus)
						.map(Map::keySet)
						.flatMap(Set::stream)
						.collect(Collectors.toSet());
			} 
		} else {
			fieldValueOf = eventService::fieldValueOf;
			find = eventService::find;
			if(serviceType == ServiceType.ITEM) {
				fill = s -> eventService.findByIds(s)
						.stream()
						.map(Event::getItemIds)
						.flatMap(Set::stream)
						.collect(Collectors.toSet());
			} 
		}
		final Stream<Object> values;
		if(field.isFields()) {
			values = strValues.stream().map(str -> Map.entry(field.subName, fieldValueOf.apply(field.name, str)));
		} else {
			values = strValues.stream().map(str -> fieldValueOf.apply(field.name, str));
		}		
		var keys = values.map(e -> getPredicate.apply(field.name, e))
				.map(find::apply)
				.flatMap(Set::stream)
				.collect(Collectors.toSet());
		stack.push(fill.apply(keys));
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
		var field = getFieldName(ctx.getChild(0));
		var strValue = pullString(ctx, 2);
		evaluate(field, Collections.singletonList(strValue), QueryPredicate::notEqual);
	}

	@Override
	public void exitExprEqual(ExprEqualContext ctx) {
		var field = getFieldName(ctx.getChild(0));
		var strValue = pullString(ctx, 2);
		evaluate(field, Collections.singletonList(strValue), QueryPredicate::equal);
	}

	@Override
	public void exitExprNotContain(ExprNotContainContext ctx) {
		var field = getFieldName(ctx.getChild(0));
		var strValue = pullString(ctx, 3);
		evaluate(field, Collections.singletonList(strValue), QueryPredicate::notContain);
	}

	@Override
	public void exitExprNotInclude(ExprNotIncludeContext ctx) {
		var field = getFieldName(ctx.getChild(0));
		var strValue = pullString(ctx, 3);
		evaluate(field, Collections.singletonList(strValue), QueryPredicate::notInclude);
	}

	@Override
	public void exitExprLessThan(ExprLessThanContext ctx) {
		var field = getFieldName(ctx.getChild(0));
		var strValue = pullString(ctx, 2);
		evaluate(field, Collections.singletonList(strValue), QueryPredicate::lessThan);
	}

	@Override
	public void exitExprGreaterEqual(ExprGreaterEqualContext ctx) {
		var field = getFieldName(ctx.getChild(0));
		var strValue = pullString(ctx, 2);
		evaluate(field, Collections.singletonList(strValue), QueryPredicate::greaterEqual);
	}

	@Override
	public void exitExprContain(ExprContainContext ctx) {
		var field = getFieldName(ctx.getChild(0));
		var strValue = pullString(ctx, 2);
		evaluate(field, Collections.singletonList(strValue), QueryPredicate::contain);
	}

	@Override
	public void exitExprGreaterThan(ExprGreaterThanContext ctx) {
		var field = getFieldName(ctx.getChild(0));
		var strValue = pullString(ctx, 2);
		evaluate(field, Collections.singletonList(strValue), QueryPredicate::greaterThan);
	}
	
	@Override
	public void exitExprInEqual(ExprInEqualContext ctx) {
		var field = getFieldName(ctx.getChild(0));
		var values = new ArrayList<String>();
		childToList(ctx.getChild(3), values);
		evaluate(field, values, QueryPredicate::greaterThan);
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

	private FieldName getFieldName(ParseTree obj) {
		var c = obj.getChildCount();
		if(c == 1) {
			return new FieldName(serviceType, obj.getText(), null, false);
		} else if(c == 3) {
			var name = obj.getChild(0).getText();
			var subname = obj.getChild(2).getText();
			if(ITEM.equalsIgnoreCase(name)) {
				return new FieldName(ServiceType.ITEM, obj.getChild(2).getText(), null, false);
			} else if(EVENT.equalsIgnoreCase(name)) {
				return new FieldName(ServiceType.EVENT, obj.getChild(2).getText(), null, false);
			} if(Entity.FIELD_FIELDS.equalsIgnoreCase(name)) {
				return new FieldName(serviceType, Entity.FIELD_FIELDS, subname.substring(1, subname.length() - 1), true);
			}		
		} else if(c == 5) {
			ServiceType type;
			if(ITEM.equalsIgnoreCase(obj.getChild(0).getText())) {
				type = ServiceType.ITEM;
			} else {
				type = ServiceType.EVENT;
			}
			var subname = obj.getChild(4).getText();
			return new FieldName(type, Entity.FIELD_FIELDS, subname.substring(1, subname.length() - 1), true);
		}
		throw new RuntimeException("Error parse fieldName");
	}

	private String pullString(ExprContext ctx, int idx) {
		var value = ctx.getChild(idx).getText();
		if ('"' == value.charAt(0) || '\'' == value.charAt(0)) {
			return value.substring(1, value.length() - 1);
		}
		return value;
	}

	public Set<String> getResult() {
		return stack.pop();
	}

}
