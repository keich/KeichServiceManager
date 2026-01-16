package ru.keich.mon.servicemanager.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.BiFunction;

import org.antlr.v4.runtime.tree.ParseTree;

import ru.keich.mon.servicemanager.KQueryBaseListener;
import ru.keich.mon.servicemanager.KQueryParser.ExprANDContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprContainContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprEqualContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprFieldsContainContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprFieldsEqualContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprGreaterEqualContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprGreaterThanContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprInEqualContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprLessThanContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprNotContainContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprNotEqualContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprNotIncludeContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprORContext;
import ru.keich.mon.servicemanager.entity.Entity;
import ru.keich.mon.servicemanager.entity.EntityService;

/*
 * Copyright 2025 the original author or authors.
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

public class QueryListener<K, T extends Entity<K>> extends KQueryBaseListener {

	private final EntityService<K, T> entityService;
	private final Stack<Set<K>> stack = new Stack<>();
	private final BiFunction<String, String, Object> valueConverter;

	public QueryListener(EntityService<K, T> entityService, BiFunction<String, String, Object> valueConverter) {
		this.entityService = entityService;
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
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.notEqual(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprEqual(ExprEqualContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.equal(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprNotContain(ExprNotContainContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.notContain(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprNotInclude(ExprNotIncludeContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.notInclude(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprLessThan(ExprLessThanContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.lessThan(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprGreaterEqual(ExprGreaterEqualContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.greaterEqual(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprContain(ExprContainContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.contain(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprGreaterThan(ExprGreaterThanContext ctx) {
		var name = getFieldName(ctx, 0);
		var value = pullString(ctx, 2);
		var p = QueryPredicate.greaterThan(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	private String getFieldName(ExprContext ctx, int idx) {
		return pullString(ctx, idx);
	}

	private String pullString(ExprContext ctx, int idx) {
		var value = ctx.getChild(idx).getText();
		if ('"' == value.charAt(0)) {
			return value.substring(1, value.length() - 1);
		}
		return value;
	}

	@Override
	public void exitExprFieldsContain(ExprFieldsContainContext ctx) {
		var key = pullString(ctx, 2);
		var value = pullString(ctx, 4);
		var entry = Map.entry(key, value);
		var p = QueryPredicate.contain(Entity.FIELD_FIELDS, entry);
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprFieldsEqual(ExprFieldsEqualContext ctx) {
		var key = pullString(ctx, 2);
		var value = pullString(ctx, 4);
		var entry = Map.entry(key, value);
		var p = QueryPredicate.equal(Entity.FIELD_FIELDS, entry);
		var r = entityService.find(p);
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
			var p = QueryPredicate.equal(name, valueConverter.apply(name, value));
			return entityService.find(p);
		}).reduce((result, el) -> {
			result.addAll(el);
			return result;
		}).orElse(Collections.emptySet());

		stack.push(r);
	}

	public Set<K> getResult() {
		return stack.pop();
	}

}
