package ru.keich.mon.servicemanager.query;

import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.BiFunction;

import ru.keich.mon.indexedhashmap.query.predicates.Predicates;
import ru.keich.mon.servicemanager.KQueryBaseListener;
import ru.keich.mon.servicemanager.KQueryParser.ExprANDContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprContainContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprEqualContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprFieldsContainContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprFieldsEqualContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprGreaterEqualContext;
import ru.keich.mon.servicemanager.KQueryParser.ExprGreaterThanContext;
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
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.notEqual(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprEqual(ExprEqualContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.equal(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprNotContain(ExprNotContainContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.notContain(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprNotInclude(ExprNotIncludeContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.notInclude(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprLessThan(ExprLessThanContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.lessThan(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprGreaterEqual(ExprGreaterEqualContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.greaterEqual(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprContain(ExprContainContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.contain(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprGreaterThan(ExprGreaterThanContext ctx) {
		var name = pullString(ctx, 0);
		var value = pullString(ctx, 2);
		var p = Predicates.greaterThan(name, valueConverter.apply(name, value));
		var r = entityService.find(p);
		stack.push(r);
	}

	private String pullString(ExprContext ctx, int idx) {
		var value = ctx.getChild(idx).getText();
		if('"' == value.charAt(0)) {
			return value.substring(1, value.length()-1);
		}
		return value;
	}

	@Override
	public void exitExprFieldsContain(ExprFieldsContainContext ctx) {
		var key = pullString(ctx, 2);
		var value = pullString(ctx, 4);
		var entry = Map.entry(key, value);
		var p = Predicates.contain(Entity.FIELD_FIELDS, entry);
		var r = entityService.find(p);
		stack.push(r);
	}

	@Override
	public void exitExprFieldsEqual(ExprFieldsEqualContext ctx) {
		var key = pullString(ctx, 2);
		var value = pullString(ctx, 4);
		var entry = Map.entry(key, value);
		var p = Predicates.equal(Entity.FIELD_FIELDS, entry);
		var r = entityService.find(p);
		stack.push(r);
	}

	public Set<K> getResult() {
		return stack.pop();
	}

}
