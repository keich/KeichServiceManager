package ru.keich.mon.servicemanager.entity;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.util.MultiValueMap;

import lombok.Getter;
import ru.keich.mon.indexedhashmap.query.Operator;
import ru.keich.mon.servicemanager.query.QuerySort;

@Getter
public class EntityQueryParamsParser {
	public static final String QUERY_SEARCH = "search";
	public static final String QUERY_PROPERTY = "property";
	public static final String QUERY_LIMIT = "limit";
	public static final String QUERY_ID = "id";
	private final List<QuerySort> sorts;
	private final Set<String> properties;
	private final long limit;
	private final String search;
	
	public EntityQueryParamsParser(MultiValueMap<String, String> reqParam) {	
		if (reqParam.containsKey(QUERY_LIMIT)) {
			limit = Long.valueOf(reqParam.get(QUERY_LIMIT).get(0));
		} else {
			limit = -1;
		}
		
		if(reqParam.containsKey(QUERY_SEARCH)) {
			search = reqParam.get(QUERY_SEARCH).get(0);
		} else {
			search = "";
		}
		
		if(reqParam.containsKey(QUERY_PROPERTY)) {
			properties = reqParam.get(QUERY_PROPERTY).stream().collect(Collectors.toSet());
			properties.add(QUERY_ID);
		} else {
			properties = Collections.emptySet();
		}
		
		var filteredPrams = reqParam.entrySet().stream()
				.filter(p -> !p.getKey().toLowerCase().equals(QUERY_PROPERTY))
				.filter(p -> !p.getKey().toLowerCase().equals(QUERY_LIMIT))
				.filter(p -> !p.getKey().toLowerCase().equals(QUERY_SEARCH))
				.collect(Collectors.toList());
		
		this.sorts = filteredPrams.stream()
				.flatMap(param -> {
					return param.getValue().stream().map(value -> QuerySort.fromParam(param.getKey(), value));
				})
				.filter(s -> s.getOperator() != Operator.ERROR)
				.sorted(QuerySort::compareTo)
				.collect(Collectors.toList());
	}

}
