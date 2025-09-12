package ru.keich.mon.servicemanager.entity;

import java.util.Collections;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryParams {
	public static final QueryParams DEFAULT = new QueryParams();
	private long limit = -1;
	private Set<String> sort = Collections.emptySet();
	private boolean sortRevers = false;
	
	@Override
	public String toString() {
		return "QueryParams [limit=" + limit + ", sort=" + sort + ", sortRevers=" + sortRevers + "]";
	}
	
}