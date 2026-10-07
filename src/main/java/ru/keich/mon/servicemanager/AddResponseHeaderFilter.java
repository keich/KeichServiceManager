package ru.keich.mon.servicemanager;

import java.io.IOException;
import java.time.Instant;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.annotation.WebFilter;
import jakarta.servlet.http.HttpServletResponse;

@WebFilter("/api/*")
@Component
public class AddResponseHeaderFilter implements Filter {

	public final static String HEADER_START_TIME = "KeichServiceManager-Start-Time";
	public final static String HEADER_NODE_NAME = "KeichServiceManager-nodeName";
	private final String startTime = Instant.now().toString();
	private final String nodeName;

	public AddResponseHeaderFilter(@Value("${replication.nodename}") String nodeName) {
		super();
		this.nodeName = nodeName;
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		HttpServletResponse httpServletResponse = (HttpServletResponse) response;
		httpServletResponse.setHeader(HEADER_START_TIME, startTime);
		httpServletResponse.setHeader(HEADER_NODE_NAME, nodeName);
		chain.doFilter(request, response);
	}

}
