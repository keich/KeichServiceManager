package ru.keich.mon.servicemanager;

import java.io.IOException;
import java.time.Instant;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.annotation.WebFilter;
import jakarta.servlet.http.HttpServletResponse;

@WebFilter("/api/*")
public class AddResponseHeaderFilter implements Filter {
	
	public final static String HEADER_START_TIME = "KeichServiceManager-Start-Time";
	private final String startTime = Instant.now().toString();
	
	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		HttpServletResponse httpServletResponse = (HttpServletResponse) response;
		httpServletResponse.setHeader(HEADER_START_TIME, startTime);
		chain.doFilter(request, response);
	}

}
