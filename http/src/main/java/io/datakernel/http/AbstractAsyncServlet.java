/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.http;

import io.datakernel.async.ParseException;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.time.CurrentTimeProviderSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.Arrays.asList;

/**
 * Represent an asynchronous HTTP servlet which receives and responds to requests from clients across HTTP.
 * For using this servlet you should override method doServeAsync(, in this method must be logic
 * for handling requests and creating result.
 */
public abstract class AbstractAsyncServlet implements AsyncHttpServlet, EventloopJmxMBean {
	protected static final Logger logger = LoggerFactory.getLogger(AbstractAsyncServlet.class);

	protected final Eventloop eventloop;

	// JMX
	private final EventStats requests = new EventStats();
	private final ServletErrorStats errors = new ServletErrorStats();
	private final ValueStats requestsTimings = new ValueStats();
	private final ValueStats errorsTimings = new ValueStats();

	private final Map<Integer, ServletErrorStats> errorCodeToStats = new HashMap<>();

	protected AbstractAsyncServlet(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	/**
	 * Method with logic for handling request and creating the response to client
	 *
	 * @param request  received request from client
	 * @param callback callback for handling result
	 */
	protected abstract void doServeAsync(HttpRequest request, Callback callback) throws ParseException;

	/**
	 * Handles the received {@link HttpRequest},  creates the {@link HttpResponse} and responds to client with
	 * {@link ResultCallback}
	 *
	 * @param request  received request
	 * @param callback ResultCallback for handling result
	 */
	@Override
	public final void serveAsync(final HttpRequest request, final Callback callback) throws ParseException {
		if (!isMonitoring(request)) {
			doServeAsync(request, callback);
			return;
		}
		requests.recordEvent();
		final long timestamp = eventloop.currentTimeMillis();
		try {
			doServeAsync(request, new Callback() {
				@Override
				public void onResult(HttpResponse result) {
					// jmx
					int duration = (int) (eventloop.currentTimeMillis() - timestamp);
					requestsTimings.recordValue(duration);

					callback.onResult(result);
				}

				@Override
				public void onHttpError(HttpServletError httpServletError) {
					// jmx
					int duration = (int) (eventloop.currentTimeMillis() - timestamp);
					errorsTimings.recordValue(duration);
					recordError(httpServletError, request);

					callback.onHttpError(httpServletError);
				}
			});
		} catch (ParseException parseException) {
			int badRequestHttpCode = 400;
			HttpServletError error = new HttpServletError(badRequestHttpCode, parseException);

			// jmx
			int duration = (int) (eventloop.currentTimeMillis() - timestamp);
			errorsTimings.recordValue(duration);
			recordError(error, request);

			callback.onHttpError(error);
		}
	}

	private void recordError(HttpServletError error, HttpRequest request) {
		int code = error.getCode();
		String url = extractUrl(request);
		Throwable cause = error.getCause();
		String message = error.getMessage();

		errors.recordError(cause, url, message);
		ServletErrorStats stats = ensureStats(code);
		stats.recordError(cause, url, message);
	}

	private ServletErrorStats ensureStats(int code) {
		ServletErrorStats stats = errorCodeToStats.get(code);
		if (stats == null) {
			stats = new ServletErrorStats(eventloop);
			errorCodeToStats.put(code, stats);
		}
		return stats;
	}

	// jmx
	protected boolean isMonitoring(HttpRequest request) {
		return true;
	}

	private static String extractUrl(HttpRequest request) {
		return request.toString();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public final EventStats getRequests() {
		return requests;
	}

	@JmxAttribute(description = "requests that were handled with error")
	public final ServletErrorStats getErrors() {
		return errors;
	}

	@JmxAttribute(description = "duration of handling one request in case of success")
	public final ValueStats getRequestsTimings() {
		return requestsTimings;
	}

	@JmxAttribute(description = "duration of handling one request in case of error")
	public final ValueStats getErrorsTimings() {
		return errorsTimings;
	}

	@JmxAttribute(description = "servlet errors distributed by http code")
	public final Map<Integer, ServletErrorStats> getErrorCodeToStats() {
		return errorCodeToStats;
	}

	public static final class ServletErrorStats implements JmxStats<ServletErrorStats> {
		public static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

		private final CurrentTimeProvider timeProvider;

		private Throwable throwable;
		private int count;
		private long lastErrorTimestamp;
		private String url;
		private String message;

		public ServletErrorStats(CurrentTimeProvider timeProvider) {
			this.timeProvider = timeProvider;
		}

		public ServletErrorStats() {
			this.timeProvider = new CurrentTimeProviderSystem();
		}

		public void recordError(Throwable throwable, String url, String message) {
			this.count++;
			this.throwable = throwable;
			this.lastErrorTimestamp = timeProvider.currentTimeMillis();
			this.url = url;
			this.message = message;
		}

		@Override
		public void add(ServletErrorStats another) {
			this.count += another.count;
			if (another.lastErrorTimestamp > this.lastErrorTimestamp) {
				this.throwable = another.throwable;
				this.lastErrorTimestamp = another.lastErrorTimestamp;
				this.url = another.url;
				this.message = another.message;
			}
		}

		@JmxAttribute
		public int getTotalErrors() {
			return count;
		}

		@JmxAttribute
		public String getLastErrorType() {
			return throwable != null ? throwable.getClass().getName() : "";
		}

		@JmxAttribute
		public String getLastErrorTimestamp() {
			return TIMESTAMP_FORMAT.format(new Date(lastErrorTimestamp));
		}

		@JmxAttribute
		public String getLastErrorUrl() {
			return url;
		}

		@JmxAttribute
		public String getLastErrorMessage() {
			return message;
		}

		@JmxAttribute
		public List<String> getLastErrorStackTrace() {
			if (throwable != null) {
				return asList(MBeanFormat.formatException(throwable));
			} else {
				return new ArrayList<>();
			}
		}
	}

}
