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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;

import java.net.InetAddress;
import java.util.*;

import static io.datakernel.http.GzipProcessor.toGzip;
import static io.datakernel.http.HttpHeaders.*;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.http.HttpUtils.nullToEmpty;
import static io.datakernel.util.ByteBufStrings.*;

/**
 * Represent the HTTP request which {@link AsyncHttpClient} send to {@link AsyncHttpServer}. It must have only one owner in
 * each  part of time. After creating in server {@link HttpResponse} it will be recycled and you can not
 * use it later.
 */
public final class HttpRequest extends HttpMessage {
	private final HttpMethod method;
	private HttpUri url;
	private InetAddress remoteAddress;
	private Map<String, String> urlParameters;
	private Map<String, String> bodyParameters;
	private int pos;

	private HttpRequest(HttpMethod method) {
		this.method = method;
	}

	public static HttpRequest of(HttpMethod method) {
		assert method != null;
		return new HttpRequest(method);
	}

	public static HttpRequest get(String url) {
		return of(GET).withUrl(url);
	}

	public static HttpRequest post(String url) {
		return of(POST).withUrl(url);
	}

	// common builder methods
	public HttpRequest withHeader(HttpHeader header, ByteBuf value) {
		setHeader(header, value);
		return this;
	}

	public HttpRequest withHeader(HttpHeader header, byte[] value) {
		setHeader(header, value);
		return this;
	}

	public HttpRequest withHeader(HttpHeader header, String value) {
		setHeader(header, value);
		return this;
	}

	public HttpRequest withBody(byte[] array) {
		return withBody(ByteBuf.wrapForReading(array));
	}

	public HttpRequest withBody(ByteBuf body) {
		setBody(body);
		return this;
	}

	// specific builder methods
	public HttpRequest withAccept(List<AcceptMediaType> value) {
		addHeader(ofAcceptContentTypes(HttpHeaders.ACCEPT, value));
		return this;
	}

	public HttpRequest withAccept(AcceptMediaType... value) {
		return withAccept(Arrays.asList(value));
	}

	public HttpRequest withAcceptCharsets(List<AcceptCharset> values) {
		addHeader(ofCharsets(HttpHeaders.ACCEPT_CHARSET, values));
		return this;
	}

	public HttpRequest withAcceptCharsets(AcceptCharset... values) {
		return withAcceptCharsets(Arrays.asList(values));
	}

	public HttpRequest withCookies(List<HttpCookie> cookies) {
		addHeader(ofCookies(COOKIE, cookies));
		return this;
	}

	public HttpRequest withCookies(HttpCookie... cookie) {
		return withCookies(Arrays.asList(cookie));
	}

	public HttpRequest withCookie(HttpCookie cookie) {
		return withCookies(Collections.singletonList(cookie));
	}

	public HttpRequest withContentType(ContentType contentType) {
		setHeader(ofContentType(HttpHeaders.CONTENT_TYPE, contentType));
		return this;
	}

	public HttpRequest withDate(Date date) {
		setHeader(ofDate(HttpHeaders.DATE, date));
		return this;
	}

	public HttpRequest withIfModifiedSince(Date date) {
		setHeader(ofDate(IF_MODIFIED_SINCE, date));
		return this;
	}

	public HttpRequest withIfUnModifiedSince(Date date) {
		setHeader(ofDate(IF_UNMODIFIED_SINCE, date));
		return this;
	}

	public HttpRequest withUrl(HttpUri url) {
		assert !recycled;
		this.url = url;
		if (!url.isPartial()) {
			setHeader(HttpHeaders.HOST, url.getHostAndPort());
		}
		return this;
	}

	public HttpRequest withUrl(String url) {
		return withUrl(HttpUri.ofUrl(url));
	}

	public HttpRequest withRemoteAddress(InetAddress inetAddress) {
		assert !recycled;
		this.remoteAddress = inetAddress;
		return this;
	}

	private boolean gzip = false;

	public HttpRequest withGzipCompression() {
		setHeader(CONTENT_ENCODING, "gzip");
		gzip = true;
		return this;
	}

	// getters
	public List<AcceptMediaType> parseAccept() throws ParseException {
		assert !recycled;
		List<AcceptMediaType> list = new ArrayList<>();
		List<Value> headers = getHeaderValues(ACCEPT);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			AcceptMediaType.parse(value.array, value.offset, value.size, list);
		}
		return list;
	}

	public List<AcceptCharset> parseAcceptCharsets() throws ParseException {
		assert !recycled;
		List<AcceptCharset> charsets = new ArrayList<>();
		List<Value> headers = getHeaderValues(ACCEPT_CHARSET);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			AcceptCharset.parse(value.array, value.offset, value.size, charsets);
		}
		return charsets;
	}

	@Override
	public List<HttpCookie> parseCookies() throws ParseException {
		assert !recycled;
		List<HttpCookie> cookie = new ArrayList<>();
		List<Value> headers = getHeaderValues(COOKIE);
		for (Value header : headers) {
			ValueOfBytes value = (ValueOfBytes) header;
			HttpCookie.parseSimple(value.array, value.offset, value.offset + value.size, cookie);
		}
		return cookie;
	}

	public Date parseIfModifiedSince() throws ParseException {
		assert !recycled;
		ValueOfBytes header = (ValueOfBytes) getHeaderValue(IF_MODIFIED_SINCE);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}

	public Date parseIfUnModifiedSince() throws ParseException {
		assert !recycled;
		ValueOfBytes header = (ValueOfBytes) getHeaderValue(IF_UNMODIFIED_SINCE);
		if (header != null)
			return new Date(HttpDate.parse(header.array, header.offset));
		return null;
	}

	// internal
	public Map<String, String> getParameters() throws ParseException {
		assert !recycled;
		return url.getParameters();
	}

	public Map<String, String> getPostParameters() throws ParseException {
		assert !recycled;
		if (method == POST && getContentType() != null
				&& getContentType().getMediaType() == MediaTypes.X_WWW_FORM_URLENCODED
				&& body.head() != body.tail()) {
			if (bodyParameters == null) {
				bodyParameters = HttpUtils.extractParameters(decodeAscii(getBody()));
			}
			return bodyParameters;
		} else {
			return Collections.emptyMap();
		}
	}

	public String getPostParameter(String name) throws ParseException {
		return getPostParameters().get(name);
	}

	public String getParameter(String name) throws ParseException {
		assert !recycled;
		return url.getParameter(name);
	}

	int getPos() {
		return pos;
	}

	void setPos(int pos) {
		this.pos = pos;
	}

	public boolean isHttps() {
		return getUrl().getSchema().equals("https");
	}

	public HttpMethod getMethod() {
		assert !recycled;
		return method;
	}

	public HttpUri getUrl() {
		assert !recycled;
		return url;
	}

	public InetAddress getRemoteAddress() {
		assert !recycled;
		return remoteAddress;
	}

	public String getPath() {
		assert !recycled;
		return url.getPath();
	}

	String getRelativePath() {
		assert !recycled;
		String path = url.getPath();
		if (pos < path.length()) {
			return path.substring(pos);
		}
		return "";
	}

	private final static int LONGEST_HTTP_METHOD_SIZE = 12;
	private static final byte[] HTTP_1_1 = encodeAscii(" HTTP/1.1");
	private static final int HTTP_1_1_SIZE = HTTP_1_1.length;

	public String getUrlParameter(String key) {
		return urlParameters == null ? null : urlParameters.get(key);
	}

	String pollUrlPart() {
		String path = url.getPath();
		if (pos < path.length()) {
			int start = pos + 1;
			pos = path.indexOf('/', start);
			String part;
			if (pos == -1) {
				part = path.substring(start);
				pos = path.length();
			} else {
				part = path.substring(start, pos);
			}
			return part;
		} else {
			return "";
		}
	}

	void removeUrlParameter(String key) {
		urlParameters.remove(key);
	}

	void putUrlParameter(String key, String value) {
		if (urlParameters == null) {
			urlParameters = new HashMap<>();
		}
		urlParameters.put(key, value);
	}

	ByteBuf write() {
		assert !recycled;
		if (body != null || method != GET) {
			if (gzip) {
				try {
					body = toGzip(body);
				} catch (ParseException ignored) {
					throw new AssertionError("Can't encode http request body");
				}
			}
			setHeader(HttpHeaders.ofDecimal(HttpHeaders.CONTENT_LENGTH, body == null ? 0 : body.headRemaining()));
		}
		int estimatedSize = estimateSize(LONGEST_HTTP_METHOD_SIZE
				+ 1 // SPACE
				+ url.getPathAndQuery().length())
				+ HTTP_1_1_SIZE;
		ByteBuf buf = ByteBufPool.allocate(estimatedSize);

		method.write(buf);
		buf.put(SP);
		putAscii(buf, url.getPathAndQuery());
		buf.put(HTTP_1_1);

		writeHeaders(buf);

		writeBody(buf);

		return buf;
	}

	@Override
	public String toString() {
		String host = nullToEmpty(getHeader(HttpHeaders.HOST));
		if (url == null)
			return host;
		return host + url.getPathAndQuery();
	}

	public String getFullUrl() {
		String host = nullToEmpty(getHeader(HttpHeaders.HOST));
		if (url == null)
			return host;
		return host + url.getPathAndQuery();
	}
}
