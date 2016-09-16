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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;

import java.util.Date;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.HttpUtils.skipSpaces;

// RFC 6265
public final class HttpCookie {
	private abstract static class AvHandler {
		protected abstract void handle(HttpCookie cookie, byte[] bytes, int start, int end) throws ParseException;
	}

	private static final byte[] EXPIRES = encodeAscii("Expires");
	private static final int EXPIRES_HC = 433574931;
	private static final byte[] MAX_AGE = encodeAscii("Max-Age");
	private static final int MAX_AGE_HC = -1709216267;
	private static final byte[] DOMAIN = encodeAscii("Domain");
	private static final int DOMAIN_HC = -438693883;
	private static final byte[] PATH = encodeAscii("Path");
	private static final int PATH_HC = 4357030;
	private static final byte[] HTTPONLY = encodeAscii("HttpOnly");
	private static final int SECURE_HC = -18770248;
	private static final byte[] SECURE = encodeAscii("Secure");
	private static final int HTTP_ONLY_HC = -1939729611;

	private final String name;
	private String value;
	private Date expirationDate;
	private int maxAge;
	private String domain;
	private String path = "/";
	private boolean secure;
	private boolean httpOnly;
	private String extension;

	// region builders
	private HttpCookie(String name, String value) {
		this.name = name;
		this.value = value;
	}

	public static HttpCookie of(String name, String value) {
		return new HttpCookie(name, value);
	}

	public static HttpCookie of(String name) {
		return new HttpCookie(name, null);
	}

	public HttpCookie withValue(String value) {
		setValue(value);
		return this;
	}

	public HttpCookie withExpirationDate(Date expirationDate) {
		setExpirationDate(expirationDate);
		return this;
	}


	public HttpCookie withMaxAge(int maxAge) {
		setMaxAge(maxAge);
		return this;
	}

	public HttpCookie withDomain(String domain) {
		setDomain(domain);
		return this;
	}

	public HttpCookie withPath(String path) {
		setPath(path);
		return this;
	}

	public HttpCookie withSecure(boolean secure) {
		setSecure(secure);
		return this;
	}

	public HttpCookie withHttpOnly(boolean httpOnly) {
		setHttpOnly(httpOnly);
		return this;
	}

	public HttpCookie withExtension(String extension) {
		setExtension(extension);
		return this;
	}
	// endregion

	// region accessors
	public String getName() {
		return name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Date getExpirationDate() {
		return expirationDate;
	}

	public void setExpirationDate(Date expirationDate) {
		this.expirationDate = expirationDate;
	}

	public int getMaxAge() {
		return maxAge;
	}

	public void setMaxAge(int maxAge) {
		this.maxAge = maxAge;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public boolean isSecure() {
		return secure;
	}

	public void setSecure(boolean secure) {
		this.secure = secure;
	}

	public boolean isHttpOnly() {
		return httpOnly;
	}

	public void setHttpOnly(boolean httpOnly) {
		this.httpOnly = httpOnly;
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}
	// endregion

	// region etc
	static void parse(String cookieString, List<HttpCookie> cookies) throws ParseException {
		byte[] bytes = encodeAscii(cookieString);
		parse(bytes, 0, bytes.length, cookies);
	}

	static void parse(ByteBuf buf, List<HttpCookie> cookies) throws ParseException {
		parse(buf.array(), buf.head(), buf.tail(), cookies);
	}

	static void parse(byte[] bytes, int pos, int end, List<HttpCookie> cookies) throws ParseException {
		try {
			HttpCookie cookie = new HttpCookie("", "");
			while (pos < end) {
				pos = skipSpaces(bytes, pos, end);
				int keyStart = pos;
				while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
					pos++;
				}
				int valueEnd = pos;
				int equalSign = -1;
				for (int i = keyStart; i < valueEnd; i++) {
					if (bytes[i] == '=') {
						equalSign = i;
						break;
					}
				}
				AvHandler handler = getCookieHandler(hashCodeLowerCaseAscii
						(bytes, keyStart, (equalSign == -1 ? valueEnd : equalSign) - keyStart));
				if (equalSign == -1 && handler == null) {
					cookie.setExtension(decodeAscii(bytes, keyStart, valueEnd - keyStart));
				} else if (handler == null) {
					String key = decodeAscii(bytes, keyStart, equalSign - keyStart);
					String value;
					if (bytes[equalSign + 1] == '\"' && bytes[valueEnd - 1] == '\"') {
						value = decodeAscii(bytes, equalSign + 2, valueEnd - equalSign - 3);
					} else {
						value = decodeAscii(bytes, equalSign + 1, valueEnd - equalSign - 1);
					}
					cookie = new HttpCookie(key, value);
					cookies.add(cookie);
				} else {
					handler.handle(cookie, bytes, equalSign + 1, valueEnd);
				}
				pos = valueEnd + 1;
			}
		} catch (RuntimeException e) {
			throw new ParseException();
		}
	}

	static void renderSimple(List<HttpCookie> cookies, ByteBuf buf) {
		int pos = renderSimple(cookies, buf.array(), buf.tail());
		buf.tail(pos);
	}

	static int renderSimple(List<HttpCookie> cookies, byte[] bytes, int pos) {
		for (int i = 0; i < cookies.size(); i++) {
			HttpCookie cookie = cookies.get(i);
			encodeAscii(bytes, pos, cookie.name);
			pos += cookie.name.length();

			if (cookie.value != null) {
				encodeAscii(bytes, pos, "=\"");
				pos += 2;
				encodeAscii(bytes, pos, cookie.value);
				pos += cookie.value.length();
				encodeAscii(bytes, pos, "\"");
				pos += 1;
			}

			if (i != cookies.size() - 1) {
				encodeAscii(bytes, pos, "; ");
				pos += 2;
			}
		}
		return pos;
	}

	static void parseSimple(String string, List<HttpCookie> cookies) throws ParseException {
		byte[] bytes = encodeAscii(string);
		parseSimple(bytes, 0, bytes.length, cookies);
	}

	static void parseSimple(byte[] bytes, int pos, int end, List<HttpCookie> cookies) throws ParseException {
		try {
			while (pos < end) {
				pos = skipSpaces(bytes, pos, end);
				int keyStart = pos;
				while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
					pos++;
				}
				int valueEnd = pos;
				int equalSign = -1;
				for (int i = keyStart; i < valueEnd; i++) {
					if (bytes[i] == '=') {
						equalSign = i;
						break;
					}
				}

				if (equalSign == -1) {
					String key = decodeAscii(bytes, keyStart, valueEnd - keyStart);
					cookies.add(HttpCookie.of(key));
				} else {
					String key = decodeAscii(bytes, keyStart, equalSign - keyStart);
					String value;
					if (bytes[equalSign + 1] == '\"' && bytes[valueEnd - 1] == '\"') {
						value = decodeAscii(bytes, equalSign + 2, valueEnd - equalSign - 3);
					} else {
						value = decodeAscii(bytes, equalSign + 1, valueEnd - equalSign - 1);
					}

					cookies.add(new HttpCookie(key, value));
				}

				pos = valueEnd + 1;
			}
		} catch (RuntimeException e) {
			throw new ParseException();
		}
	}

	static void renderFull(List<HttpCookie> cookies, ByteBuf buf) {
		for (int i = 0; i < cookies.size(); i++) {
			cookies.get(i).renderFull(buf);
			if (i < cookies.size() - 1) {
				buf.put((byte) ',');
				buf.put((byte) ' ');
			}
		}
	}

	void renderFull(ByteBuf buf) {
		putAscii(buf, name);
		putAscii(buf, "=\"");
		if (value != null) {
			putAscii(buf, value);
		}
		putAscii(buf, "\"");
		if (expirationDate != null) {
			putAscii(buf, "; ");
			buf.put(EXPIRES);
			putAscii(buf, "=");
			HttpDate.render(expirationDate.getTime(), buf);
		}
		if (maxAge > 0) {
			putAscii(buf, "; ");
			buf.put(MAX_AGE);
			putAscii(buf, "=");
			putDecimal(buf, maxAge);
		}
		if (domain != null) {
			putAscii(buf, "; ");
			buf.put(DOMAIN);
			putAscii(buf, "=");
			putAscii(buf, domain);
		}
		if (!(path == null || path.equals("/"))) {
			putAscii(buf, "; ");
			buf.put(PATH);
			putAscii(buf, "=");
			putAscii(buf, path);
		}
		if (secure) {
			putAscii(buf, "; ");
			buf.put(SECURE);
		}
		if (httpOnly) {
			putAscii(buf, "; ");
			buf.put(HTTPONLY);
		}
		if (extension != null) {
			putAscii(buf, "; \"");
			putAscii(buf, extension);
			putAscii(buf, "\"");
		}
	}

	private static AvHandler getCookieHandler(int hash) {
		switch (hash) {
			case EXPIRES_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) throws ParseException {
						cookie.setExpirationDate(parseExpirationDate(bytes, start, end));
					}
				};
			case MAX_AGE_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) throws ParseException {
						cookie.setMaxAge(decodeDecimal(bytes, start, end - start));
					}
				};
			case DOMAIN_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
						cookie.setDomain(decodeAscii(bytes, start, end - start));
					}
				};
			case PATH_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
						cookie.setPath(decodeAscii(bytes, start, end - start));
					}
				};
			case SECURE_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
						cookie.setSecure(true);
					}
				};
			case HTTP_ONLY_HC:
				return new AvHandler() {
					@Override
					protected void handle(HttpCookie cookie, byte[] bytes, int start, int end) {
						cookie.setHttpOnly(true);
					}
				};

		}
		return null;
	}

	private static Date parseExpirationDate(byte[] bytes, int start, int end) throws ParseException {
		assert end - start <= 29;
		long timestamp = HttpDate.parse(bytes, start);
		return new Date(timestamp);
	}

	@Override
	public String toString() {
		return "HttpCookie{" +
				"name='" + name + '\'' +
				", value='" + value + '\'' + '}';
	}
	// endregion
}