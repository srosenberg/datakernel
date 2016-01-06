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
import io.datakernel.util.ByteBufStrings;

import static io.datakernel.http.HttpCharset.US_ASCII;
import static io.datakernel.http.HttpCharset.UTF_8;
import static io.datakernel.http.HttpUtils.skipSpaces;
import static io.datakernel.http.MediaTypes.*;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

public final class ContentTypes {
	public static final ContentType ANY_TEXT_UTF_8 = register(ANY_TEXT, UTF_8);
	public static final ContentType PLAIN_TEXT_UTF_8 = register(PLAIN_TEXT, UTF_8);
	public static final ContentType JSON_UTF_8 = register(JSON, UTF_8);
	public static final ContentType HTML_UTF_8 = register(HTML, UTF_8);
	public static final ContentType CSS_UTF_8 = register(CSS, UTF_8);
	public static final ContentType PLAIN_TEXT_ASCII = register(PLAIN_TEXT, US_ASCII);

	private static final byte[] CHARSET_KEY = encodeAscii("charset");

	private ContentTypes() {
		throw new AssertionError();
	}

	static ContentType lookup(MediaType mime, HttpCharset charset) {
		if (mime == JSON && charset == UTF_8) {
			return JSON_UTF_8;
		}
		return new ContentType(mime, charset);
	}

	static ContentType register(MediaType mime, HttpCharset charset) {
		return new ContentType(mime, charset);
	}

	static ContentType parse(byte[] bytes, int pos, int length) {
		// parsing media type
		pos = skipSpaces(bytes, pos, length);
		int start = pos;
		int lowerCaseHashCode = 1;
		int end = pos + length;
		while (pos < end && bytes[pos] != ';') {
			byte b = bytes[pos];
			if (b >= 'A' && b <= 'Z') {
				b += 'a' - 'A';
			}
			lowerCaseHashCode = lowerCaseHashCode * 31 + b;
			pos++;
		}
		MediaType type = MediaTypes.parse(bytes, start, pos - start, lowerCaseHashCode);
		pos++;

		// parsing parameters if any (interested in 'charset' only)
		HttpCharset charset = null;
		if (pos < end) {
			pos = skipSpaces(bytes, pos, length);
			start = pos;
			while (pos < end) {
				if (bytes[pos] == '=' && ByteBufStrings.equalsLowerCaseAscii(CHARSET_KEY, bytes, start, pos - start)) {
					pos++;
					start = pos;
					while (pos < end && bytes[pos] != ';') {
						pos++;
					}
					charset = HttpCharset.parse(bytes, start, pos - start);
				} else if (bytes[pos] == ';' && pos + 1 < end) {
					start = skipSpaces(bytes, pos + 1, length);
				}
				pos++;
			}
		}
		return lookup(type, charset);
	}

	static void render(ContentType type, ByteBuf buf) {
		int pos = render(type, buf.array(), buf.position());
		buf.position(pos);
	}

	static int render(ContentType type, byte[] container, int pos) {
		pos += MediaTypes.render(type.getMediaType(), container, pos);
		if (type.charset != null) {
			container[pos++] = ';';
			container[pos++] = ' ';
			System.arraycopy(CHARSET_KEY, 0, container, pos, CHARSET_KEY.length);
			pos += CHARSET_KEY.length;
			container[pos++] = '=';
			pos += HttpCharset.render(type.charset, container, pos);
		}
		return pos;
	}
}

