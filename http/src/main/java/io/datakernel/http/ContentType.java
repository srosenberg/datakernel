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

import java.nio.charset.Charset;

public final class ContentType {
	final MediaType mime;
	final HttpCharset charset;

	ContentType(MediaType mime, HttpCharset charset) {
		this.mime = mime;
		this.charset = charset;
	}

	public static ContentType of(MediaType mime) {
		return new ContentType(mime, null);
	}

	public static ContentType of(MediaType mime, Charset charset) {
		return ContentTypes.lookup(mime, HttpCharset.of(charset));
	}

	public Charset getCharset() {
		return charset == null ? null : charset.toJavaCharset();
	}

	public MediaType getMediaType() {
		return mime;
	}

	int size() {
		int size = mime.size();
		if (charset != null) {
			size += charset.size();
			size += 10; // '; charset='
		}
		return size;
	}

	@Override
	public String toString() {
		return "ContentType{" +
				"type=" + mime +
				", charset=" + charset +
				'}';
	}
}
