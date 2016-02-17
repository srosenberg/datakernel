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

import io.datakernel.async.SimpleException;

public class HttpException extends SimpleException {
	private final int code;

	public HttpException(int code) {
		super();
		this.code = code;
	}

	public HttpException(int code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;
	}

	public HttpException(int code, String s) {
		super(s);
		this.code = code;
	}

	public HttpException(int code, Throwable cause) {
		super(cause);
		this.code = code;
	}

	public int getCode() {
		return code;
	}

	@Override
	public String toString() {
		String s = getClass().getName() + ": ";
		String message = getMessage();
		return ((message != null) ? (s + message + ", ") : s) + "code: " + code;
	}
}
