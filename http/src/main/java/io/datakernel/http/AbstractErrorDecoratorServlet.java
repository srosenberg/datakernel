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

import io.datakernel.async.ResultCallback;

public abstract class AbstractErrorDecoratorServlet extends MiddlewareServlet {
	@Override
	public void serveAsync(final HttpRequest request, final ResultCallback<HttpResponse> callback) {
		try {
			super.serveAsync(request, new ResultCallback<HttpResponse>() {
				@Override
				public void onResult(HttpResponse result) {
					callback.onResult(result);
				}

				@Override
				public void onException(Exception e) {
					if (e instanceof HttpException) {
						callback.onResult(onHttpException(request, (HttpException) e));
					} else {
						callback.onResult(onCommonException(request, e));
					}
				}
			});
		} catch (RuntimeException e) {
			onRuntimeException(request, e);
		}
	}

	protected abstract HttpResponse onHttpException(HttpRequest request, HttpException e);

	protected abstract HttpResponse onCommonException(HttpRequest request, Exception e);

	protected abstract HttpResponse onRuntimeException(HttpRequest request, RuntimeException e);
}
