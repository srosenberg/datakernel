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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncCancellable;
import io.datakernel.async.ParseException;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebufnew.ByteBufN;
import io.datakernel.eventloop.AsyncSslSocket;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;

import static io.datakernel.http.HttpHeaders.CONNECTION;
import static io.datakernel.util.ByteBufStrings.SP;
import static io.datakernel.util.ByteBufStrings.decodeDecimal;

/**
 * Realization of {@link AbstractHttpConnection},  which represents a client side and used for sending requests and
 * receiving responses.
 */
@SuppressWarnings("ThrowableInstanceNeverThrown")
final class HttpClientConnection extends AbstractHttpConnection {
	private static final TimeoutException TIMEOUT_EXCEPTION = new TimeoutException();
	private static final ParseException CLOSED_CONNECTION = new ParseException("Connection unexpectedly closed");
	private static final HttpHeaders.Value CONNECTION_KEEP_ALIVE = HttpHeaders.asBytes(CONNECTION, "keep-alive");

	private ResultCallback<HttpResponse> callback;
	private AsyncCancellable cancellable;
	private HttpResponse response;
	private final AsyncHttpClient httpClient;
	protected ExposedLinkedList.Node<HttpClientConnection> ipConnectionListNode;

	private final InetSocketAddress remoteSocketAddress;

	HttpClientConnection(Eventloop eventloop, InetSocketAddress remoteSocketAddress, AsyncTcpSocket asyncTcpSocket, AsyncHttpClient httpClient, char[] headerChars, int maxHttpMessageSize) {
		super(eventloop, asyncTcpSocket, httpClient.connectionsList, headerChars, maxHttpMessageSize);
		this.remoteSocketAddress = remoteSocketAddress;
		this.httpClient = httpClient;
	}

	@Override
	public void onClosedWithError(Exception e) {
		assert eventloop.inEventloopThread();
		onClosed();
		if (this.callback != null) {
			ResultCallback<HttpResponse> callback = this.callback;
			this.callback = null;
			callback.onException(e);
		}
	}

	@Override
	protected void onFirstLine(ByteBufN line) throws ParseException {
		if (line.peek(0) != 'H' || line.peek(1) != 'T' || line.peek(2) != 'T' || line.peek(3) != 'P' || line.peek(4) != '/' || line.peek(5) != '1')
			throw new ParseException("Invalid response");

		int sp1;
		if (line.peek(6) == SP) {
			sp1 = line.getReadPosition() + 7;
		} else if (line.peek(6) == '.' && (line.peek(7) == '1' || line.peek(7) == '0') && line.peek(8) == SP) {
			sp1 = line.getReadPosition() + 9;
		} else
			throw new ParseException("Invalid response: " + new String(line.array(), line.getReadPosition(), line.remainingToRead()));

		int sp2;
		for (sp2 = sp1; sp2 < line.getWritePosition(); sp2++) {
			if (line.at(sp2) == SP) {
				break;
			}
		}

		int statusCode = decodeDecimal(line.array(), sp1, sp2 - sp1);
		if (!(statusCode >= 100 && statusCode < 600))
			throw new ParseException("Invalid HTTP Status Code " + statusCode);
		response = HttpResponse.create(statusCode);
		if (isNoBodyMessage(response)) {
			// Reset Content-Length for the case keep-alive connection
			contentLength = 0;
		}
	}

	/**
	 * RFC 2616, section 4.4
	 * 1.Any response message which "MUST NOT" include a message-body (such as the 1xx, 204, and 304 responses and any response to a HEAD request) is always
	 * terminated by the first empty line after the header fields, regardless of the entity-header fields present in the message.
	 */
	private static boolean isNoBodyMessage(HttpResponse message) {
		int messageCode = message.getCode();
		return (messageCode >= 100 && messageCode < 200) || messageCode == 204 || messageCode == 304;
	}

	/**
	 * Calls after receiving header, sets this header to httpResponse.
	 *
	 * @param header received header
	 * @param value  value of header
	 */
	@Override
	protected void onHeader(HttpHeader header, ByteBufN value) throws ParseException {
		super.onHeader(header, value);
		response.addHeader(header, value);
	}

	@Override
	protected void onHttpMessage(ByteBufN bodyBuf) {
		assert !isClosed();
		response.body(bodyBuf);
		ResultCallback<HttpResponse> callback = this.callback;
		this.callback = null;
		callback.onResult(response);
		if (isClosed())
			return;
		if (keepAlive) {
			reset();
			httpClient.addToIpPool(this);
		} else {
			close();
		}
	}

	@Override
	public void onReadEndOfStream() {
		assert eventloop.inEventloopThread();
		if (callback != null) {
			if (reading == BODY && contentLength == UNKNOWN_LENGTH) {
				onHttpMessage(bodyQueue.takeRemaining());
			} else {
				closeWithError(CLOSED_CONNECTION);
			}
		}
		close();
	}

	@Override
	protected void reset() {
		reading = END_OF_STREAM;
		if (response != null) {
			response.recycleBufs();
			response = null;
		}
		if (cancellable != null) {
			cancellable.cancel();
			cancellable = null;
		}
		super.reset();
	}

	private void writeHttpRequest(HttpRequest httpRequest) {
		if (keepAlive) {
			httpRequest.addHeader(CONNECTION_KEEP_ALIVE);
		}
		asyncTcpSocket.write(httpRequest.write());
	}

	/**
	 * Sends the request, recycles it and closes connection in case of timeout
	 *
	 * @param request  request for sending
	 * @param timeout  time after which connection will be closed
	 * @param callback callback for handling result
	 */
	public void send(HttpRequest request, long timeout, ResultCallback<HttpResponse> callback) {
		this.callback = callback;
		writeHttpRequest(request);
		request.recycleBufs();
		scheduleTimeout(timeout); // FIXME connection closes before we set timeout
	}

	private void scheduleTimeout(final long timeoutTime) {
		assert !isClosed();

		cancellable = eventloop.scheduleBackground(timeoutTime, new Runnable() {
			@Override
			public void run() {
				if (!isClosed()) {
					closeWithError(TIMEOUT_EXCEPTION);
				}
			}
		});
	}

	@Override
	public void onWrite() {
		assert !isClosed();
		assert eventloop.inEventloopThread();
		reading = FIRSTLINE;
		asyncTcpSocket.read();
	}

	/**
	 * After closing this connection it removes it from its connections cache and recycles
	 * Http response.
	 */
	@Override
	protected void onClosed() {
		super.onClosed();
		bodyQueue.clear();
		if (connectionsListNode != null) {
			connectionsList.removeNode(connectionsListNode);
		}
		if (response != null) {
			response.recycleBufs();
		}
		httpClient.removeFromIpPool(this);
	}

	@Nullable
	public InetSocketAddress getRemoteSocketAddress() {
		return remoteSocketAddress;
	}

	public boolean isSecuredConnection() {
		return this.asyncTcpSocket instanceof AsyncSslSocket;
	}
}
