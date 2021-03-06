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

package io.datakernel.rpc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.protocol.RpcException;
import io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory;
import io.datakernel.rpc.server.RpcRequestHandler;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.util.Stopwatch;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static io.datakernel.rpc.protocol.stream.RpcStreamProtocolFactory.streamProtocol;
import static io.datakernel.util.MemSize.kilobytes;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class CumulativeBenchmark {
	public static final class ValueMessage {
		@Serialize(order = 0)
		public int value;

		public ValueMessage(@Deserialize("value") int value) {
			this.value = value;
		}
	}

	private static final int TOTAL_ROUNDS = 30;
	private static final int REQUESTS_TOTAL = 1_000_000;
	private static final int REQUESTS_AT_ONCE = 100;
	private static final int DEFAULT_TIMEOUT = 2_000;

	private static final int SERVICE_PORT = 55555;
	private static final RpcStreamProtocolFactory PROTOCOL = streamProtocol(kilobytes(64), kilobytes(64), true);

	private final Eventloop serverEventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	private final Eventloop clientEventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

	private final RpcServer server = RpcServer.create(serverEventloop)
			.withProtocol(PROTOCOL)
			.withMessageTypes(ValueMessage.class)
			.withHandler(ValueMessage.class, ValueMessage.class, new RpcRequestHandler<ValueMessage, ValueMessage>() {
				private final ValueMessage currentSum = new ValueMessage(0);

				@Override
				public void run(ValueMessage request, ResultCallback<ValueMessage> callback) {
					if (request.value != 0) {
						currentSum.value += request.value;
					} else {
						currentSum.value = 0;
					}
					callback.setResult(currentSum);
				}
			})
			.withListenPort(SERVICE_PORT);

	private final RpcClient client = RpcClient.create(clientEventloop)
			.withMessageTypes(ValueMessage.class)
			.withProtocol(PROTOCOL)
			.withStrategy(server(new InetSocketAddress(SERVICE_PORT)));

	private final ValueMessage incrementMessage;
	private final int totalRounds;
	private final int roundRequests;
	private final int requestsAtOnce;
	private final int requestTimeout;

	public CumulativeBenchmark() {
		this(TOTAL_ROUNDS, REQUESTS_TOTAL, REQUESTS_AT_ONCE, DEFAULT_TIMEOUT);
	}

	public CumulativeBenchmark(int totalRounds, int roundRequests, int requestsAtOnce, int requestTimeout) {
		this.totalRounds = totalRounds;
		this.roundRequests = roundRequests;
		this.requestsAtOnce = requestsAtOnce;
		this.requestTimeout = requestTimeout;
		this.incrementMessage = new ValueMessage(2);
	}

	private void printBenchmarkInfo() {
		System.out.println("Benchmark rounds   : " + totalRounds);
		System.out.println("Requests per round : " + roundRequests);
		System.out.println("Requests at once   : " + requestsAtOnce);
		System.out.println("Increment value    : " + incrementMessage.value);
		System.out.println("Request timeout    : " + requestTimeout + " ms");
	}

	private void run() throws Exception {
		printBenchmarkInfo();

		server.listen();
		Executors.defaultThreadFactory().newThread(new Runnable() {
			@Override
			public void run() {
				serverEventloop.run();
			}
		}).start();

		try {
			final CompletionCallback finishCallback = new CompletionCallback() {
				@Override
				public void onException(Exception exception) {
					System.err.println("Exception while benchmark: " + exception);
					try {
						client.stopFuture();
						server.closeFuture();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void onComplete() {
					System.out.println("----------------------------------------");
					System.out.printf("Average time elapsed per round: %.1f ms\n", totalElapsed / (double) totalRounds);
					try {
						client.stopFuture();
						server.closeFuture();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			};

			CompletionCallback startCallback = new CompletionCallback() {
				@Override
				public void onComplete() {
					startBenchmarkRound(0, finishCallback);
				}

				@Override
				public void onException(Exception exception) {
					finishCallback.setException(exception);
				}
			};

			client.start(startCallback);

			clientEventloop.run();

		} finally {
			serverEventloop.execute(new Runnable() {
				@Override
				public void run() {

					server.close(IgnoreCompletionCallback.create());
				}
			});
			serverEventloop.keepAlive(false);

		}
	}

	private int success;
	private int errors;
	private int overloads;
	private int lastResponseValue;

	private int totalElapsed = 0;

	private void startBenchmarkRound(final int roundNumber, final CompletionCallback finishCallback) {
		if (roundNumber == totalRounds) {
			finishCallback.setComplete();
			return;
		}

		success = 0;
		errors = 0;
		overloads = 0;
		lastResponseValue = 0;

		final Stopwatch stopwatch = Stopwatch.createUnstarted();
		final CompletionCallback roundComplete = new CompletionCallback() {
			@Override
			public void onComplete() {
				stopwatch.stop();
				totalElapsed += stopwatch.elapsed(MILLISECONDS);

				System.out.println((roundNumber + 1) + ": Summary Elapsed " + stopwatch.toString()
						+ " rps: " + roundRequests * 1000.0 / stopwatch.elapsed(MILLISECONDS)
						+ " (" + success + "/" + roundRequests + " with " + overloads + " overloads) sum=" + lastResponseValue);

				clientEventloop.post(new Runnable() {
					@Override
					public void run() {
						startBenchmarkRound(roundNumber + 1, finishCallback);
					}
				});
			}

			@Override
			public void onException(Exception exception) {
				finishCallback.setException(exception);
			}
		};

		clientEventloop.post(new Runnable() {
			@Override
			public void run() {
				stopwatch.start();
				sendRequests(roundRequests, roundComplete);
			}
		});
	}

	private boolean clientOverloaded;

	private void sendRequests(final int numberRequests, final CompletionCallback completionCallback) {
		clientOverloaded = false;

		for (int i = 0; i < requestsAtOnce; i++) {
			if (i >= numberRequests)
				return;

			client.sendRequest(incrementMessage, requestTimeout, new ResultCallback<ValueMessage>() {
				@Override
				public void onResult(ValueMessage result) {
					success++;
					lastResponseValue = result.value;
					tryCompete();
				}

				private void tryCompete() {
					int totalCompletion = success + errors;
					if (totalCompletion == roundRequests)
						completionCallback.setComplete();
				}

				@Override
				public void onException(Exception exception) {
					if (exception.getClass() == RpcException.class) {
						clientOverloaded = true;
						overloads++;
					} else {
						errors++;
					}
					tryCompete();
				}
			});

			if (clientOverloaded) {
				// post to next event loop
				scheduleContinue(numberRequests - i, completionCallback);
				return;
			}
		}
		postContinue(numberRequests - requestsAtOnce, completionCallback);
	}

	private void postContinue(final int numberRequests, final CompletionCallback completionCallback) {
		clientEventloop.post(new Runnable() {
			@Override
			public void run() {
				sendRequests(numberRequests, completionCallback);
			}
		});
	}

	private void scheduleContinue(final int numberRequests, final CompletionCallback completionCallback) {
		clientEventloop.schedule(clientEventloop.currentTimeMillis() + 1, new Runnable() {
			@Override
			public void run() {
				sendRequests(numberRequests, completionCallback);
			}
		});
	}

	public static void main(String[] args) throws Exception {
		loggerLevel(Level.OFF);

		new CumulativeBenchmark().run();
	}

	private static void loggerLevel(Level level) {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(level);
	}

}
