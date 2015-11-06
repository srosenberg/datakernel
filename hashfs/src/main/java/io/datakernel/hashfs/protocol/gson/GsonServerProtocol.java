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

package io.datakernel.hashfs.protocol.gson;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractNioServer;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.hashfs.Server;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.hashfs.protocol.ServerProtocol;
import io.datakernel.stream.StreamForwarder;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Set;

public class GsonServerProtocol extends AbstractNioServer<GsonServerProtocol> implements ServerProtocol {
	private final Server server;
	private final int deserializerBufferSize;
	private final int serializerBufferSize;
	private final int serializerMaxMessageSize;
	private final int serializerFlushDelayMillis;

	public GsonServerProtocol(NioEventloop eventloop, Server server, int deserializerBufferSize,
	                          int serializerBufferSize, int serializerMaxMessageSize, int serializerFlushDelayMillis) {
		super(eventloop);
		this.server = server;
		this.deserializerBufferSize = deserializerBufferSize;
		this.serializerBufferSize = serializerBufferSize;
		this.serializerMaxMessageSize = serializerMaxMessageSize;
		this.serializerFlushDelayMillis = serializerFlushDelayMillis;
	}

	@Override
	public void start(CompletionCallback callback) {
		try {
			self().listen();
			callback.onComplete();
		} catch (IOException e) {
			callback.onException(e);
		}
	}

	@Override
	public void stop(CompletionCallback callback) {
		self().close();
		callback.onComplete();
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, HashFsCommandSerializer.GSON, HashFsCommand.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, HashFsResponseSerializer.GSON, HashFsResponse.class, serializerBufferSize,
						serializerMaxMessageSize, serializerFlushDelayMillis))
				.addHandler(HashFsCommandUpload.class, defineUploadHandler())
				.addHandler(HashFsCommandCommit.class, defineCommitHandler())
				.addHandler(HashFsCommandDownload.class, defineDownloadHandler())
				.addHandler(HashFsCommandDelete.class, defineDeleteHandler())
				.addHandler(HashFsCommandList.class, defineListHandler())
				.addHandler(HashFsCommandAlive.class, defineAliveHandler())
				.addHandler(HashFsCommandOffer.class, defineOfferHandler());
	}

	private MessagingHandler<HashFsCommandUpload, HashFsResponse> defineUploadHandler() {
		return new MessagingHandler<HashFsCommandUpload, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandUpload item, final Messaging<HashFsResponse> messaging) {
				messaging.sendMessage(new HashFsResponseOk());
				server.upload(item.filePath, messaging.read(), new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new HashFsResponseAcknowledge());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandCommit, HashFsResponse> defineCommitHandler() {
		return new MessagingHandler<HashFsCommandCommit, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandCommit item, final Messaging<HashFsResponse> messaging) {
				server.commit(item.filePath, item.isOk, new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new HashFsResponseOk());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandDownload, HashFsResponse> defineDownloadHandler() {
		return new MessagingHandler<HashFsCommandDownload, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandDownload item, final Messaging<HashFsResponse> messaging) {
				final StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
				server.download(item.filePath, forwarder.getInput(), new ResultCallback<CompletionCallback>() {
					@Override
					public void onResult(final CompletionCallback callback) {
						messaging.sendMessage(new HashFsResponseOk());
						messaging.write(forwarder.getOutput(), callback);
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandList, HashFsResponse> defineListHandler() {
		return new MessagingHandler<HashFsCommandList, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandList item, final Messaging<HashFsResponse> messaging) {
				server.list(new ResultCallback<Set<String>>() {
					@Override
					public void onResult(Set<String> result) {
						messaging.sendMessage(new HashFsResponseListFiles(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandDelete, HashFsResponse> defineDeleteHandler() {
		return new MessagingHandler<HashFsCommandDelete, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandDelete item, final Messaging<HashFsResponse> messaging) {
				server.delete(item.filePath, new CompletionCallback() {
					@Override
					public void onComplete() {
						messaging.sendMessage(new HashFsResponseOk());
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandAlive, HashFsResponse> defineAliveHandler() {
		return new MessagingHandler<HashFsCommandAlive, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandAlive item, final Messaging<HashFsResponse> messaging) {
				server.showAlive(new ResultCallback<Set<ServerInfo>>() {
					@Override
					public void onResult(Set<ServerInfo> result) {
						messaging.sendMessage(new HashFsResponseListServers(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	private MessagingHandler<HashFsCommandOffer, HashFsResponse> defineOfferHandler() {
		return new MessagingHandler<HashFsCommandOffer, HashFsResponse>() {
			@Override
			public void onMessage(HashFsCommandOffer item, final Messaging<HashFsResponse> messaging) {
				server.checkOffer(item.forUpload, item.forDeletion, new ResultCallback<Set<String>>() {
					@Override
					public void onResult(Set<String> result) {
						messaging.sendMessage(new HashFsResponseListFiles(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new HashFsResponseError(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}
}






























