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

package io.datakernel;

import com.google.gson.Gson;
import io.datakernel.FsCommands.*;
import io.datakernel.FsResponses.*;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.AsyncTcpSocketImpl;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingConnection;
import io.datakernel.stream.net.MessagingSerializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@SuppressWarnings("unchecked")
public abstract class FsServer<S extends FsServer<S>> extends AbstractServer<S> {
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected final FileManager fileManager;

	// creators & builder methods
	protected FsServer(Eventloop eventloop, FileManager fileManager) {
		super(eventloop);
		this.fileManager = fileManager;
	}

	// set up connection
	@Override
	protected final AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocketImpl asyncTcpSocket) {
		MessagingConnection<FsCommand, FsResponse> messaging =
				new MessagingConnection<>(eventloop, asyncTcpSocket,
						MessagingSerializers.ofGson(getCommandGSON(), FsCommand.class, getResponseGson(), FsResponse.class));

		messaging.read();


		return messaging;
	}

	protected Gson getResponseGson() {
		return FsResponses.responseGson;
	}

	protected Gson getCommandGSON() {
		return FsCommands.commandGSON;
	}

	protected void addHandlers(StreamMessagingConnection<FsCommand, FsResponse> conn) {
		conn.addHandler(Upload.class, new UploadMessagingHandler());
		conn.addHandler(Download.class, new DownloadMessagingHandler());
		conn.addHandler(Delete.class, new DeleteMessagingHandler());
		conn.addHandler(ListFiles.class, new ListFilesMessagingHandler());
	}

	// handler classes
	private class UploadMessagingHandler implements MessagingHandler<Upload, FsResponse> {
		@Override
		public void onMessage(final Upload item, final Messaging<FsCommands.Upload, FsResponse> messaging) {
			logger.info("received command to upload file: {}", item.filePath);
			messaging.write(new Ok(), new CompletionCallback() {
				@Override
				public void onException(Exception exception) {

				}

				@Override
				public void onComplete() {

				}
			});
			upload(item.filePath, messaging.read(), new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("succeed to upload file: {}", item.filePath);
					messaging.sendMessage(new Acknowledge());
					messaging.shutdown();
				}

				@Override
				public void onException(Exception e) {
					logger.error("failed to upload file: {}", item.filePath, e);
					messaging.sendMessage(new Err(e.getMessage()));
					messaging.shutdown();
				}
			});
		}
	}

	private class DownloadMessagingHandler implements MessagingHandler<Download, FsResponse> {
		@Override
		public void onMessage(final Download item, final Messaging<FsResponse> messaging) {
			logger.info("received command to download file: {}", item.filePath);
			fileManager.size(item.filePath, new ResultCallback<Long>() {
				@Override
				public void onResult(final Long size) {
					if (size < 0) {
						logger.warn("missing file: {}", item.filePath);
						messaging.sendMessage(new Err("File not found"));
						messaging.shutdown();
					} else {
						// preventing output stream from being explicitly closed
						messaging.shutdownReader();
						download(item.filePath, item.startPosition, new ResultCallback<StreamProducer<ByteBuf>>() {
							@Override
							public void onResult(StreamProducer<ByteBuf> result) {
								logger.info("opened stream for file: {}", item.filePath);
								messaging.sendMessage(new Ready(size));
								messaging.writeStream(result, new CompletionCallback() {
									@Override
									public void onComplete() {
										logger.info("succeed to stream file: {}", item.filePath);
										messaging.shutdownWriter();
									}

									@Override
									public void onException(Exception e) {
										logger.error("failed to stream file: {}", item.filePath, e);
										messaging.shutdownWriter();
									}
								});
							}

							@Override
							public void onException(Exception e) {
								logger.error("failed to open stream for file: {}", item.filePath, e);
								messaging.shutdown();
							}
						});
					}
				}

				@Override
				public void onException(Exception e) {
					messaging.sendMessage(new Err(e.getMessage()));
					messaging.shutdown();
				}
			});
		}
	}

	private class DeleteMessagingHandler implements MessagingHandler<Delete, FsResponse> {
		@Override
		public void onMessage(final Delete item, final Messaging<FsResponse> messaging) {
			logger.info("received command to delete file: {}", item.filePath);
			delete(item.filePath, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("succeed to delete file: {}", item.filePath);
					messaging.sendMessage(new Ok());
					messaging.shutdown();
				}

				@Override
				public void onException(Exception e) {
					logger.error("failed to delete file: {}", item.filePath, e);
					messaging.sendMessage(new Err(e.getMessage()));
					messaging.shutdown();
				}
			});
		}
	}

	private class ListFilesMessagingHandler implements MessagingHandler<ListFiles, FsResponse> {
		@Override
		public void onMessage(ListFiles item, final Messaging<FsResponse> messaging) {
			logger.info("received command to list files");
			list(new ResultCallback<List<String>>() {
				@Override
				public void onResult(List<String> result) {
					logger.info("succeed to list files: {}", result.size());
					messaging.sendMessage(new ListOfFiles(result));
					messaging.shutdown();
				}

				@Override
				public void onException(Exception e) {
					logger.error("failed to list files", e);
					messaging.sendMessage(new Err(e.getMessage()));
					messaging.shutdown();
				}
			});
		}
	}

	// abstract core methods
	protected abstract void upload(String filePath, StreamProducer<ByteBuf> producer, CompletionCallback callback);

	protected abstract void download(String filePath, long startPosition, ResultCallback<StreamProducer<ByteBuf>> callback);

	protected abstract void delete(String filePath, CompletionCallback callback);

	protected abstract void list(ResultCallback<List<String>> callback);
}