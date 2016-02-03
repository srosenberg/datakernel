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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.nio.file.StandardOpenOption.*;
import static org.slf4j.LoggerFactory.getLogger;

public final class FileSystem {
	private static final Logger logger = getLogger(FileSystem.class);
	public static final String DEFAULT_IN_PROGRESS_EXTENSION = ".partial";

	private final Eventloop eventloop;
	private final ExecutorService executor;

	private final Path storage;
	private final Path tmpStorage;

	private final String inProgressExtension = DEFAULT_IN_PROGRESS_EXTENSION;

	// creators
	private FileSystem(Eventloop eventloop, ExecutorService executor,
	                   Path storage, Path tmpStorage) {

		this.eventloop = checkNotNull(eventloop);
		this.executor = checkNotNull(executor);

		check(!(storage.startsWith(tmpStorage) || tmpStorage.startsWith(storage)), "Directories must not relate(i.e. parent-child)");
		this.storage = storage;
		this.tmpStorage = tmpStorage;
	}

	public static FileSystem newInstance(Eventloop eventloop, ExecutorService executor, Path storage, Path tmpStorage) {
		return new FileSystem(eventloop, executor, storage, tmpStorage);
	}

	// api
	public void saveToTmp(String fileName, ResultCallback<AsyncFile> callback) {
		logger.trace("Saving to temporary dir {}", fileName);
		try {
			Path tmpPath = ensureInProgressDirectory(fileName);
			AsyncFile.open(eventloop, executor, tmpPath, new OpenOption[]{CREATE, WRITE, TRUNCATE_EXISTING}, callback);
		} catch (IOException e) {
			logger.trace("Caught exception while trying to ensure in-progress-directory: {}", e.getMessage());
			callback.onException(e);
		}
	}

	public void commitTmp(String fileName, CompletionCallback callback) {
		logger.trace("Moving file from temporary dir to {}", fileName);
		Path destinationPath;
		Path tmpPath;
		try {
			tmpPath = ensureInProgressDirectory(fileName);
			destinationPath = ensureDestinationDirectory(fileName);
		} catch (IOException e) {
			logger.error("Can't ensure directory for {}", fileName, e);
			callback.onException(e);
			return;
		}
		try {
			Files.move(tmpPath, destinationPath);
			callback.onComplete();
		} catch (IOException e) {
			logger.error("Can't move file from temporary dir to {}", fileName, e);
			try {
				Files.delete(tmpPath);
			} catch (IOException ignored) {
				logger.error("Can't delete file that didn't manage to move from temporary", ignored);
			}
			callback.onException(e);
		}
	}

	public void deleteTmp(String fileName, CompletionCallback callback) {
		logger.trace("Deleting temporary file {}", fileName);
		Path path = tmpStorage.resolve(fileName + inProgressExtension);
		AsyncFile.delete(eventloop, executor, path, callback);
	}

	public void get(String fileName, ResultCallback<AsyncFile> callback) {
		logger.trace("Streaming file {}", fileName);
		Path destination = storage.resolve(fileName);
		AsyncFile.open(eventloop, executor, destination, new OpenOption[]{READ}, callback);
	}

	public void delete(String fileName, CompletionCallback callback) {
		logger.trace("Deleting file {}", fileName);
		Path path = storage.resolve(fileName);
		AsyncFile.delete(eventloop, executor, path, callback);
	}

	public void list(ResultCallback<List<String>> callback) {
		logger.trace("Listing files");
		List<String> result = new ArrayList<>();
		try {
			listFiles(storage, result, "");
			callback.onResult(result);
		} catch (IOException e) {
			logger.error("Can't list files", e);
			callback.onException(e);
		}
	}

	public long fileSize(String fileName) {
		File file = storage.resolve(fileName).toFile();
		if (!file.exists() || file.isDirectory()) {
			return -1;
		} else {
			return file.length();
		}
	}

	// utils
	private void listFiles(Path parent, List<String> files, String previousPath) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(parent)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path)) {
					listFiles(path, files, previousPath + path.getFileName().toString() + File.separator);
				} else {
					files.add(previousPath + path.getFileName().toString());
				}
			}
		}
	}

	private Path ensureDestinationDirectory(String path) throws IOException {
		return ensureDirectory(storage, path);
	}

	private Path ensureInProgressDirectory(String path) throws IOException {
		return ensureDirectory(tmpStorage, path + inProgressExtension);
	}

	private Path ensureDirectory(Path container, String path) throws IOException {
		String fileName = getFileName(path);
		String filePath = getPathToFile(path);
		Path destinationDirectory = container.resolve(filePath);
		Files.createDirectories(destinationDirectory);
		return destinationDirectory.resolve(fileName);
	}

	private String getFileName(String filePath) {
		if (filePath.contains(File.separator)) {
			filePath = filePath.substring(filePath.lastIndexOf(File.separator) + 1);
		}
		return filePath;
	}

	private String getPathToFile(String filePath) {
		if (filePath.contains(File.separator)) {
			filePath = filePath.substring(0, filePath.lastIndexOf(File.separator));
		} else {
			filePath = "";
		}
		return filePath;
	}

	public void initDirectories() throws IOException {
		Files.createDirectories(storage);
		Files.createDirectories(tmpStorage);
		cleanDirectory(tmpStorage);
	}

	private void cleanDirectory(Path directory) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path) && path.iterator().hasNext()) {
					cleanDirectory(path);
				}
				Files.delete(path);
			}
		}
	}
}