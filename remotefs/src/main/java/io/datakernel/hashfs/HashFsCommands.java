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

package io.datakernel.hashfs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.protocol.FsCommands;
import io.datakernel.serializer.GsonSubclassesAdapter;

import java.util.Set;

import static java.util.Collections.unmodifiableSet;

final class HashFsCommands extends FsCommands {
	static Gson commandGSON = new GsonBuilder()
			.registerTypeAdapter(FsCommand.class, GsonSubclassesAdapter.builder()
					.subclassField("commandType")
					.subclass("Upload", Upload.class)
					.subclass("Commit", Commit.class)
					.subclass("Download", Download.class)
					.subclass("Delete", Delete.class)
					.subclass("List", ListFiles.class)
					.subclass("Alive", Alive.class)
					.subclass("Offer", Offer.class)
					.build())
			.setPrettyPrinting()
			.enableComplexMapKeySerialization()
			.create();

	public static class Alive extends FsCommand {

		@Override
		public String toString() {
			return "Alive{servers}";
		}
	}

	public static class Offer extends FsCommand {
		public final Set<String> forDeletion;
		public final Set<String> forUpload;

		public Offer(Set<String> forDeletion, Set<String> forUpload) {
			this.forDeletion = unmodifiableSet(forDeletion);
			this.forUpload = unmodifiableSet(forUpload);
		}

		@Override
		public String toString() {
			return "Offer{forDeletion:" + forDeletion.size() + ",forUpload:" + forUpload.size() + "}";
		}

	}
}