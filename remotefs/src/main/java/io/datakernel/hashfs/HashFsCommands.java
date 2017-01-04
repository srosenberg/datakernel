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
import io.datakernel.FsCommands;
import io.datakernel.serializer.GsonSubclassesAdapter;

import java.util.List;

import static java.util.Collections.unmodifiableList;

final class HashFsCommands extends FsCommands {
	public static Gson commandGSON = new GsonBuilder()
			.registerTypeAdapter(FsCommand.class, GsonSubclassesAdapter.create()
					.withSubclassField("commandType")
					.withSubclass("Upload", Upload.class)
					.withSubclass("Download", Download.class)
					.withSubclass("Delete", Delete.class)
					.withSubclass("List", ListFiles.class)
					.withSubclass("Alive", Alive.class)
					.withSubclass("Announce", Announce.class))
			.setPrettyPrinting()
			.enableComplexMapKeySerialization()
			.create();

	public static class Alive extends FsCommand {
		@Override
		public String toString() {
			return "Alive{servers}";
		}
	}

	public static class Announce extends FsCommand {
		final List<String> forDeletion;
		final List<String> forUpload;

		Announce(List<String> forDeletion, List<String> forUpload) {
			this.forDeletion = unmodifiableList(forDeletion);
			this.forUpload = unmodifiableList(forUpload);
		}

		@Override
		public String toString() {
			return "Offer{forDeletion:" + forDeletion.size() + ",forUpload:" + forUpload.size() + "}";
		}
	}
}