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

package io.datakernel.serializer;

public interface BufferSerializer<T> {
	int serialize(byte[] byteArray, int pos, T item);

	T deserialize(SerializationInputBuffer input);
//	int deserialize(byte[] byteArray, int startPos, T ref);
//	 T deserialize(byte[] byteArray, int startPosContainerForPosition container);
//	 int deserialize(byte[] byteArray, int startPos, T ref); Ref<T>

}
