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

package io.datakernel.stream.net;

import io.datakernel.async.ParseException;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serializer.BufferSerializer;

public class MessagingSerializerAdapter<I, O> implements MessagingSerializer<I, O> {
	private final BufferSerializer<Integer> bufferSerializer;

	public MessagingSerializerAdapter(BufferSerializer<Integer> bufferSerializer) {
		this.bufferSerializer = bufferSerializer;
	}

	@Override
	public I tryDeserialize(ByteBuf buf) throws ParseException {
		if (!buf.hasRemaining())
			return null;
		// TODO
//		try {
////			byte  = buf.peek(0);
//		}
		return null;
	}

	@Override
	public ByteBuf serialize(O item) {
		return null;
	}
}
