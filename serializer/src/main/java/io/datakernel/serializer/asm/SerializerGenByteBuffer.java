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

package io.datakernel.serializer.asm;

import io.datakernel.codegen.Expression;
import io.datakernel.serializer.SerializationInputHelper;
import io.datakernel.serializer.SerializationOutputHelper;
import io.datakernel.serializer.SerializerBuilder;

import java.nio.ByteBuffer;

import static io.datakernel.codegen.Expressions.*;

public class SerializerGenByteBuffer implements SerializerGen {
	private final boolean wrapped;

	public SerializerGenByteBuffer() {
		this(false);
	}

	public SerializerGenByteBuffer(boolean wrapped) {
		this.wrapped = wrapped;
	}

	@Override
	public void getVersions(VersionsCollector versions) {
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return ByteBuffer.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {

	}

	@Override
	public Expression serialize(Expression value, int version, SerializerBuilder.StaticMethods staticMethods) {
		Expression array = call(cast(value, ByteBuffer.class), "array");
		Expression position = call(cast(value, ByteBuffer.class), "position");
		Expression remaining = let(call(cast(value, ByteBuffer.class), "remaining"));
		Expression writeLength = set(arg(1), callStatic(SerializationOutputHelper.class, "writeVarInt", arg(0), arg(1), remaining));

		return sequence(writeLength, set(arg(1), callStatic(SerializationOutputHelper.class, "write", array, position, arg(0), arg(1), remaining)), arg(1));
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {

	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods) {
		Expression pos = set(arg(1), callStatic(SerializationInputHelper.class, "readVarInt", arg(0), arg(1), arg(2)));
		Expression getLength = let(cast(call(arg(2), "get"), int.class));

		if (!wrapped) {
			return sequence(pos,
					set(arg(1), callStatic(SerializationInputHelper.class, "read", arg(0), arg(1), getLength, arg(2))),
					call(arg(2), "set", cast(callStatic(ByteBuffer.class, "wrap", cast(call(arg(2), "get"), byte[].class)), Object.class)),
					arg(1));
		} else {

			return sequence(pos,
					call(arg(2), "set", cast(callStatic(ByteBuffer.class, "wrap", arg(0), arg(1), getLength), Object.class)),
					add(arg(1), getLength));
		}
	}
}
