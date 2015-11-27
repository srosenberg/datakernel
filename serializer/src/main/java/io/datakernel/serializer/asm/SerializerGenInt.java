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

import static io.datakernel.codegen.Expressions.*;

public final class SerializerGenInt extends SerializerGenPrimitive {

	private final boolean varLength;

	public SerializerGenInt(boolean varLength) {
		super(int.class);
		this.varLength = varLength;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenInt that = (SerializerGenInt) o;

		if (varLength != that.varLength) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = 0;
		result = 31 * result + (varLength ? 1 : 0);
		return result;
	}

	@Override
	public Expression serialize(Expression value, int version, SerializerBuilder.StaticMethods staticMethods) {
		if (varLength) {
			return callStatic(SerializationOutputHelper.class, "writeVarInt", arg(0), arg(1), cast(value, int.class));
		} else {
			return callStatic(SerializationOutputHelper.class, "writeInt", arg(0), arg(1), cast(value, int.class));
		}
	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods) {
		if (varLength) {
			return callStatic(SerializationInputHelper.class, "readVarInt", arg(0), arg(1));
		} else {
			return callStatic(SerializationInputHelper.class, "readInt", arg(0), arg(1));
		}
	}
}
