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
import io.datakernel.codegen.Expressions;
import io.datakernel.codegen.ForVar;
import io.datakernel.serializer.SerializationInputHelper;
import io.datakernel.serializer.SerializationOutputHelper;
import io.datakernel.serializer.SerializerBuilder;

import java.util.Arrays;
import java.util.List;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenList implements SerializerGen {
	private final SerializerGen valueSerializer;

	public SerializerGenList(SerializerGen valueSerializer) {
		this.valueSerializer = checkNotNull(valueSerializer);
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		versions.addRecursive(valueSerializer);
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return List.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression serialize(final Expression value, final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression length = let(length(value));
		Expression len = set(arg(1), callStatic(SerializationOutputHelper.class, "writeVarInt", arg(0), arg(1), length));
		Expression forEach = forEach(value, valueSerializer.getRawType(), new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				return set(arg(1), valueSerializer.serialize(it, version, staticMethods));
			}
		});

		return sequence(length, len, forEach, arg(1));
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression deserialize(Class<?> targetType, final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression len = set(arg(1), callStatic(SerializationInputHelper.class, "readVarInt", arg(0), arg(1), arg(2)));
		final Expression array = let(Expressions.newArray(Object[].class, cast(call(arg(2), "get"), int.class)));
		Expression forEach = expressionFor(cast(call(arg(2), "get"), int.class), new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				return sequence(set(arg(1), valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods)),
						setArrayItem(array, it, cast(call(arg(2), "get"), valueSerializer.getRawType())));
			}
		});

		return sequence(len, array, forEach, call(arg(2), "set", cast(callStatic(Arrays.class, "asList", array), Object.class)), arg(1));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenList that = (SerializerGenList) o;

		if (!valueSerializer.equals(that.valueSerializer)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return valueSerializer.hashCode();
	}
}
