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
import io.datakernel.codegen.ForVar;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer.SerializationInputHelper;
import io.datakernel.serializer.SerializationOutputHelper;
import io.datakernel.serializer.SerializerBuilder;

import java.util.*;

import static io.datakernel.codegen.Expressions.*;

public class SerializerGenSet implements SerializerGen {
	private final SerializerGen valueSerializer;

	public SerializerGenSet(SerializerGen valueSerializer) {this.valueSerializer = valueSerializer;}

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
		return Set.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression serialize(Expression value, final int version, final SerializerBuilder.StaticMethods staticMethods) {
		return sequence(
				set(arg(1), callStatic(SerializationOutputHelper.class, "writeVarInt", arg(0), arg(1), call(value, "size"))),
				forEach(value, valueSerializer.getRawType(), new ForVar() {
					@Override
					public Expression forVar(Expression it) {
						return set(arg(1), valueSerializer.serialize(it, version, staticMethods));
					}
				}),	arg(1)
		);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods) {
		boolean isEnum = valueSerializer.getRawType().isEnum();
		Class<?> targetInstance = isEnum ? EnumSet.class : LinkedHashSet.class;
		Preconditions.check(targetType.isAssignableFrom(targetInstance));

		if (!isEnum) {
			return deserializeSimpleSet(version, staticMethods);
		} else {
			return deserializeEnumSet(version, staticMethods);
		}
	}

	private Expression deserializeEnumSet(final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression pos = set(arg(1), callStatic(SerializationInputHelper.class, "readVarInt", arg(0), arg(1), arg(2)));
		Expression getInt = let(cast(call(arg(2), "get"), int.class));
		final Expression container = let(newArray(Object[].class, getInt));
		Expression array = expressionFor(getInt, new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				return sequence(set(arg(1), valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods)),
						setArrayItem(container, it, cast(call(arg(2), "get"), Object.class)));
			}
		});
		Expression list = let(cast(callStatic(Arrays.class, "asList", container), Collection.class));
		Expression enumSet = callStatic(EnumSet.class, "copyOf", list);
		return sequence(pos, container, array, list, call(arg(2), "set", cast(enumSet, Object.class)), arg(1));
	}

	private Expression deserializeSimpleSet(final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression pos = set(arg(1), callStatic(SerializationInputHelper.class, "readVarInt", arg(0), arg(1), arg(2)));
		Expression getInt = let(cast(call(arg(2), "get"), int.class));
		final Expression container = let(constructor(LinkedHashSet.class, getInt));
		return sequence(pos, container, expressionFor(getInt, new ForVar() {
					@Override
					public Expression forVar(Expression it) {
						return sequence(
								set(arg(1), valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods)),
								call(container, "add", cast(call(arg(2), "get"), Object.class)),
								voidExp()
						);
					}
				}), call(arg(2), "set", cast(container, Object.class)), arg(1)
		);
	}
}
