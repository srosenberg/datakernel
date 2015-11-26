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
import io.datakernel.codegen.ExpressionLet;
import io.datakernel.codegen.ForVar;
import io.datakernel.serializer.SerializationInputHelper;
import io.datakernel.serializer.SerializationOutputHelper;
import io.datakernel.serializer.SerializerBuilder;

import java.util.*;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.getType;

@SuppressWarnings("PointlessArithmeticExpression")
public final class SerializerGenMap implements SerializerGen {
	private final SerializerGen keySerializer;
	private final SerializerGen valueSerializer;

	public SerializerGenMap(SerializerGen keySerializer, SerializerGen valueSerializer) {
		this.keySerializer = checkNotNull(keySerializer);
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
		return Map.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		keySerializer.prepareSerializeStaticMethods(version, staticMethods);
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression serialize(Expression value, final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression length = set(arg(1), callStatic(SerializationOutputHelper.class, "writeVarInt", arg(0), arg(1), length(value)));

		return sequence(length,	mapForEach(value,
						new ForVar() {
							@Override
							public Expression forVar(Expression it) {return set(arg(1), keySerializer.serialize(cast(it, keySerializer.getRawType()), version, staticMethods));}
						},
						new ForVar() {
							@Override
							public Expression forVar(Expression it) {return set(arg(1), valueSerializer.serialize(cast(it, valueSerializer.getRawType()), version, staticMethods));}
						}),	arg(1)
		);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {
		keySerializer.prepareDeserializeStaticMethods(version, staticMethods);
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods);
	}

	@Override
	public Expression deserialize(Class<?> targetType, final int version, SerializerBuilder.StaticMethods staticMethods) {
		boolean isEnum = keySerializer.getRawType().isEnum();

		if (!isEnum) {
			return deserializeSimple(version, staticMethods);
		} else {
			return deserializeEnum(version, staticMethods);
		}
	}

	public Expression deserializeSimple(final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression pos = set(arg(1), callStatic(SerializationInputHelper.class, "readVarInt", arg(0), arg(1), arg(2)));
		Expression getInt = let(cast(call(arg(2), "get"), int.class));
		final Expression local = let(constructor(LinkedHashMap.class, getInt));
		Expression forEach = expressionFor(getInt, new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				List<Expression> list = new ArrayList<>();

				list.add(set(arg(1), keySerializer.deserialize(keySerializer.getRawType(), version, staticMethods)));
				ExpressionLet varKey = let(cast(call(arg(2), "get"), Object.class));
				list.add(varKey);

				list.add(set(arg(1), valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods)));
				ExpressionLet varValue = let(cast(call(arg(2), "get"), Object.class));
				list.add(varValue);

				list.add(call(local, "put", varKey, varValue));
				list.add(voidExp());

				return sequence(list);
			}
		});
		return sequence(pos, local, forEach, call(arg(2), "set", cast(local, Object.class)), arg(1));
	}

	public Expression deserializeEnum(final int version, final SerializerBuilder.StaticMethods staticMethods) {
		Expression pos = set(arg(1), callStatic(SerializationInputHelper.class, "readVarInt", arg(0), arg(1), arg(2)));
		Expression getInt = let(cast(call(arg(2), "get"), int.class));
		final Expression localMap = let(constructor(EnumMap.class, cast(value(getType(keySerializer.getRawType())), Class.class)));
		Expression forEach = expressionFor(getInt, new ForVar() {
			@Override
			public Expression forVar(Expression it) {
				List<Expression> list = new ArrayList<>();

				list.add(set(arg(1), keySerializer.deserialize(keySerializer.getRawType(), version, staticMethods)));
				ExpressionLet varKey = let(cast(call(arg(2), "get"), Object.class));
				list.add(varKey);

				list.add(set(arg(1), valueSerializer.deserialize(valueSerializer.getRawType(), version, staticMethods)));
				ExpressionLet varValue = let(cast(call(arg(2), "get"), Object.class));
				list.add(varValue);

				list.add(call(localMap, "put", varKey, varValue));
				list.add(voidExp());

				return sequence(list);
			}
		});
		return sequence(pos, localMap, forEach, call(arg(2), "set", cast(localMap, Object.class)), arg(1));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenMap that = (SerializerGenMap) o;

		if (!keySerializer.equals(that.keySerializer)) return false;
		if (!valueSerializer.equals(that.valueSerializer)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = keySerializer.hashCode();
		result = 31 * result + valueSerializer.hashCode();
		return result;
	}
}
