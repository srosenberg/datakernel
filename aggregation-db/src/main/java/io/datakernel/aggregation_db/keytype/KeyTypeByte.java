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

package io.datakernel.aggregation_db.keytype;

import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.asm.SerializerGenByte;

public class KeyTypeByte extends KeyType implements KeyTypeEnumerable {
	public KeyTypeByte() {
		super(byte.class);
	}

	@Override
	public SerializerGen serializerGen() {
		return new SerializerGenByte();
	}

	@Override
	public Object toInternalRepresentation(String o) {
		return Byte.valueOf(o);
	}

	@Override
	public Object increment(Object object) {
		Byte byteToIncrement = (Byte) object;
		return ++byteToIncrement;
	}

	@Override
	public long difference(Object o1, Object o2) {
		return ((Byte) o1) - ((Byte) o2);
	}

	@Override
	public int compare(Object o1, Object o2) {
		return ((Byte) o1).compareTo((Byte) o2);
	}
}