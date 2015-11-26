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

// TODO (vsavchuk) rename
public class Ref {
	private Object value;
	private char[] charArray;

	public Ref() {
		this.value = null;
	}

	public Ref(Object value) {
		this.value = value;
	}

	public Object get() {
		return value;
	}

	public void set(Object anotherValue) {
		value = anotherValue;
	}

	public char[] getCharArray(int length) {
		if (charArray == null || charArray.length < length) {
			charArray = new char[length + (length >>> 2)];
		}
		return charArray;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Ref ref = (Ref) o;

		return !(value != null ? !value.equals(ref.value) : ref.value != null);

	}

	@Override
	public String toString() {
		return "Ref{" +
				"value=" + value +
				'}';
	}

	@Override
	public int hashCode() {
		return value != null ? value.hashCode() : 0;
	}
}
