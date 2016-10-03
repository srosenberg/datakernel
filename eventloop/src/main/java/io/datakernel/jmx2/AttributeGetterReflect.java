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

package io.datakernel.jmx2;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

final class AttributeGetterReflect implements AttributeGetter {
	private final AttributeGetter upperGetter;
	private final Method getterMethod;

	public AttributeGetterReflect(AttributeGetter upperGetter, Method getterMethod) {
		this.upperGetter = upperGetter;
		this.getterMethod = getterMethod;
	}

	@Override
	public Object get() {
		Object source = upperGetter.get();
		try {
			return getterMethod.invoke(source);
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e); // TODO(vmykh)
		}
	}
}
