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

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.util.Preconditions.checkArgument;

public final class JmxScanner {

	public ScanResult scan(Object object) {
		AttributeGetter rootGetter = new AttributeGetterConstant(object);
		Class<?> clazz = object.getClass();
		return scan(rootGetter, clazz);
	}

	private ScanResult scan(final AttributeGetter attrGetter, Type type) {
		if (type instanceof Class) {
			Class<?> clazz = (Class<?>) type;
			if (isPrimitiveType(clazz) || isPrimitiveTypeWrapper(clazz) || isString(clazz)) {
				return new ScanResult(new VarScalarImpl(attrGetter), new VarTypeScalarImpl(clazz));
			} else {
				// assume it is POJO
				List<AttributeDescriptor> descriptors = fetchAttributeDescriptors(clazz);
				Map<String, Var> vars = new HashMap<>();
				Map<String, VarType> types = new HashMap<>();
				for (AttributeDescriptor descriptor : descriptors) {
					Method getterMethod = descriptor.getGetterMethod();
					AttributeGetter pojoGetter = new AttributeGetterReflect(attrGetter, getterMethod);
					String name = extractFieldNameFromGetter(getterMethod);
					ScanResult scanResult = scan(pojoGetter, getterMethod.getReturnType());
					vars.put(name, scanResult.getVar());
					types.put(name, scanResult.getVarType());
				}
				Var rootVar = new VarCompositeImpl(vars);
				VarType rootType = new VarTypeCompositeImpl(types);
				return new ScanResult(rootVar, rootType);
			}
		}
		throw new RuntimeException();  // TODO(vmykh)
	}

	private List<AttributeDescriptor> fetchAttributeDescriptors(Class<?> clazz) {
		List<AttributeDescriptor> attrs = new ArrayList<>();
		for (Method method : clazz.getMethods()) {
			if (method.isAnnotationPresent(JmxAttribute2.class) && isGetter(method)) {
				attrs.add(new AttributeDescriptor(method, method.getAnnotation(JmxAttribute2.class)));
			}
		}
		return attrs;
	}

	public static boolean isGetter(Method method) {
		boolean returnsBoolean = method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class;
		boolean isIsGetter = method.getName().length() > 2 && method.getName().startsWith("is") && returnsBoolean;

		boolean doesntReturnVoid = method.getReturnType() != void.class;
		boolean isGetGetter = method.getName().length() > 3 && method.getName().startsWith("get") && doesntReturnVoid;

		return isIsGetter || isGetGetter;
	}

	public static String extractFieldNameFromGetter(Method getter) {
		checkArgument(isGetter(getter));

		if (getter.getName().startsWith("get")) {
			String getterName = getter.getName();
			String firstLetter = getterName.substring(3, 4);
			String restOfName = getterName.substring(4);
			return firstLetter.toLowerCase() + restOfName;
		} else if (getter.getName().startsWith("is")) {
			String getterName = getter.getName();
			String firstLetter = getterName.substring(2, 3);
			String restOfName = getterName.substring(3);
			return firstLetter.toLowerCase() + restOfName;
		} else {
			throw new RuntimeException();
		}
	}

	public static boolean isPrimitiveType(Class<?> clazz) {
		return boolean.class.isAssignableFrom(clazz)
				|| byte.class.isAssignableFrom(clazz)
				|| short.class.isAssignableFrom(clazz)
				|| char.class.isAssignableFrom(clazz)
				|| int.class.isAssignableFrom(clazz)
				|| long.class.isAssignableFrom(clazz)
				|| float.class.isAssignableFrom(clazz)
				|| double.class.isAssignableFrom(clazz);
	}

	public static boolean isPrimitiveTypeWrapper(Class<?> clazz) {
		return Boolean.class.isAssignableFrom(clazz)
				|| Byte.class.isAssignableFrom(clazz)
				|| Short.class.isAssignableFrom(clazz)
				|| Character.class.isAssignableFrom(clazz)
				|| Integer.class.isAssignableFrom(clazz)
				|| Long.class.isAssignableFrom(clazz)
				|| Float.class.isAssignableFrom(clazz)
				|| Double.class.isAssignableFrom(clazz);
	}

	public static boolean isString(Class<?> clazz) {
		return String.class.isAssignableFrom(clazz);
	}

	private static final class AttributeDescriptor {
		private final Method getterMethod;
		private final JmxAttribute2 annotation;

		public AttributeDescriptor(Method getterMethod, JmxAttribute2 annotation) {
			this.getterMethod = getterMethod;
			this.annotation = annotation;
		}

		public Method getGetterMethod() {
			return getterMethod;
		}

		public JmxAttribute2 getAnnotation() {
			return annotation;
		}
	}
}
