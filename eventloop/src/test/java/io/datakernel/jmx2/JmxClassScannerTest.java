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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JmxClassScannerTest {

	@Test
	public void collectsDirectScalarJmxAttributes() {
		PojoWithJmxScalars pojo = new PojoWithJmxScalars(10, "ukr");
		JmxScanner scanner = new JmxScanner();
		ScanResult scanResult = scanner.scan(pojo);

		// check types
		VarTypeComposite type = (VarTypeComposite) scanResult.getVarType();
		assertEquals(2, type.getFieldTypes().size());

		VarType countType = type.getFieldTypes().get("count");
		Class<?> actualJavaTypeOfCount = ((VarTypeScalar) countType).getJavaType();
		assertEquals(int.class, actualJavaTypeOfCount);

		VarType nameType = type.getFieldTypes().get("name");
		Class<?> actualJavaTypeOfName = ((VarTypeScalar) nameType).getJavaType();
		assertEquals(String.class, actualJavaTypeOfName);

		// check values
		VarComposite var = (VarComposite) scanResult.getVar();
		Var count = var.get("count");
		assertEquals(10, ((VarScalar) count).get());
		Var name = var.get("name");
		assertEquals("ukr", ((VarScalar) name).get());
	}

	public static class PojoWithJmxScalars {
		private final int count;
		private final String name;

		public PojoWithJmxScalars(int count, String name) {
			this.count = count;
			this.name = name;
		}

		@JmxAttribute2
		public int getCount() {
			return count;
		}

		@JmxAttribute2
		public String getName() {
			return name;
		}
	}

	@Test
	public void collectsScalarJmxAttributesInIntermediatePOJO() {
		PojoWithInnerPojo pojo = new PojoWithInnerPojo(new InnerPojoWithScalars(100L));
		JmxScanner scanner = new JmxScanner();
		ScanResult scanResult = scanner.scan(pojo);

		// check types
		VarTypeComposite rootType = (VarTypeComposite) scanResult.getVarType();
		assertEquals(1, rootType.getFieldTypes().size());

		VarTypeComposite typeOfInnerPojo = (VarTypeComposite) rootType.getFieldTypes().get("innerPojo");
		assertEquals(1, typeOfInnerPojo.getFieldTypes().size());

		VarTypeScalar typeOfTotal = (VarTypeScalar) typeOfInnerPojo.getFieldTypes().get("total");
		Class<?> actualJavaTypeOfCount = typeOfTotal.getJavaType();
		assertEquals(long.class, actualJavaTypeOfCount);

		// check values
		VarComposite rootVar = (VarComposite) scanResult.getVar();
		VarComposite varOfInnerPojo = (VarComposite) rootVar.get("innerPojo");
		VarScalar varOfTotal = (VarScalar) varOfInnerPojo.get("total");
		assertEquals(100L, varOfTotal.get());
	}

	public static class PojoWithInnerPojo {
		private final InnerPojoWithScalars innerPojo;

		public PojoWithInnerPojo(InnerPojoWithScalars innerPojo) {
			this.innerPojo = innerPojo;
		}

		@JmxAttribute2
		public InnerPojoWithScalars getInnerPojo() {
			return innerPojo;
		}
	}

	public static class InnerPojoWithScalars {
		private final long total;

		public InnerPojoWithScalars(long total) {
			this.total = total;
		}

		@JmxAttribute2
		public long getTotal() {
			return total;
		}
	}
}
