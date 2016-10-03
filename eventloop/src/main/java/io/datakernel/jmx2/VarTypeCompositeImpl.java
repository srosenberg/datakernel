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

import java.util.Map;

public class VarTypeCompositeImpl implements VarTypeComposite {
	private final Map<String, VarType> subtypes;

	public VarTypeCompositeImpl(Map<String, VarType> subtypes) {
		this.subtypes = subtypes;
	}

	@Override
	public Map<String, VarType> getFieldTypes() {
		return subtypes;
	}
}
