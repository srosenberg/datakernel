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

package io.datakernel.config;

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

/**
 * Provides an ability to create custom implementations of converters by simply
 * extending this class and overriding {@link #toString(T)} and
 * {@link #fromString(String)} which define how do converters actually convert
 * a specific data type.
 * <p>
 * A {@link ConfigConverters} class provides convenient implementations for
 * different often used data types.
 *
 * @param <T> data type for conversion
 * @see ConfigConverters
 */
public abstract class ConfigConverterSingle<T> implements ConfigConverter<T> {

	protected abstract T fromString(String string);

	protected abstract String toString(T item);

	/**
	 * Returns a T-type value of a property, represented by a given config.
	 *
	 * @param config a config instance which represents a property
	 * @return value of the property
	 */
	@Override
	public final T get(Config config) {
		checkState(config.getChildren().isEmpty());
		String string = config.get();
		checkNotNull(string, "Config %s not found", config);
		return fromString(string);
	}

	/**
	 * Returns a T-type value of a property, represented by a given config.
	 * Assigns the default value of the property.
	 *
	 * @param config a config instance which represents a property
	 * @param defaultValue default value of the property
	 * @return value of the property
	 */
	@Override
	public final T get(Config config, T defaultValue) {
		checkState(config.getChildren().isEmpty());
		String defaultString = toString(defaultValue);
		String string = config.get(defaultString);
		checkNotNull(string);
		string = string.trim();
		T result = fromString(string);
		checkNotNull(result);
		config.set(toString(result));
		return result;
	}
}
