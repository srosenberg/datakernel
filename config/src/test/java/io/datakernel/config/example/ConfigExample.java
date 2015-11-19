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

package io.datakernel.config.example;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverter;
import io.datakernel.config.ConfigConverters;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.List;

public class ConfigExample {
	private static final String CONFIGS_FILE_NAME = "config-example.properties";

	public static void main(String[] args) throws URISyntaxException {
		File configsFile = new File(CONFIGS_FILE_NAME);
		Config configs = Config.ofProperties(configsFile);

		ConfigConverter<String> stringConverter = ConfigConverters.ofString();
		ConfigConverter<Integer> integerConverter = ConfigConverters.ofInteger();
		ConfigConverter<Color> enumConverter = ConfigConverters.ofEnum(Color.class);
		String listSeparator = ",";
		ConfigConverter<List<InetSocketAddress>> addressListConverter =
				ConfigConverters.ofList(ConfigConverters.ofInetSocketAddress(), listSeparator);

		Config nameConfig = configs.getChild("name");
		Config numbersConfig = configs.getChild("numbers");
		Config maxNumberConfig = numbersConfig.getChild("max");
		Config minNumberConfig = numbersConfig.getChild("min");
		Config colorConfig = configs.getChild("color");
		Config addressesConfig = configs.getChild("servers");

		String name = stringConverter.get(nameConfig);
		int minNumber = integerConverter.get(minNumberConfig);
		int maxNumber = integerConverter.get(maxNumberConfig);
		Color color = enumConverter.get(colorConfig);
		List<InetSocketAddress> addresses = addressListConverter.get(addressesConfig);

		System.out.println("name: " + name);
		System.out.println("minNumber: " + minNumber);
		System.out.println("maxNumber: " + maxNumber);
		System.out.println("color: " + color);
		System.out.println("addresses: " + addresses);
	}

	public enum Color {
		RED, GREEN, BLUE
	}
}
