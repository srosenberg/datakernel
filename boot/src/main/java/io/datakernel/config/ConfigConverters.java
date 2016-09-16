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

import com.google.common.base.Preconditions;
import io.datakernel.eventloop.InetAddressRange;
import io.datakernel.exception.ParseException;
import io.datakernel.net.DatagramSocketSettings;
import io.datakernel.net.ServerSocketSettings;
import io.datakernel.net.SocketSettings;
import io.datakernel.util.MemSize;
import io.datakernel.util.StringUtils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.datakernel.net.DatagramSocketSettings.defaultDatagramSocketSettings;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyList;

@SuppressWarnings("unused, WeakerAccess")
public final class ConfigConverters {

	private static final class ConfigConverterString extends ConfigConverterSingle<String> {
		static ConfigConverterString INSTANCE = new ConfigConverterString();

		@Override
		public String fromString(String string) {
			return checkNotNull(string);
		}

		@Override
		public String toString(String item) {
			return checkNotNull(item);
		}
	}

	private static final class ConfigConverterByte extends ConfigConverterSingle<Byte> {
		static ConfigConverterByte INSTANCE = new ConfigConverterByte();

		@Override
		public Byte fromString(String string) {
			return Byte.parseByte(string);
		}

		@Override
		public String toString(Byte item) {
			return Byte.toString(item);
		}
	}

	private static final class ConfigConverterInteger extends ConfigConverterSingle<Integer> {
		static ConfigConverterInteger INSTANCE = new ConfigConverterInteger();

		@Override
		public Integer fromString(String string) {
			return Integer.parseInt(string);
		}

		@Override
		public String toString(Integer item) {
			return Integer.toString(item);
		}
	}

	private static final class ConfigConverterLong extends ConfigConverterSingle<Long> {
		static ConfigConverterLong INSTANCE = new ConfigConverterLong();

		@Override
		public Long fromString(String string) {
			return Long.parseLong(string);
		}

		@Override
		public String toString(Long item) {
			return Long.toString(item);
		}
	}

	private static final class ConfigConverterFloat extends ConfigConverterSingle<Float> {
		static ConfigConverterFloat INSTANCE = new ConfigConverterFloat();

		@Override
		public Float fromString(String string) {
			return Float.parseFloat(string);
		}

		@Override
		public String toString(Float item) {
			return Float.toString(item);
		}
	}

	private static final class ConfigConverterDouble extends ConfigConverterSingle<Double> {
		static ConfigConverterDouble INSTANCE = new ConfigConverterDouble();

		@Override
		public Double fromString(String string) {
			return Double.parseDouble(string);
		}

		@Override
		public String toString(Double item) {
			return Double.toString(item);
		}
	}

	private static final class ConfigConverterBoolean extends ConfigConverterSingle<Boolean> {
		static ConfigConverterBoolean INSTANCE = new ConfigConverterBoolean();

		@Override
		public Boolean fromString(String string) {
			return Boolean.parseBoolean(string);
		}

		@Override
		public String toString(Boolean item) {
			return Boolean.toString(item);
		}
	}

	private static final class ConfigConverterEnum<E extends Enum<E>> extends ConfigConverterSingle<E> {
		private final Class<E> enumClass;

		public ConfigConverterEnum(Class<E> enumClass) {
			this.enumClass = enumClass;
		}

		@Override
		protected E fromString(String string) {
			return Enum.valueOf(enumClass, string);
		}

		@Override
		protected String toString(E item) {
			return item.name();
		}
	}

	private static final class ConfigConverterInetSocketAddress extends ConfigConverterSingle<InetSocketAddress> {
		static ConfigConverterInetSocketAddress INSTANCE = new ConfigConverterInetSocketAddress();

		@Override
		protected InetSocketAddress fromString(String addressPort) {
			int portPos = addressPort.lastIndexOf(':');
			checkArgument(portPos != -1, "Invalid address. Port is not specified");
			String addressStr = addressPort.substring(0, portPos);
			String portStr = addressPort.substring(portPos + 1);
			int port = parseInt(portStr);
			checkArgument(port > 0 && port < 65536, "Invalid address. Port is not in range (0, 65536) " + addressStr);
			InetSocketAddress socketAddress;
			if ("*".equals(addressStr)) {
				socketAddress = new InetSocketAddress(port);
			} else {
				try {
					InetAddress address = InetAddress.getByName(addressStr);
					socketAddress = new InetSocketAddress(address, port);
				} catch (UnknownHostException e) {
					throw new IllegalArgumentException(e);
				}
			}
			return socketAddress;
		}

		@Override
		protected String toString(InetSocketAddress item) {
			return item.getAddress().getHostAddress() + ":" + item.getPort();
		}
	}

	private static final class ConfigConverterList<T> extends ConfigConverterSingle<List<T>> {
		private final ConfigConverterSingle<T> elementConverter;
		private final CharSequence separators;
		private final char joinSeparator;

		public ConfigConverterList(ConfigConverterSingle<T> elementConverter, CharSequence separators) {
			this.elementConverter = elementConverter;
			this.joinSeparator = separators.charAt(0);
			this.separators = separators;
		}

		@Override
		protected List<T> fromString(String string) {
			string = string.trim();
			if (string.isEmpty())
				return emptyList();
			List<T> list = new ArrayList<>();
			for (String elementString : StringUtils.splitToList(separators, string)) {
				T element = elementConverter.fromString(elementString.trim());
				list.add(element);
			}
			return Collections.unmodifiableList(list);
		}

		@Override
		protected String toString(List<T> item) {
			List<String> strings = new ArrayList<>(item.size());
			for (T e : item) {
				strings.add(elementConverter.toString(e));
			}
			return StringUtils.join(joinSeparator, strings);
		}
	}

	private static final class ConfigConverterMemSize extends ConfigConverterSingle<MemSize> {
		static ConfigConverterMemSize INSTANCE = new ConfigConverterMemSize();

		@Override
		protected MemSize fromString(String string) {
			return MemSize.valueOf(string);
		}

		@Override
		protected String toString(MemSize item) {
			return item.format();
		}
	}

	private static final class ConfigConverterServerSocketSettings implements ConfigConverter<ServerSocketSettings> {
		static ConfigConverterServerSocketSettings INSTANCE = new ConfigConverterServerSocketSettings();

		private final ServerSocketSettings defaultValue = ServerSocketSettings.create();

		@Override
		public ServerSocketSettings get(Config config) {
			return get(config, defaultValue);
		}

		@Override
		public ServerSocketSettings get(Config config, ServerSocketSettings defaultValue) {
			ServerSocketSettings result = Preconditions.checkNotNull(defaultValue);
			result = result.withBacklog(config.get(ofInteger(), "backlog", result.getBacklog()));
			if (config.hasValue("receiveBufferSize"))
				result = result.withReceiveBufferSize(config.get(ofInteger(), "receiveBufferSize"));
			if (config.hasValue("reuseAddress"))
				result = result.withReuseAddress(config.get(ofBoolean(), "reuseAddress"));
			return result;
		}
	}

	private static final class ConfigConverterSocketSettings implements ConfigConverter<SocketSettings> {
		static ConfigConverterSocketSettings INSTANCE = new ConfigConverterSocketSettings();

		private final SocketSettings defaultValue = SocketSettings.create();

		@Override
		public SocketSettings get(Config config) {
			return get(config, defaultValue);
		}

		@Override
		public SocketSettings get(Config config, SocketSettings defaultValue) {
			SocketSettings result = Preconditions.checkNotNull(defaultValue);
			if (config.hasValue("receiveBufferSize"))
				result = result.withReceiveBufferSize((int) config.get(ofMemSize(), "receiveBufferSize").getBytes());
			if (config.hasValue("sendBufferSize"))
				result = result.withSendBufferSize((int) config.get(ofMemSize(), "sendBufferSize").getBytes());
			if (config.hasValue("reuseAddress"))
				result = result.withReuseAddress(config.get(ofBoolean(), "reuseAddress"));
			if (config.hasValue("keepAlive"))
				result = result.withKeepAlive(config.get(ofBoolean(), "keepAlive"));
			if (config.hasValue("tcpNoDelay"))
				result = result.withTcpNoDelay(config.get(ofBoolean(), "tcpNoDelay"));
			if (config.hasValue("readTimeout"))
				result = result.withReadTimeout(config.get(ofLong(), "readTimeout"));
			if (config.hasValue("writeTimeout"))
				result = result.withWriteTimeout(config.get(ofLong(), "writeTimeout"));
			return result;
		}
	}

	private static final class ConfigConverterDatagramSocketSettings implements ConfigConverter<DatagramSocketSettings> {
		static ConfigConverterDatagramSocketSettings INSTANCE = new ConfigConverterDatagramSocketSettings();

		@Override
		public DatagramSocketSettings get(Config config) {
			return get(config, defaultDatagramSocketSettings());
		}

		@Override
		public DatagramSocketSettings get(Config config, DatagramSocketSettings defaultValue) {
			DatagramSocketSettings result = Preconditions.checkNotNull(defaultDatagramSocketSettings());
			if (config.hasValue("receiveBufferSize"))
				result = result.withReceiveBufferSize((int) config.get(ofMemSize(), "receiveBufferSize").getBytes());
			if (config.hasValue("sendBufferSize"))
				result = result.withSendBufferSize((int) config.get(ofMemSize(), "sendBufferSize").getBytes());
			if (config.hasValue("reuseAddress"))
				result = result.withReuseAddress(config.get(ofBoolean(), "reuseAddress"));
			if (config.hasValue("broadcast"))
				result = result.withBroadcast(config.get(ofBoolean(), "broadcast"));
			return result;
		}
	}

	private static final class ConfigConverterInetAddressRange extends ConfigConverterSingle<InetAddressRange> {
		static ConfigConverterInetAddressRange INSTANCE = new ConfigConverterInetAddressRange();

		@Override
		protected InetAddressRange fromString(String string) {
			try {
				return InetAddressRange.parse(string);
			} catch (ParseException e) {
				throw new IllegalArgumentException("Can't parse inetAddressRange config", e);
			}
		}

		@Override
		protected String toString(InetAddressRange item) {
			return item.toString();
		}
	}

	public static ConfigConverterSingle<String> ofString() {
		return ConfigConverterString.INSTANCE;
	}

	public static ConfigConverterSingle<Byte> ofByte() {
		return ConfigConverterByte.INSTANCE;
	}

	public static ConfigConverterSingle<Integer> ofInteger() {
		return ConfigConverterInteger.INSTANCE;
	}

	public static ConfigConverterSingle<Long> ofLong() {
		return ConfigConverterLong.INSTANCE;
	}

	public static ConfigConverterSingle<Float> ofFloat() {
		return ConfigConverterFloat.INSTANCE;
	}

	public static ConfigConverterSingle<Double> ofDouble() {
		return ConfigConverterDouble.INSTANCE;
	}

	public static ConfigConverterSingle<Boolean> ofBoolean() {
		return ConfigConverterBoolean.INSTANCE;
	}

	public static <E extends Enum<E>> ConfigConverterSingle<E> ofEnum(Class<E> enumClass) {
		return new ConfigConverterEnum<>(enumClass);
	}

	public static ConfigConverterSingle<InetSocketAddress> ofInetSocketAddress() {
		return ConfigConverterInetSocketAddress.INSTANCE;
	}

	public static <T> ConfigConverter<List<T>> ofList(ConfigConverterSingle<T> elementConverter, CharSequence separators) {
		return new ConfigConverterList<>(elementConverter, separators);
	}

	public static <T> ConfigConverter<List<T>> ofList(ConfigConverterSingle<T> elementConverter) {
		return ofList(elementConverter, ",;");
	}

	public static ConfigConverterSingle<MemSize> ofMemSize() {
		return ConfigConverterMemSize.INSTANCE;
	}

	public static ConfigConverter<ServerSocketSettings> ofServerSocketSettings() {
		return ConfigConverterServerSocketSettings.INSTANCE;
	}

	public static ConfigConverter<SocketSettings> ofSocketSettings() {
		return ConfigConverterSocketSettings.INSTANCE;
	}

	public static ConfigConverter<DatagramSocketSettings> ofDatagramSocketSettings() {
		return ConfigConverterDatagramSocketSettings.INSTANCE;
	}

	public static ConfigConverterSingle<InetAddressRange> ofInetAddressRange() {
		return ConfigConverterInetAddressRange.INSTANCE;
	}
}

