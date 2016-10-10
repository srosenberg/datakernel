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

import io.datakernel.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Arrays.asList;

/**
 * Represents properties in tree form. The tree path represents a key of
 * some property. The value of property is stored in the last node of the tree
 * path.
 *
 * Provides convenient static factory methods for instantiating, persisting and
 * modifying a {@code Config}. The {@code Config} can be instantiated with
 * given {@code Properties} object, file or path to file with properties.
 *
 * Also there are methods that allow to work with {@code Config} tree.
 */
@SuppressWarnings("unchecked")
public final class Config {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static final String ROOT = "";
    public static final char SEPARATOR = '.';

    private final Map<String, Config> children = new LinkedHashMap<>();

    private final Config parent;
    private String value;
    private String defaultValue;

    private boolean accessed;
    private boolean modified;

    private Config() {
        this.parent = null;
    }

    public static Config create() {
        return new Config();
    }

    private Config(Config parent) {
        this.parent = parent;
    }

    /**
     * Creates a {@link Config} with properties in specified object.
     *
     * @param properties config properties
     * @return config with given properties
     */
    public static Config ofProperties(Properties properties) {
        Config root = new Config(null);
        for (String key : properties.stringPropertyNames()) {
            Config entry = root.ensureChild(key);
            entry.value = properties.getProperty(key);
        }
        return root;
    }

    /**
     * Creates a {@link Config} with properties contained in the file.
     *
     * @param propertiesFile file containing properties
     * @return config with properties from file
     */
    public static Config ofProperties(File propertiesFile) {
        final Properties properties = new Properties();
        try (InputStream fis = new FileInputStream(propertiesFile)) {
            properties.load(fis);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ofProperties(properties);
    }

    /**
     * Returns a {@link Config} with properties from a file on the specified path
     *
     * @param propertiesFile path to properties file
     * @return config with properties from file
     */
    public static Config ofProperties(String propertiesFile) {
        return ofProperties(propertiesFile, false);
    }

    public static Config ofProperties(String propertiesFile, boolean optional) {
        final File file = new File(propertiesFile);
        return ofProperties(file, optional);
    }

    /**
     * Creates a {@link Config} from a given file if file exists, otherwise
     * instantiates a blank object.
     *
     * @param file file containing properties
     * @param optional default object creation flag
     * @return config
     */
    public static Config ofProperties(File file, boolean optional) {
        if (!file.exists() && optional) {
            return new Config(null);
        }
        return ofProperties(file);
    }

    /**
     * Creates a single {@link Config}, consisting of specified configs.
     *
     * @param configs set of configs
     * @return single {@link Config}
     */
    public static Config union(Config... configs) {
        return doUnion(null, asList(configs));
    }

    /**
     * Creates a single {@link Config} object, consisting of configs, contained
     * in collection.
     *
     * @param configs collection of configs
     * @return single {@code Config} object
     */
    public static Config union(Collection<Config> configs) {
        if (configs.size() == 1)
            return configs.iterator().next();

        return doUnion(null, configs);
    }

    private static Config doUnion(Config parent, Collection<Config> configs) {
        Config result = new Config(parent);
        Map<String, List<Config>> childrenList = new LinkedHashMap<>();

        for (Config config : configs) {
            if (config.value != null) {
                if (result.value != null) {
                    throw new IllegalStateException("Multiple values for " + config.getKey());
                }
                result.value = config.value;
            }
            for (String key : config.children.keySet()) {
                Config child = config.children.get(key);
                List<Config> list = childrenList.get(key);
                if (list == null) {
                    list = new ArrayList<>();
                    childrenList.put(key, list);
                }
                list.add(child);
            }
        }

        for (String key : childrenList.keySet()) {
            List<Config> childConfigs = childrenList.get(key);
            Config joined = doUnion(result, childConfigs);
            result.children.put(key, joined);
        }

        return result;
    }

    private static String propertiesFileEncode(String string, boolean escapeKey) {
        StringBuilder sb = new StringBuilder(string.length() * 2);

        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            if ((c > 61) && (c < 127)) {
                if (c == '\\') {
                    sb.append('\\');
                    sb.append('\\');
                    continue;
                }
                sb.append(c);
                continue;
            }
            switch (c) {
                case ' ':
                    if (i == 0 || escapeKey)
                        sb.append('\\');
                    sb.append(' ');
                    break;
                case '\t':
                    sb.append('\\');
                    sb.append('t');
                    break;
                case '\n':
                    sb.append('\\');
                    sb.append('n');
                    break;
                case '\r':
                    sb.append('\\');
                    sb.append('r');
                    break;
                case '\f':
                    sb.append('\\');
                    sb.append('f');
                    break;
                case '=':
                case ':':
                case '#':
                case '!':
                    sb.append('\\');
                    sb.append(c);
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    synchronized private boolean saveToPropertiesFile(String prefix, Writer writer) throws IOException {
        boolean rootLevel = prefix.isEmpty();
        StringBuilder sb = new StringBuilder();
        if (value != null || defaultValue != null) {
            if (!accessed) {
                assert defaultValue == null;
                sb.append("# Unused: ");
                sb.append(propertiesFileEncode(prefix, true));
                sb.append(" = ");
                sb.append(propertiesFileEncode(value, false));
            } else {
                if (value != null && !value.equals(defaultValue)) {
                    sb.append(propertiesFileEncode(prefix, true));
                    sb.append(" = ");
                    sb.append(propertiesFileEncode(value, false));
                } else { // defaultValue != null
                    sb.append("#");
                    sb.append(propertiesFileEncode(prefix, true));
                    sb.append(" = ");
                    if (defaultValue != null) {
                        sb.append(propertiesFileEncode(defaultValue, false));
                    }
                }
            }
        }
        boolean saved = false;
        String line = sb.toString();
        if (!line.isEmpty()) {
            writer.write(line + '\n');
            saved = true;
        }
        for (String key : children.keySet()) {
            Config child = children.get(key);
            boolean savedByChild = child.saveToPropertiesFile(rootLevel ? key : (prefix + "." + key), writer);
            if (rootLevel && savedByChild) {
                writer.write('\n');
            }
            saved |= savedByChild;
        }
        return saved;
    }

    /**
     * Saves config to the specified file using {@code UTF-8} charset
     *
     * @param file the file to be opened for saving
     * @throws IOException if an I/O error occurs
     */
    public void saveToPropertiesFile(File file) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))) {
            saveToPropertiesFile("", writer);
        }
    }

    /**
     * Traverses through config tree nodes sequence according to the path and
     * creates non-existing nodes.
     * Returns the {@link Config} that corresponds to given path.
     *
     * @param path path to a node in the {@code Config} tree
     * @return config node by specified path
     *         current config node if specified path is empty
     */
    public Config ensureChild(String path) {
        if (path.isEmpty())
            return this;
        Config result = this;
        for (String key : StringUtils.splitToList(SEPARATOR, path)) {
            checkState(!key.isEmpty(), "Child path must not be empty: %s", path);
            Config child = result.children.get(key);
            if (child == null) {
                child = new Config(result);
                result.children.put(key, child);
            }
            result = child;
        }
        return result;
    }

    /**
     * Returns a {@code Config} node which corresponds to the specified path.
     *
     * @param path path to required {@code Config} node
     * @return config node from tree
     */
    synchronized public Config getChild(String path) {
        final Config config = ensureChild(path);
        config.accessed = true;
        return config;
    }

    synchronized public boolean isAccessed() {
        return accessed;
    }

    synchronized public boolean isModified() {
        return modified;
    }

    /**
     * Returns read-only map of current config's children.
     *
     * @return unmodifiable map of config's children
     */
    synchronized public Map<String, Config> getChildren() {
        return Collections.unmodifiableMap(children);
    }

    /**
     * Returns a sequence of config's path components separated by "."
     *
     * @return key of config property
     */
    synchronized public String getKey() {
        if (parent == null)
            return "";
        for (String childKey : parent.children.keySet()) {
            Config child = parent.children.get(childKey);
            if (child == this) {
                String childRootKey = parent.getKey();
                return childRootKey.isEmpty() ? childKey : childRootKey + "." + childKey;
            }
        }
        throw new IllegalStateException();
    }

    /**
     * Returns a value of a config and marks this node as accessed.
     *
     * @return value of a config
     */
    synchronized public String get() {
        accessed = true;
        return value;
    }

    /**
     * Assigns a default value of config. Returns either value or default
     * value of a config and marks it as accessed.
     *
     * @param defaultValue default value of a property
     * @return value if it is not null
     *         default value otherwise
     */
    synchronized public String get(String defaultValue) {
        if (this.defaultValue != null) {
            if (!this.defaultValue.equals(defaultValue)) {
                throw new IllegalArgumentException("Key '" + getKey() + "': Previous default value '" + this.defaultValue + "' differs from new default value '"
                        + defaultValue + "'");
            }
        } else {
            this.defaultValue = defaultValue;
        }
        String result = get();
        if (result == null) {
            logger.info("Using default value for '{}'", getKey());
            result = defaultValue;
        }
        return result;
    }

    /**
     * Sets property's value and marks it as modified.
     *
     * @param value value of a property
     */
    synchronized public void set(String value) {
        modified = true;
        this.value = value;
    }

    /**
     * Sets property's value on given path and marks it as modified.
     * Creates non-existent nodes on the specified tree path.
     *
     * @param path path to property's value in config tree
     * @param value value of config property
     */
    synchronized public void set(String path, String value) {
        ensureChild(path).set(value);
    }

    synchronized public <T> T get(ConfigConverter<T> converter, String path) {
        checkNotNull(converter);
        return converter.get(ensureChild(path));
    }

    synchronized public <T> T get(ConfigConverter<T> converter, String path, T defaultValue) {
        checkNotNull(converter);
        checkNotNull(defaultValue);
        return converter.get(ensureChild(path), defaultValue);
    }

    /**
     * Checks if there is a value assigned to a node and marks node found on
     * given path as accessed. Creates non-existent nodes of the specified tree path.
     *
     * @param path path to required {@code Config} node
     * @return true if value is not null
     *         false otherwise
     */
    synchronized public boolean hasValue(String path) {
        Config child = ensureChild(path);
        child.accessed = true;
        return child.value != null;
    }

    /**
     *
     *
     * @param path
     * @return
     */
    synchronized public boolean hasSection(String path) {
        Config child = ensureChild(path);
        child.accessed = true;
        return child.children.size() > 0 && child.value == null;
    }

    @Override
    public String toString() {
        return getKey();
    }

    @Override
    public synchronized boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Config config = (Config) o;
        return Objects.equals(this.children, config.children) &&
                Objects.equals(this.value, config.value);
    }

}
