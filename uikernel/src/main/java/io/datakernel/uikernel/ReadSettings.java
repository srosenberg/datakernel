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

package io.datakernel.uikernel;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.*;

public final class ReadSettings {
	public enum SortOrder {
		ASC, DESC
	}

	private List<String> fields = new ArrayList<>();
	private int offset = 0;
	private int limit = Integer.MAX_VALUE;
	private Map<String, String> filters = new LinkedHashMap<>();
	private Map<String, SortOrder> sort = new LinkedHashMap<>();
	private Set<Integer> extra = new LinkedHashSet<>();

	public static ReadSettings parse(Gson gson, Map<String, String> parameters) {
		ReadSettings readSettings = new ReadSettings();

		String fields = parameters.get("fields");
		if (fields != null && !fields.isEmpty()) {
			readSettings.fields = gson.fromJson(fields, new TypeToken<List<String>>() {}.getType());
		}

		String offset = parameters.get("offset");
		if (offset != null && !offset.isEmpty()) {
			readSettings.offset = Integer.valueOf(offset);
		}

		String limit = parameters.get("limit");
		if (limit != null && !limit.isEmpty()) {
			readSettings.limit = Integer.valueOf(limit);
		}

		String filters = parameters.get("filters");
		if (filters != null && !filters.isEmpty()) {
			readSettings.filters = gson.fromJson(filters, new TypeToken<Map<String, String>>() {}.getType());
		}

		String sort = parameters.get("sort");
		if (sort != null && !sort.isEmpty()) {
			List<List<String>> list = gson.fromJson(sort, new TypeToken<List<List<String>>>() {}.getType());
			for (List<String> el : list) {
				SortOrder order = el.get(1).equals("asc") ? SortOrder.ASC : SortOrder.DESC;
				readSettings.sort.put(el.get(0), order);
			}
		}

		String extra = parameters.get("extra");
		if (extra != null && !extra.isEmpty()) {
			readSettings.extra = gson.fromJson(extra, new TypeToken<Set<String>>() {}.getType());
		}

		return readSettings;
	}

	public List<String> getFields() {
		return fields;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public Map<String, String> getFilters() {
		return filters;
	}

	public void setFilters(Map<String, String> filters) {
		this.filters = filters;
	}

	public Map<String, SortOrder> getSort() {
		return sort;
	}

	public void setSort(Map<String, SortOrder> sort) {
		this.sort = sort;
	}

	public Set<Integer> getExtra() {
		return extra;
	}

	public void setExtra(Set<Integer> extra) {
		this.extra = extra;
	}

	@Override
	public String toString() {
		return "ReadSettings{" +
				"fields=" + fields +
				", offset=" + offset +
				", limit=" + limit +
				", filters=" + filters +
				", sort=" + sort +
				", extra=" + extra +
				'}';
	}
}