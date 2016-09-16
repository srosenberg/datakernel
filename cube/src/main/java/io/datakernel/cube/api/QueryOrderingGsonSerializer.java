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

package io.datakernel.cube.api;

import com.google.gson.*;
import io.datakernel.aggregation_db.api.QueryException;
import io.datakernel.cube.CubeQuery;

import java.lang.reflect.Type;

import static java.lang.String.format;

public final class QueryOrderingGsonSerializer implements JsonSerializer<CubeQuery.Ordering>,
		JsonDeserializer<CubeQuery.Ordering> {
	private static final String FIELD = "field";
	private static final String DIRECTION = "direction";
	private static final String ASC = "asc";
	private static final String DESC = "desc";

	private QueryOrderingGsonSerializer() {}

	public static QueryOrderingGsonSerializer create() {return new QueryOrderingGsonSerializer();}

	@Override
	public CubeQuery.Ordering deserialize(JsonElement json, Type type, JsonDeserializationContext ctx)
			throws JsonParseException {
		if (!(json instanceof JsonObject))
			throw new QueryException("Incorrect sort format. Should be represented as a JSON object");

		JsonObject orderingJson = (JsonObject) json;

		String orderingField = getOrThrow(FIELD, orderingJson);
		String direction = getOrThrow(DIRECTION, orderingJson);

		if (direction.equals(ASC))
			return CubeQuery.Ordering.asc(orderingField);

		if (direction.equals(DESC))
			return CubeQuery.Ordering.desc(orderingField);

		throw new QueryException(format("Unknown '%s' property value in sort object. Should be either '%s' or '%s'", DIRECTION, ASC, DESC));
	}

	private static String getOrThrow(String property, JsonObject json) throws QueryException {
		JsonElement fieldJson = json.get(property);
		if (fieldJson == null)
			throw new QueryException(format("Incorrect sort format. Does not contain property '%s'", property));
		return fieldJson.getAsString();
	}

	@Override
	public JsonElement serialize(CubeQuery.Ordering ordering, Type type, JsonSerializationContext ctx) {
		JsonObject orderingJson = new JsonObject();
		orderingJson.addProperty(FIELD, ordering.getPropertyName());
		orderingJson.addProperty(DIRECTION, ordering.isAsc() ? ASC : DESC);
		return orderingJson;
	}
}
