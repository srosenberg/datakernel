/**
 * This class is generated by jOOQ
 */
package io.datakernel.aggregation.sql;

import io.datakernel.aggregation.sql.tables.AggregationDbChunk;
import io.datakernel.aggregation.sql.tables.AggregationDbRevision;
import org.jooq.Table;
import org.jooq.impl.SchemaImpl;

import javax.annotation.Generated;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * This class is generated by jOOQ.
 */
@Generated(
	value = {
		"http://www.jooq.org",
		"jOOQ version:3.7.2"
	},
	comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class DefaultSchema extends SchemaImpl {

	private static final long serialVersionUID = -1668343438;

	/**
	 * The reference instance of <code></code>
	 */
	public static final DefaultSchema DEFAULT_SCHEMA = new DefaultSchema();

	/**
	 * No further instances allowed
	 */
	private DefaultSchema() {
		super("");
	}

	@Override
	public final List<Table<?>> getTables() {
		List result = new ArrayList();
		result.addAll(getTables0());
		return result;
	}

	private final List<Table<?>> getTables0() {
		return Arrays.<Table<?>>asList(
			AggregationDbChunk.AGGREGATION_DB_CHUNK,
			AggregationDbRevision.AGGREGATION_DB_REVISION);
	}
}
