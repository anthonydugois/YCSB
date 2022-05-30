package site.ycsb.db;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import site.ycsb.*;
import site.ycsb.tracing.TraceInfo;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class CassandraClient extends DB {
	private static CqlSession session;

	public static final String PARTITION_KEY = "id";

	public static final String OPTION_PROFILE = "profile";
	public static final String OPTION_PROFILE_DEFAULT = "default";

	public static final String OPTION_TRACING = "tracing";
	public static final boolean OPTION_TRACING_DEFAULT = false;

	private static final ConcurrentMap<Set<String>, PreparedStatement> readStmts = new ConcurrentHashMap<>();
	private static final ConcurrentMap<Set<String>, PreparedStatement> insertStmts = new ConcurrentHashMap<>();

	private static final AtomicReference<PreparedStatement> readAllStmt = new AtomicReference<>();

	private final Set<ExecutionInfo> executionInfos = new HashSet<>();

	@Override
	public void init() throws DBException {
		// Check if the cluster has already been initialized
		if (session != null) {
			return;
		}

		try {
			session = CqlSession.builder().build();
		} catch (Exception exception) {
			throw new DBException(exception);
		}
	}

	@Override
	public void cleanup() throws DBException {
		if (session == null) {
			return;
		}

		readStmts.clear();
		insertStmts.clear();

		readAllStmt.set(null);

		session.close();
		session = null;
	}

	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, Object> options,
					   Map<String, ByteIterator> result) {
		try {
			boolean readAllFields = fields == null;

			PreparedStatement stmt = readAllFields ?
					readAllStmt.get() :
					readStmts.get(fields);

			if (stmt == null) {
				Select select = readAllFields ?
						QueryBuilder.selectFrom(table).all() :
						QueryBuilder.selectFrom(table).columns(fields);

				select = select.whereColumn(PARTITION_KEY).isEqualTo(QueryBuilder.bindMarker()).limit(1);

				stmt = session.prepare(select.build());

				PreparedStatement prevStmt = readAllFields ?
						readAllStmt.getAndSet(stmt) :
						readStmts.putIfAbsent(new HashSet<>(fields), stmt);

				if (prevStmt != null) {
					stmt = prevStmt;
				}
			}

			// Bind the prepared statement
			BoundStatement bound = stmt.bind(key);

			// Enable some options
			String profile = (String) options.getOrDefault(OPTION_PROFILE, OPTION_PROFILE_DEFAULT);
			boolean tracing = (boolean) options.getOrDefault(OPTION_TRACING, OPTION_TRACING_DEFAULT);

			bound = bound.setExecutionProfileName(profile).setTracing(tracing);

			// Execute the statement
			ResultSet resultSet = session.execute(bound);

			if (tracing) {
				executionInfos.add(resultSet.getExecutionInfo());
			}

			Row row = resultSet.one();

			if (row == null) {
				return Status.NOT_FOUND;
			}

			for (ColumnDefinition definition : row.getColumnDefinitions()) {
				String field = definition.getName().toString();
				ByteBuffer value = row.getBytesUnsafe(definition.getName());

				result.put(field, value == null ? null : new ByteArrayByteIterator(value.array()));
			}

			return Status.OK;
		} catch (Exception exception) {
			return Status.ERROR;
		}
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		return Status.NOT_IMPLEMENTED;
	}

	@Override
	public Status update(String table, String key, Map<String, ByteIterator> values) {
		return Status.NOT_IMPLEMENTED;
	}

	@Override
	public Status insert(String table, String key, Map<String, ByteIterator> values, Map<String, Object> options) {
		try {
			Set<String> fields = values.keySet();
			PreparedStatement stmt = insertStmts.get(fields);

			if (stmt == null) {
				Map<String, Term> markers = new HashMap<>();

				markers.put(PARTITION_KEY, QueryBuilder.bindMarker());
				for (String field : fields) {
					markers.put(field, QueryBuilder.bindMarker());
				}

				Insert insert = QueryBuilder.insertInto(table).values(markers);

				stmt = session.prepare(insert.build());

				PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet<>(fields), stmt);

				if (prevStmt != null) {
					stmt = prevStmt;
				}
			}

			// Bind the prepared statement
			BoundStatement bound = stmt.bind();

			bound = bound.setString(PARTITION_KEY, key);

			for (String field : fields) {
				bound = bound.setString(field, values.get(field).toString());
			}

			// Enable some options
			String profile = (String) options.getOrDefault(OPTION_PROFILE, OPTION_PROFILE_DEFAULT);

			bound = bound.setExecutionProfileName(profile);

			// Execute the statement
			session.execute(bound);

			return Status.OK;
		} catch (Exception exception) {
			return Status.ERROR;
		}
	}

	@Override
	public Status delete(String table, String key) {
		return Status.NOT_IMPLEMENTED;
	}

	@Override
	public Collection<TraceInfo> traces() {
		Set<TraceInfo> traces = new HashSet<>();

		for (ExecutionInfo executionInfo : executionInfos) {
			TraceInfo traceInfo = new TraceInfo(executionInfo.getTracingId());

			traceInfo.setResponseSizeBytes(executionInfo.getResponseSizeInBytes());
			traceInfo.setCompressedResponseSizeBytes(executionInfo.getCompressedResponseSizeInBytes());

			// Warning; this is expensive (blocking request towards Cassandra)
			QueryTrace queryTrace = executionInfo.getQueryTrace();

			for (TraceEvent event : queryTrace.getEvents()) {
				TraceInfo.Event traceInfoEvent = new TraceInfo.Event(
						traceInfo,
						event.getActivity(),
						event.getSourceAddress(),
						event.getThreadName(),
						event.getTimestamp(),
						event.getSourceElapsedMicros()
				);

				traceInfo.registerEvent(traceInfoEvent);
			}

			traces.add(traceInfo);
		}

		return traces;
	}
}
