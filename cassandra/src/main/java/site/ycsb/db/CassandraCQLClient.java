package site.ycsb.db;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import site.ycsb.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

public class CassandraCQLClient extends DB {

  private static CqlSession session;

  public static final String HOSTS_PROPERTY = "hosts";
  public static final String PORT_PROPERTY = "port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";
  public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String YCSB_KEY = "y_id";

  private static final ConcurrentMap<Set<String>, PreparedStatement> readStmts = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> scanStmts = new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> insertStmts = new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> updateStmts = new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static final AtomicReference<PreparedStatement> readAllStmt = new AtomicReference<PreparedStatement>();
  private static final AtomicReference<PreparedStatement> scanAllStmt = new AtomicReference<PreparedStatement>();
  private static final AtomicReference<PreparedStatement> deleteStmt = new AtomicReference<PreparedStatement>();

  @Override
  public void init() throws DBException {
    String[] hosts = getProperties().getProperty(HOSTS_PROPERTY).split(",");
    String port = getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT);

    List<InetSocketAddress> contactPoints = new ArrayList<>(hosts.length);
    for (String host : hosts) {
      contactPoints.add(new InetSocketAddress(host, Integer.parseInt(port)));
    }

    String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);

    session = CqlSession.builder()
        .addContactPoints(contactPoints)
        .withKeyspace(keyspace)
        .withLocalDatacenter("datacenter1")
        .build();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      PreparedStatement stmt = fields == null ?
          readAllStmt.get() :
          readStmts.get(fields);

      if (stmt == null) {
        Select select = fields == null ?
            selectFrom(table).all() :
            selectFrom(table).columns(fields);

        select = select.whereColumn(YCSB_KEY).isEqualTo(bindMarker()).limit(1);

        stmt = session.prepare(select.build());

        PreparedStatement prevStmt = fields == null ?
            readAllStmt.getAndSet(stmt) :
            readStmts.putIfAbsent(new HashSet<>(fields), stmt);

        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      Row row = session.execute(stmt.bind(key)).one();

      if (row == null) {
        return Status.ERROR;
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
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.ERROR;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = insertStmts.get(fields);

      if (stmt == null) {
        Map<String, Term> _values = new HashMap<>();

        _values.put(YCSB_KEY, bindMarker());
        for (String field : fields) {
          _values.put(field, bindMarker());
        }

        Insert insert = insertInto(table).values(_values);
        stmt = session.prepare(insert.build());

        PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet<>(fields), stmt);

        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      BoundStatement bindings = stmt.bind().setString(YCSB_KEY, key);
      for (String field : fields) {
        bindings = bindings.setString(field, values.get(field).toString());
      }

      session.execute(bindings);

      return Status.OK;
    } catch (Exception exception) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.ERROR;
  }
}
