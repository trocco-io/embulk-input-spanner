package org.embulk.input.spanner.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.JdbcLiteral;
import org.embulk.input.jdbc.JdbcSchema;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.spanner.SpannerClient;
import org.embulk.input.spanner.SpannerClient.ParallelRead;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerJdbcInputConnection extends JdbcInputConnection {
  private static final Logger logger = LoggerFactory.getLogger(SpannerJdbcInputConnection.class);

  private final SpannerClient client;
  private final boolean useEmulator;

  public SpannerJdbcInputConnection(Connection connection, SpannerClient client)
      throws SQLException {
    this(connection, client, false);
  }

  public SpannerJdbcInputConnection(
      Connection connection, SpannerClient client, boolean useEmulator) throws SQLException {
    super(connection, null);
    this.client = client;
    this.useEmulator = useEmulator;
  }

  @Override
  public void close() throws SQLException {
    super.close();
    client.close();
  }

  @Override
  public JdbcSchema getSchemaOfQuery(String query) throws SQLException {
    return useEmulator ? getSchemaOfQueryForSpannerEmulator(query) : super.getSchemaOfQuery(query);
  }

  protected JdbcSchema getSchemaOfQueryForSpannerEmulator(String query) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      // NOTE: We want to set `LIMIT 0` to the query, but it is difficult to do.
      //       So, setMaxRows(1) and setFetchSize(1) are used instead.
      stmt.setMaxRows(1);
      stmt.setFetchSize(1);
      try (ResultSet rs = stmt.executeQuery()) {
        return getSchemaOfResultMetadata(rs.getMetaData());
      }
    }
  }

  @Override
  protected BatchSelect newBatchSelect(
      PreparedQuery preparedQuery, List<ColumnGetter> getters, int fetchRows, int queryTimeout)
      throws SQLException {
    String query = preparedQuery.getQuery();
    if (useEmulator) {
      query = "@{spanner_emulator.disable_query_partitionability_check=true}\n" + query;
    }
    List<JdbcLiteral> params = preparedQuery.getParameters();

    PreparedStatement stmt = connection.prepareStatement(query);
    stmt.setFetchSize(fetchRows);
    stmt.setQueryTimeout(queryTimeout);
    logger.info("SQL: " + query);
    if (!params.isEmpty()) {
      logger.info("Parameters: {}", params);
      prepareParameters(stmt, getters, params);
    }
    return new ParallelReadBatchSelect(client.newParallelRead(stmt));
  }

  public class ParallelReadBatchSelect implements BatchSelect {
    private ParallelRead cursor;

    public ParallelReadBatchSelect(ParallelRead cursor) {
      this.cursor = cursor;
    }

    @Override
    public ResultSet fetch() throws SQLException {
      return cursor.fetchAsJdbcResultSet();
    }

    @Override
    public void close() throws SQLException {
      cursor.close();
    }
  }
}
