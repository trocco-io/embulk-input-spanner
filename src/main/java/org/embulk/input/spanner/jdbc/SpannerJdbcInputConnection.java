package org.embulk.input.spanner.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.JdbcSchema;

public class SpannerJdbcInputConnection extends JdbcInputConnection {

  private final boolean useEmulator;

  public SpannerJdbcInputConnection(Connection connection) throws SQLException {
    this(connection, false);
  }

  public SpannerJdbcInputConnection(Connection connection, boolean useEmulator)
      throws SQLException {
    super(connection, null);
    this.useEmulator = useEmulator;
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
}
