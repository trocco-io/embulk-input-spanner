package org.embulk.input.spanner.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import org.embulk.input.jdbc.JdbcInputConnection;

public class SpannerJdbcInputConnection extends JdbcInputConnection {
  public SpannerJdbcInputConnection(Connection connection) throws SQLException {
    super(connection, null);
  }
}
