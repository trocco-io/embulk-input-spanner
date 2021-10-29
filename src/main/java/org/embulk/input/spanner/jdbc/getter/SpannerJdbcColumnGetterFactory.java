package org.embulk.input.spanner.jdbc.getter;

import java.sql.Types;
import java.time.ZoneId;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin.PluginTask;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.jdbc.getter.TimestampWithTimeZoneIncrementalHandler;
import org.embulk.spi.PageBuilder;

public class SpannerJdbcColumnGetterFactory extends ColumnGetterFactory {
  public SpannerJdbcColumnGetterFactory(PageBuilder to, ZoneId defaultTimeZone) {
    super(to, defaultTimeZone);
  }

  @Override
  public ColumnGetter newColumnGetter(
      JdbcInputConnection con, PluginTask task, JdbcColumn column, JdbcColumnOption option) {
    switch (column.getSqlType()) {
      case Types.ARRAY:
        return new ArrayColumnGetter(to, getToType(option));
      case Types.TIMESTAMP:
        ColumnGetter getter = super.newColumnGetter(con, task, column, option);
        return new TimestampWithTimeZoneIncrementalHandler(getter);
      default:
        return super.newColumnGetter(con, task, column, option);
    }
  }

  @Override
  protected String sqlTypeToValueType(JdbcColumn column, int sqlType) {
    // ref. https://cloud.google.com/spanner/docs/data-types
    switch (column.getTypeName()) {
      case "array":
        return "json";
      default:
        return super.sqlTypeToValueType(column, sqlType);
    }
  }
}
