package org.embulk.input.spanner.jdbc.getter;

import java.time.ZoneId;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.PageBuilder;

public class SpannerJdbcColumnGetterFactory extends ColumnGetterFactory {
  public SpannerJdbcColumnGetterFactory(PageBuilder to, ZoneId defaultTimeZone) {
    super(to, defaultTimeZone);
  }
}
