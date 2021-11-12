package org.embulk.input.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.embulk.config.ConfigSource;
import org.embulk.input.spanner.extension.embulk.EmbulkExtension;
import org.embulk.input.spanner.extension.embulk.EmbulkTester;
import org.embulk.input.spanner.extension.spanner.SpannerExtension;
import org.embulk.input.spanner.extension.spanner.SpannerExtension.AfterQuery;
import org.embulk.input.spanner.extension.spanner.SpannerExtension.BeforeQuery;
import org.embulk.spi.InputPlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestSpannerInputPlugin {

  static final String TEST_PROJECT = "test-project";
  static final String TEST_INSTANCE = "test-instance";
  static final String TEST_DATABASE = "test-database";

  @RegisterExtension
  static EmbulkExtension embulk =
      EmbulkExtension.builder()
          .registerPlugin(InputPlugin.class, "spanner", SpannerInputPlugin.class)
          .build();

  @RegisterExtension
  static SpannerExtension spanner =
      SpannerExtension.builder()
          .projectId(TEST_PROJECT)
          .instanceId(TEST_INSTANCE)
          .databaseId(TEST_DATABASE)
          .autoConfigEmulator(true)
          .build();

  @Test
  @BeforeQuery.List({
    @BeforeQuery("create table test_int (id int64) primary key(id)"),
    @BeforeQuery("insert into test_int(id) VALUES (1), (-1) ")
  })
  @AfterQuery("drop table test_int")
  public void testPlugin(EmbulkTester embulkTester) throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join(
                "\n",
                "type: spanner",
                "project_id: " + TEST_PROJECT,
                "instance_id: " + TEST_INSTANCE,
                "database_id: " + TEST_DATABASE,
                "table: test_int",
                "use_emulator: true",
                "socket_timeout: 0",
                ""));

    embulkTester.runInput(
        inConfig,
        (rows) -> {
          {
            Object[] row = rows.get(0);
            assertEquals(1L, row[0]);
          }
          {
            Object[] row = rows.get(1);
            assertEquals(-1L, row[0]);
          }
        });
  }
}
