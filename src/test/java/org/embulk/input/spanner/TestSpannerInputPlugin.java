package org.embulk.input.spanner;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.embulk.config.ConfigSource;
import org.embulk.input.spanner.extension.embulk.EmbulkExtension;
import org.embulk.input.spanner.extension.embulk.EmbulkTester;
import org.embulk.input.spanner.extension.spanner.SpannerExtension;
import org.embulk.input.spanner.extension.spanner.SpannerExtension.Query;
import org.embulk.input.spanner.extension.spanner.SpannerExtension.SetupQueries;
import org.embulk.input.spanner.extension.spanner.SpannerExtension.TableName;
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

  static final String minimumConfigYaml =
      String.join(
          "\n",
          "type: spanner",
          "project_id: " + TEST_PROJECT,
          "instance_id: " + TEST_INSTANCE,
          "database_id: " + TEST_DATABASE,
          "use_emulator: true",
          "socket_timeout: 0",
          "");

  @Test
  @SetupQueries({
    @Query("create table %s (id int64) primary key(id)"),
    @Query("insert into %s (id) VALUES (1), (-1), (9223372036854775807)")
  })
  public void testInt64(EmbulkTester embulkTester, @TableName String tableName) throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(minimumConfigYaml + "table: " + tableName);
    embulkTester.runInput(
        inConfig,
        (rows) -> {
          assertAll(
              () -> assertEquals(1L, rows.get(0)[0]),
              () -> assertEquals(-1L, rows.get(1)[0]),
              () -> assertEquals(Long.MAX_VALUE, rows.get(2)[0]));
        });
  }
}
