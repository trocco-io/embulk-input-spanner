package org.embulk.input.spanner;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Date;
import java.time.Instant;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.embulk.config.ConfigSource;
import org.embulk.input.spanner.extension.embulk.EmbulkExtension;
import org.embulk.input.spanner.extension.embulk.EmbulkTester;
import org.embulk.input.spanner.extension.spanner.SpannerExtension;
import org.embulk.input.spanner.extension.spanner.SpannerExtension.Query;
import org.embulk.input.spanner.extension.spanner.SpannerExtension.SetupQueries;
import org.embulk.input.spanner.extension.spanner.SpannerExtension.TableName;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.time.Timestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

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
    @Query("create table %s (v int64) primary key(v)"),
    @Query("insert into %s (v) VALUES (1), (-1), (9223372036854775807), (-9223372036854775807)")
  })
  public void testInt64(EmbulkTester embulkTester, @TableName String tableName) throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join("\n", minimumConfigYaml, "table: " + tableName, "order_by: v asc"));
    embulkTester.runInput(
        inConfig,
        (rows) -> {
          long[] expected = LongStream.of(-Long.MAX_VALUE, -1L, 1L, Long.MAX_VALUE).toArray();
          assertAll(
              IntStream.range(0, expected.length)
                  .mapToObj(i -> () -> assertEquals(expected[i], rows.get(i)[0])));
        });
  }

  @Test
  @SetupQueries({
    @Query("create table %s (v bool) primary key(v)"),
    @Query("insert into %s (v) VALUES (true), (false), (null)")
  })
  public void testBool(EmbulkTester embulkTester, @TableName String tableName) throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join("\n", minimumConfigYaml, "table: " + tableName, "order_by: v asc"));
    embulkTester.runInput(
        inConfig,
        (rows) -> {
          Boolean[] expected = {null, false, true};
          assertAll(
              IntStream.range(0, expected.length)
                  .mapToObj(i -> () -> assertEquals(expected[i], rows.get(i)[0])));
        });
  }

  @Test
  @SetupQueries({
    @Query("create table %s (v bytes(max)) primary key(v)"),
    @Query("insert into %s (v) VALUES (FROM_BASE64('/+A='))")
  })
  public void testByteWithoutColumnOptions(EmbulkTester embulkTester, @TableName String tableName)
      throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join("\n", minimumConfigYaml, "table: " + tableName, "order_by: v asc"));

    assertThrows(
        UnsupportedOperationException.class, () -> embulkTester.runInput(inConfig, (rows) -> {}));
  }

  @Test
  @SetupQueries({
    @Query("create table %s (v bytes(max)) primary key(v)"),
    @Query("insert into %s (v) VALUES (FROM_BASE64('/+A='))")
  })
  public void testByteWithColumnOptions(EmbulkTester embulkTester, @TableName String tableName)
      throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join(
                "\n",
                minimumConfigYaml,
                "table: " + tableName,
                "order_by: v asc",
                "column_options:",
                "  v: {value_type: string}",
                ""));

    assertDoesNotThrow(
        () ->
            embulkTester.runInput(
                inConfig,
                (rows) -> {
                  assertEquals("/+A=", rows.get(0)[0]);
                }));
  }

  @Test
  @SetupQueries({
    @Query("create table %s (v bytes(max)) primary key(v)"),
    @Query("insert into %s (v) VALUES (FROM_BASE64('/+A='))")
  })
  public void testByteWithSelectToBase64(EmbulkTester embulkTester, @TableName String tableName)
      throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join(
                "\n",
                minimumConfigYaml,
                "table: " + tableName,
                "order_by: v asc",
                "select: TO_BASE64(v)",
                ""));

    assertDoesNotThrow(
        () ->
            embulkTester.runInput(
                inConfig,
                (rows) -> {
                  assertEquals("/+A=", rows.get(0)[0]);
                }));
  }

  @Test
  @SetupQueries({
    @Query("create table %s (v DATE) primary key(v)"),
    @Query("insert into %s (v) VALUES ('2021-11-16')")
  })
  public void testDate(EmbulkTester embulkTester, @TableName String tableName) throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join("\n", minimumConfigYaml, "table: " + tableName, "order_by: v asc", ""));

    embulkTester.runInput(
        inConfig,
        (rows) -> {
          assertAll(
              () -> assertEquals(Timestamp.class, rows.get(0)[0].getClass()),
              () ->
                  assertEquals(
                      Timestamp.ofInstant(
                          // Instant.ofEpochMilli(Date.valueOf("2021-11-16").getTime()) depends on
                          // the System Timezone.
                          // TZ=Asia/Tokyo -> 2021-11-15T15:00:00Z
                          // TZ=UTC -> 2021-11-16T00:00:00Z
                          Instant.ofEpochMilli(Date.valueOf("2021-11-16").getTime())),
                      rows.get(0)[0]));
        });
  }

  @Test
  @SetupQueries({
    @Query("create table %s (i int64, v JSON) primary key(i)"),
    @Query("insert into %s (i, v) VALUES (1, JSON '{\"a\":5,\"b\":\"c\",\"d\":{\"e\":\"f\"}}')")
  })
  public void testJson(EmbulkTester embulkTester, @TableName String tableName) throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join(
                "\n",
                minimumConfigYaml,
                "table: " + tableName,
                "select: v",
                "order_by: i asc",
                ""));

    embulkTester.runInput(
        inConfig,
        (rows) -> {
          assertAll(
              () -> assertInstanceOf(Value.class, rows.get(0)[0]),
              () ->
                  assertEquals(
                      ValueFactory.newMapBuilder()
                          .put(ValueFactory.newString("a"), ValueFactory.newInteger(5))
                          .put(ValueFactory.newString("b"), ValueFactory.newString("c"))
                          .put(
                              ValueFactory.newString("d"),
                              ValueFactory.newMapBuilder()
                                  .put(ValueFactory.newString("e"), ValueFactory.newString("f"))
                                  .build())
                          .build(),
                      rows.get(0)[0]));
        });
  }

  @Test
  @SetupQueries({
    @Query("create table %s (i int64, v numeric) primary key(i)"),
    @Query(
        "insert into %s (i, v) VALUES (1, 99999999999999999999999999999.999999999), (2, 1), (3, 1.1)")
  })
  public void testNumericDefault(EmbulkTester embulkTester, @TableName String tableName)
      throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join(
                "\n",
                minimumConfigYaml,
                "table: " + tableName,
                "select: v",
                "order_by: v asc",
                ""));

    embulkTester.runInput(
        inConfig,
        (rows) -> {
          double[] expected = {
            1.0d,
            1.1d,
            // NOTE: Embulk cannot handle 99999999999999999999999999999.999999999 accurately as a
            // double value.
            //       Use String instead if you need accuracy.
            100000000000000000000000000000.0d
          };
          assertAll(
              Stream.concat(
                  Stream.of(() -> assertEquals(Double.class, rows.get(0)[0].getClass())),
                  IntStream.range(0, expected.length)
                      .mapToObj(i -> () -> assertEquals(expected[i], rows.get(i)[0]))));
        });
  }

  @Test
  @SetupQueries({
    @Query("create table %s (i int64, v numeric) primary key(i)"),
    @Query(
        "insert into %s (i, v) VALUES (1, 99999999999999999999999999999.999999999), (2, 1), (3, 1.1)")
  })
  public void testNumericString(EmbulkTester embulkTester, @TableName String tableName)
      throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join(
                "\n",
                minimumConfigYaml,
                "table: " + tableName,
                "select: v",
                "order_by: v asc",
                "column_options:",
                "  v: {value_type: string}",
                ""));

    embulkTester.runInput(
        inConfig,
        (rows) -> {
          String[] expected = {
            "1",
            "1.1",
            // NOTE: Embulk cannot handle 99999999999999999999999999999.999999999 accurately as a
            // double value.
            //       Use String instead if you need accuracy.
            "99999999999999999999999999999.999999999"
          };
          assertAll(
              Stream.concat(
                  Stream.of(() -> assertEquals(String.class, rows.get(0)[0].getClass())),
                  IntStream.range(0, expected.length)
                      .mapToObj(i -> () -> assertEquals(expected[i], rows.get(i)[0]))));
        });
  }

  @Test
  @SetupQueries({
    @Query("create table %s (i int64, v FLOAT64) primary key(i)"),
    @Query("insert into %s (i, v) VALUES (1, 1.7976931348623157E308), (2, 1), (3, -1.1)")
  })
  public void testFloat(EmbulkTester embulkTester, @TableName String tableName) throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join(
                "\n",
                minimumConfigYaml,
                "table: " + tableName,
                "select: v",
                "order_by: v asc",
                ""));

    embulkTester.runInput(
        inConfig,
        (rows) -> {
          double[] expected = {-1.1, 1.0, Double.MAX_VALUE};
          assertAll(
              Stream.concat(
                  Stream.of(() -> assertEquals(Double.class, rows.get(0)[0].getClass())),
                  IntStream.range(0, expected.length)
                      .mapToObj(i -> () -> assertEquals(expected[i], rows.get(i)[0]))));
        });
  }

  @Test
  @SetupQueries({
    @Query("create table %s (i int64, v timestamp) primary key(i)"),
    @Query("insert into %s (i, v) VALUES (1, '2021-11-16T00:00:00Z')")
  })
  public void testTimestampDefault(EmbulkTester embulkTester, @TableName String tableName)
      throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join(
                "\n",
                minimumConfigYaml,
                "table: " + tableName,
                "select: v",
                "order_by: v asc",
                ""));

    embulkTester.runInput(
        inConfig,
        (rows) -> {
          assertAll(
              () -> assertEquals(Timestamp.class, rows.get(0)[0].getClass()),
              () ->
                  assertEquals(
                      Timestamp.ofInstant(Instant.parse("2021-11-16T00:00:00Z")), rows.get(0)[0]));
        });
  }

  @Test
  @SetupQueries({
    @Query("create table %s (i int64, v timestamp) primary key(i)"),
    @Query("insert into %s (i, v) VALUES (1, '2021-11-16T00:00:00Z')")
  })
  public void testTimestampJST(EmbulkTester embulkTester, @TableName String tableName)
      throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join(
                "\n",
                minimumConfigYaml,
                "table: " + tableName,
                "select: v",
                "order_by: v asc",
                "default_timezone: Asia/Tokyo",
                ""));

    embulkTester.runInput(
        inConfig,
        (rows) -> {
          assertAll(
              () -> assertEquals(Timestamp.class, rows.get(0)[0].getClass()),
              () ->
                  assertEquals(
                      Timestamp.ofInstant(Instant.parse("2021-11-16T00:00:00Z")), rows.get(0)[0]));
        });
  }

  @Test
  @SetupQueries({
    @Query("create table %s (i int64, v array<int64>) primary key(i)"),
    @Query("insert into %s (i, v) VALUES (1, [1,2,3])")
  })
  public void testArray(EmbulkTester embulkTester, @TableName String tableName) throws Throwable {
    ConfigSource inConfig =
        embulkTester.loadFromYamlString(
            String.join(
                "\n",
                minimumConfigYaml,
                "table: " + tableName,
                "select: v",
                "column_options:",
                "  v: {value_type: string}",
                ""));

    embulkTester.runInput(
        inConfig,
        (rows) -> {
          Value[] expected = {
            ValueFactory.newArray(
                ValueFactory.newInteger(1L),
                ValueFactory.newInteger(2L),
                ValueFactory.newInteger(3L)),
          };

          assertAll(
              Stream.concat(
                  Stream.of(() -> assertInstanceOf(Value.class, rows.get(0)[0])),
                  IntStream.range(0, expected.length)
                      .mapToObj(i -> () -> assertEquals(expected[i], rows.get(i)[0]))));
        });
  }
}
