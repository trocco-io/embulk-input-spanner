package org.embulk.input.spanner;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.embulk.config.ConfigException;
import org.embulk.input.spanner.SpannerInputPlugin.PluginTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerClient implements AutoCloseable {
  private static Logger logger = LoggerFactory.getLogger(SpannerClient.class);

  public static SpannerClient fromTask(PluginTask task) {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder();
    if (task.getHost().isPresent() || task.getPort().isPresent()) {
      StringBuilder sb = new StringBuilder();
      task.getHost().ifPresent(sb::append);
      sb.append(":");
      task.getPort().ifPresent(sb::append);
      if (task.getUseEmulator()) {
        builder.setEmulatorHost(sb.toString());
      } else {
        builder.setHost(sb.toString());
      }
    } else {
      if (task.getUseEmulator()) {
        builder.setEmulatorHost("localhost:9010");
      }
    }
    task.getJsonKeyFile()
        .ifPresent(
            f -> {
              logger.warn("'json_keyfile' option is deprecated, use 'credentials' option instead.");
              try {
                builder.setCredentials(
                    ServiceAccountCredentials.fromStream(
                        new FileInputStream(f.getPath().toAbsolutePath().toString())));
              } catch (IOException e) {
                throw new ConfigException("Cannot create a credentials.", e);
              }
            });
    task.getCredentials()
        .ifPresent(
            f -> {
              try {
                builder.setCredentials(
                    ServiceAccountCredentials.fromStream(
                        new FileInputStream(f.getPath().toAbsolutePath().toString())));
                builder.setProjectId(task.getProjectId());
              } catch (IOException e) {
                throw new ConfigException("Cannot create a credentials.", e);
              }
            });
    task.getOauthToken()
        .ifPresent(
            token ->
                builder.setCredentials(
                    OAuth2Credentials.newBuilder()
                        .setAccessToken(new AccessToken(token, null))
                        .build()));

    return new SpannerClient(
        builder.build().getService(),
        task.getProjectId(),
        task.getInstanceId(),
        task.getDatabaseId());
  }

  private final Spanner spanner;
  private final String projectId;
  private final String instanceId;
  private final String databaseId;

  public SpannerClient(Spanner spanner, String projectId, String instanceId, String databaseId) {
    this.spanner = spanner;
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.databaseId = databaseId;
  }

  public BatchClient newBatchClient() {
    return this.spanner.getBatchClient(
        DatabaseId.of(this.projectId, this.instanceId, this.databaseId));
  }

  public BatchReadOnlyTransaction newBatchReadOnlyTransaction() {
    return this.newBatchClient().batchReadOnlyTransaction(TimestampBound.strong());
  }

  public ParallelRead newParallelRead(PreparedStatement stmt) {
    return new ParallelRead(this, stmt);
  }

  @Override
  public void close() {
    if (!this.spanner.isClosed()) {
      this.spanner.close();
    }
  }

  public class ParallelRead implements AutoCloseable {
    private final AtomicBoolean isTxStarted = new AtomicBoolean(false);

    private final SpannerClient client;
    private final PreparedStatement stmt;
    private final BatchReadOnlyTransaction txn;
    private final List<Partition> partitions;
    private final AtomicInteger partitionIdx = new AtomicInteger(0);

    ParallelRead(SpannerClient client, PreparedStatement stmt) {
      this.client = client;
      this.stmt = stmt;

      this.txn = this.client.newBatchReadOnlyTransaction();
      isTxStarted.set(true);
      this.partitions =
          this.txn.partitionQuery(PartitionOptions.getDefaultInstance(), toStatement(this.stmt));

      logger.info("Load {} partitions.", this.partitions.size());
    }

    public ResultSet fetch() {
      int currentPartitionIdx = this.partitionIdx.getAndIncrement();
      if (currentPartitionIdx >= this.partitions.size()) {
        return null;
      }
      logger.info("Load the partition that index of {}", currentPartitionIdx);
      ResultSet rs = this.txn.execute(this.partitions.get(currentPartitionIdx));
      // NOTE: to avoid empty resultsets, we need to check the partition is empty or not.
      if (rs.next()) {
        return this.txn.execute(this.partitions.get(currentPartitionIdx));
      } else {
        return fetch();
      }
    }

    public java.sql.ResultSet fetchAsJdbcResultSet() {
      ResultSet rs = fetch();
      if (rs == null) {
        return null;
      }
      return toJdbcResultSet(rs);
    }

    @Override
    public void close() {
      this.txn.close();
    }

    private Statement toStatement(PreparedStatement stmt) {
      try {
        // https://github.com/googleapis/java-spanner-jdbc/blob/f9daa19/src/main/java/com/google/cloud/spanner/jdbc/JdbcPreparedStatement.java#L56-L64
        Method m = stmt.getClass().getDeclaredMethod("createStatement");
        m.setAccessible(true);
        return (Statement) m.invoke(stmt);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private java.sql.ResultSet toJdbcResultSet(ResultSet rs) {
      try {
        // https://github.com/googleapis/java-spanner-jdbc/blob/f9daa19/src/main/java/com/google/cloud/spanner/jdbc/JdbcResultSet.java#L57-L61
        Class<?> c = Class.forName("com.google.cloud.spanner.jdbc.JdbcResultSet");
        Method m = c.getDeclaredMethod("of", java.sql.Statement.class, ResultSet.class);
        m.setAccessible(true);
        return (java.sql.ResultSet) m.invoke(null, this.stmt, rs);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
