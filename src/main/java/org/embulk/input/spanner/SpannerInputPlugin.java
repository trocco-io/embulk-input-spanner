package org.embulk.input.spanner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Properties;
import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.input.spanner.jdbc.SpannerJdbcInputConnection;
import org.embulk.input.spanner.jdbc.getter.SpannerJdbcColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.units.LocalFile;

public class SpannerInputPlugin extends AbstractJdbcInputPlugin {
  public interface PluginTask extends AbstractJdbcInputPlugin.PluginTask {
    @Config("driver_path")
    @ConfigDefault("null")
    public Optional<String> getDriverPath();

    @Config("auth_method")
    @ConfigDefault("json_key")
    String getAuthMethod();

    @Config("json_keyfile")
    @ConfigDefault("null")
    Optional<LocalFile> getJsonKeyFile();

    @Config("host")
    @ConfigDefault("null")
    Optional<String> getHost();

    @Config("port")
    @ConfigDefault("null")
    Optional<Integer> getPort();

    @Config("project_id")
    String getProjectId();

    @Config("instance_id")
    String getInstanceId();

    @Config("database_id")
    String getDatabaseId();
  }

  @Override
  protected Class<? extends AbstractJdbcInputPlugin.PluginTask> getTaskClass() {
    return PluginTask.class;
  }

  @Override
  protected ColumnGetterFactory newColumnGetterFactory(
      PageBuilder pageBuilder, ZoneId dateTimeZone) {
    return new SpannerJdbcColumnGetterFactory(pageBuilder, dateTimeZone);
  }

  @Override
  protected JdbcInputConnection newConnection(AbstractJdbcInputPlugin.PluginTask task)
      throws SQLException {
    PluginTask t = (PluginTask) task;
    loadDriver("com.google.cloud.spanner.jdbc.JdbcDriver", t.getDriverPath());

    Properties props = new Properties();
    props.setProperty("readonly", "true");
    props.setProperty("lenient", "true");
    t.getJsonKeyFile()
        .ifPresent(
            file -> props.setProperty("credentials", file.getPath().toAbsolutePath().toString()));
    props.putAll(t.getOptions());

    props.setProperty(
        "connectTimeout", String.valueOf(t.getConnectTimeout() * 1000)); // milliseconds
    props.setProperty("socketTimeout", String.valueOf(t.getSocketTimeout() * 1000)); // milliseconds
    props.setProperty("autoConfigEmulator", "true");

    Connection con = DriverManager.getConnection(buildJdbcConnectionUrl(t), props);
    try {
      SpannerJdbcInputConnection c = new SpannerJdbcInputConnection(con);
      con = null;
      return c;
    } finally {
      if (con != null) {
        con.close();
      }
    }
  }

  private String buildJdbcConnectionUrl(PluginTask task) {
    // ref.
    // https://github.com/googleapis/java-spanner/blob/7de41bf/google-cloud-spanner/src/main/java/com/google/cloud/spanner/connection/ConnectionOptions.java#L320-L324
    StringBuilder sb = new StringBuilder();
    sb.append("jdbc:cloudspanner:");
    task.getHost().ifPresent(host -> sb.append("//").append(host));
    task.getPort().ifPresent(port -> sb.append(":").append(port));
    sb.append("/projects/");
    sb.append(task.getProjectId());
    sb.append("/instances/");
    sb.append(task.getInstanceId());
    sb.append("/databases/");
    sb.append(task.getDatabaseId());
    return sb.toString();
  }
}
