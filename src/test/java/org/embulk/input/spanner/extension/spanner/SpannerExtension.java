package org.embulk.input.spanner.extension.spanner;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class SpannerExtension
    implements BeforeEachCallback,
        AfterEachCallback,
        BeforeAllCallback,
        AfterAllCallback,
        ParameterResolver {

  @Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface SetupQueries {
    Query[] value() default {};

    Query[] teardown() default {};

    boolean dropTableAfterTest() default true;
  }

  @Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Query {
    String value();

    boolean injectTableName() default true;
  }

  @Target({ElementType.ANNOTATION_TYPE, ElementType.PARAMETER})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface TableName {}

  public static class Builder {
    private String projectId = "test-project";
    private String instanceId = "test-instance";
    private String databaseId = "test-database";
    private String credentials = null;
    private Boolean autocommit = null;
    private Boolean readonly = false;
    private Boolean autoConfigEmulator = true;
    private Boolean usePlainText = true;
    private String optimizerVersion = null;
    private Integer numChannels = null;
    private String oauthToken = null;
    private Boolean lenient = true;

    Builder() {}

    public Builder projectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public Builder instanceId(String instanceId) {
      this.instanceId = instanceId;
      return this;
    }

    public Builder databaseId(String databaseId) {
      this.databaseId = databaseId;
      return this;
    }

    public Builder credentials(String credentials) {
      this.credentials = credentials;
      return this;
    }

    public Builder autocommit(Boolean autocommit) {
      this.autocommit = autocommit;
      return this;
    }

    public Builder readonly(Boolean readonly) {
      this.readonly = readonly;
      return this;
    }

    public Builder autoConfigEmulator(Boolean autoConfigEmulator) {
      this.autoConfigEmulator = autoConfigEmulator;
      return this;
    }

    public Builder usePlainText(Boolean usePlainText) {
      this.usePlainText = usePlainText;
      return this;
    }

    public Builder optimizerVersion(String optimizerVersion) {
      this.optimizerVersion = optimizerVersion;
      return this;
    }

    public Builder numChannels(Integer numChannels) {
      this.numChannels = numChannels;
      return this;
    }

    public Builder lenient(Boolean lenient) {
      this.lenient = lenient;
      return this;
    }

    public SpannerExtension build() {
      StringBuilder sb = new StringBuilder();
      sb.append("jdbc:cloudspanner:");
      sb.append("/projects/");
      sb.append(projectId);
      sb.append("/instances/");
      sb.append(instanceId);
      sb.append("/databases/");
      sb.append(databaseId);
      return new SpannerExtension(this);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private static final String SPANNER_EXTENSION_NAMESPACE_STRING = "spannerExtension";
  private static final String TABLE_NAME = "tableName";

  private final String connectionUrl;
  private final Properties connectionProperties;
  private Connection connection = null;

  SpannerExtension(Builder builder) {
    this.connectionUrl = connectionUrl(builder);
    this.connectionProperties = connectionProperties(builder);
  }

  // This constructor is invoked by JUnit Jupiter via reflection or ServiceLoader
  @SuppressWarnings("unused")
  public SpannerExtension() {
    this(builder());
  }

  private static String connectionUrl(Builder builder) {
    StringBuilder sb = new StringBuilder();
    sb.append("jdbc:cloudspanner:");
    sb.append("/projects/");
    sb.append(builder.projectId);
    sb.append("/instances/");
    sb.append(builder.instanceId);
    sb.append("/databases/");
    sb.append(builder.databaseId);
    return sb.toString();
  }

  private static Properties connectionProperties(Builder builder) {
    Properties properties = new Properties();

    if (builder.credentials != null) {
      properties.setProperty("credentials", builder.credentials);
    }
    if (builder.autocommit != null) {
      properties.setProperty("autocommit", builder.autocommit.toString());
    }
    if (builder.readonly != null) {
      properties.setProperty("readonly", builder.readonly.toString());
    }
    if (builder.autoConfigEmulator != null) {
      properties.setProperty("autoConfigEmulator", builder.autoConfigEmulator.toString());
    }
    if (builder.usePlainText != null) {
      properties.setProperty("usePlainText", builder.usePlainText.toString());
    }
    if (builder.optimizerVersion != null) {
      properties.setProperty("optimizerVersion", builder.optimizerVersion);
    }
    if (builder.numChannels != null) {
      properties.setProperty("numChannels", builder.numChannels.toString());
    }
    if (builder.oauthToken != null) {
      properties.setProperty("oauthToken", builder.oauthToken);
    }
    if (builder.lenient != null) {
      properties.setProperty("lenient", builder.lenient.toString());
    }
    return properties;
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    connect();
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    close();
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    if (connection == null || connection.isClosed()) {
      reconnect();
    }

    final String tableName = String.format("a%s", UUID.randomUUID().toString().replaceAll("-", ""));
    setStoredTableName(context, tableName);

    final Method testMethod = context.getRequiredTestMethod();
    if (testMethod.isAnnotationPresent(SetupQueries.class)) {
      SetupQueries setupQueries = testMethod.getAnnotation(SetupQueries.class);
      executeBeforeAfterQueries(setupQueries.value(), tableName);
    }
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    if (connection == null || connection.isClosed()) {
      reconnect();
    }

    String tableName = removeStoredTableName(context);

    Method testMethod = context.getRequiredTestMethod();
    if (testMethod.isAnnotationPresent(SetupQueries.class)) {
      SetupQueries setupQueries = testMethod.getAnnotation(SetupQueries.class);
      executeBeforeAfterQueries(setupQueries.teardown(), tableName);
      if (setupQueries.dropTableAfterTest()) {
        dropTableIfExists(tableName);
      }
    }
  }

  private void executeBeforeAfterQueries(Query[] queries, String tableName)
      throws IllegalArgumentException, SQLException {
    for (Query query : queries) {
      String q = query.value();
      if (query.injectTableName()) {
        String after = String.format(q, tableName);
        if (q.equals(after)) {
          throw new IllegalArgumentException(
              "Query must contain a placeholder for the table name: " + q);
        }
        q = after;
      }
      executeUpdate(q);
    }
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.isAnnotated(TableName.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return getStoredTableName(extensionContext);
  }

  private Store getStore(ExtensionContext context) {
    return context.getStore(
        Namespace.create(
            SPANNER_EXTENSION_NAMESPACE_STRING, getClass(), context.getRequiredTestMethod()));
  }

  private String getStoredTableName(ExtensionContext context) {
    return getStore(context).get(TABLE_NAME, String.class);
  }

  private String removeStoredTableName(ExtensionContext context) {
    return getStore(context).remove(TABLE_NAME, String.class);
  }

  private void setStoredTableName(ExtensionContext context, String tableName) {
    getStore(context).put(TABLE_NAME, tableName);
  }

  private void executeUpdate(String sql) throws SQLException {
    System.out.println("executeUpdate: " + sql);
    Statement statement = connection.createStatement();
    statement.executeUpdate(sql);
  }

  private void dropTableIfExists(String tableName) throws SQLException {
    try {
      executeUpdate(String.format("drop table %s", tableName));
    } catch (SQLException e) {
      if (e.getMessage().contains(String.format("NOT_FOUND: Table not found: %s", tableName))) {
        // ignore
      } else {
        throw e;
      }
    }
  }

  private void reconnect() throws SQLException {
    close();
    connect();
  }

  private void connect() throws SQLException {
    connection = DriverManager.getConnection(connectionUrl, connectionProperties);
  }

  private void close() throws SQLException {
    if (connection != null) {
      if (!connection.isClosed()) {
        connection.close();
      }
      connection = null;
    }
  }
}
