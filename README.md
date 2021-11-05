# embulk-input-spanner

An embulk input plugin to load records from [Cloud Spanner](https://cloud.google.com/spanner/).

## Overview

* **Plugin type**: input
* **Resume supported**: yes

## Configuration

- **driver_path**: path to the jar file of the Spanner JDBC driver. If not set, the bundled JDBC driver ([java-spanner-jdbc v2.4.5](https://github.com/googleapis/java-spanner-jdbc/releases/tag/v2.4.5)) will be used (string, optional)
- **project_id**: GCP project ID (string, required)
- **instance_id**: Cloud Spanner instance ID (string, required)
- **database_id**: Cloud Spanner database ID (string, required)
- **credentials**: Path or the content of the credentials file to use for the connection. If you do not specify any credentials at all, the default credentials of the environment as returned by [`GoogleCredentials#getApplicationDefault()`](https://github.com/googleapis/google-auth-library-java/blob/fe3d48b/oauth2_http/java/com/google/auth/oauth2/GoogleCredentials.java#L96-L98) is used. (`LocalFile`, optional)
  - When you define the content directly in YAML, you can use **credential.content** option like below.
    ```
    credentials:
      content: |
        {
          "type": "service_account",
          ...<snip>...
        }
    ```
- **oauth_token**: A valid pre-existing OAuth token to use for authentication for this connection. Setting this property will take precedence over any value set for **credentials**. (string, optional)
- **optimizer_version**: Sets the default query optimizer version to use for this connection. See also https://cloud.google.com/spanner/docs/query-optimizer/query-optimizer-versions. (string, optional)
- **fetch_rows**: number of rows to fetch one time (used for `java.sql.Statement#setFetchSize`) (integer, default: `10000`)
- **connect_timeout**: not supported.
- **socket_timeout**: timeout for executing the query. 0 means no timeout. (integer (seconds), default: 1800)
- **options**: extra JDBC properties (hash, default: {})
  - See. [https://github.com/googleapis/java-spanner-jdbc#connection-url-properties](Cloud Spanner JDBC Connection Properties)
- If you write SQL directly,
  - **query**: SQL to run (string)
  - **use_raw_query_with_incremental**: If true, you can write optimized query using prepared statement. See [Use incremental loading with raw query](#use-incremental-loading-with-raw-query) for more detail (boolean, default: false)
- If **query** is not set,
  - **table**: destination table name (string, required)
  - **select**: expression of select (e.g. `id, created_at`) (string, default: "*")
  - **where**: WHERE condition to filter the rows (string, default: no-condition)
  - **order_by**: expression of ORDER BY to sort rows (e.g. `created_at DESC, id ASC`) (string, default: not sorted)
- **default_timezone**: If the sql type of a column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted int this default_timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **default_column_options**: advanced: column_options for each JDBC type as default. key-value pairs where key is a JDBC type (e.g. 'DATE', 'BIGINT') and value is same as column_options's value.
- **column_options**: advanced: key-value pairs where key is a column name and value is options for the column.
  - **value_type**: embulk get values from database as this value_type. Typically, the value_type determines `getXXX` method of `java.sql.PreparedStatement`. `value_type: json` is an exception which uses `getString` and parses the result as a JSON string.
  (string, default: depends on the sql type of the column. Available values options are: `long`, `double`, `float`, `decimal`, `boolean`, `string`, `json`, `date`, `time`, `timestamp`)
  - **type**: Column values are converted to this embulk type.
  Available values options are: `boolean`, `long`, `double`, `string`, `json`, `timestamp`).
  By default, the embulk type is determined according to the sql type of the column (or value_type if specified).
  - **timestamp_format**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted by this timestamp_format. And if the embulk type is `timestamp`, this timestamp_format may be used in the output plugin. For example, stdout plugin use the timestamp_format, but *csv formatter plugin doesn't use*. (string, default : `%Y-%m-%d` for `date`, `%H:%M:%S` for `time`, `%Y-%m-%d %H:%M:%S` for `timestamp`)
  - **timezone**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted in this timezone.
(string, value of default_timezone option is used by default)
- **before_setup**: if set, this SQL will be executed before setup. You can prepare table for input by this option.
- **before_select**: if set, this SQL will be executed before the SELECT query in the same transaction.
- **after_select**: if set, this SQL will be executed after the SELECT query in the same transaction.

## Incremental loading

Incremental loading uses monotonically increasing unique columns (such as auto-increment id) to load records inserted (or updated) after last execution.

First, if `incremental: true` is set, this plugin loads all records with additional ORDER BY. For example, if `incremental_columns: [updated_at, id]` option is set, query will be as following:

```
SELECT * FROM (
  ...original query is here...
)
ORDER BY updated_at, id
```

When bulk data loading finishes successfully, it outputs `last_record: ` paramater as config-diff so that next execution uses it.

At the next execution, when `last_record: ` is also set, this plugin generates additional WHERE conditions to load records larger than the last record. For example, if `last_record: ["2017-01-01 00:32:12", 5291]` is set,

```
SELECT * FROM (
  ...original query is here...
)
WHERE updated_at > '2017-01-01 00:32:12' OR (updated_at = '2017-01-01 00:32:12' AND id > 5291)
ORDER BY updated_at, id
```

Then, it updates `last_record: ` so that next execution uses the updated last_record.

**IMPORTANT**: If you set `incremental_columns: ` option, make sure that there is an index on the columns to avoid full table scan. For this example, following index should be created:

```
CREATE INDEX embulk_incremental_loading_index ON table (updated_at, id);
```

Recommended usage is to leave `incremental_columns` unset and let this plugin automatically finds an auto-increment primary key. Currently, only strings and integers are supported as incremental_columns.

TIMESTAMP, TIMESTAMPTZ, DATE and DATETIME are also supported depends on each RDBMS

### Use incremental loading with raw query

**IMPORTANT**: This is an advanced feature and assume you have an enough knowledge about incremental loading using Embulk and this plugin

Normally, you can't write your own query for incremental loading.
`use_raw_query_with_incremental` option allow you to write raw query for incremental loading. It might be well optimized and faster than SQL statement which is automatically generated by plugin.

Prepared statement starts with `:` is available instead of fixed value.
`last_record` value is necessary when you use this option.
Please use prepared statement that is well distinguishable in SQL statement
* `select * from ...` statement causes `java.lang.IndexOutOfBoundsException` error.
* Using too simple prepared statement like `:a` might cause SQL parse failure.

In the following example, prepared statement `:foo_id` will be replaced with value "1" which is specified in `last_record`.

```yaml
in:
  type: spanner
  query: |
    -- Specify the columns to be used in the incremental column at the beginning of the select clause
    SELECT
      foo.id as foo_id, bar.name
    FROM
      foo LEFT JOIN bar ON foo.id = bar.id
    WHERE
      foo.hoge IS NOT NULL
      AND foo.id > :foo_id
    ORDER BY
      foo.id ASC
  use_raw_query_with_incremental: true
  incremental_columns:
    - foo_id
  incremental: true
  last_record: [1]
```

## Example

```yaml
in:
  type: spanner
  auth_method: json_key
  json_key: /path/to/credentials.json
  project_id: test-project
  instance_id: test-instance
  database_id: test-database
  table: test_table
  socket_timeout: 0

out:
  type: stdout
```

## Development

### Run an example

```shell
$ docker-compose up
$ ./gradlew gem
$ embulk run example/config.yml -I build/gemContents/lib
```

### Run tests

```shell
$ ./gradlew test
```

### See the data in the Spanner Emulator

```shell
$ docker-compose up
$ docker-compose run --rm spanner-cli spanner-cli -p test-project -i test-instance -d test-database
```

See [DDL](./example/ddl/schema.sql) & [DML](./example/dml/dml.sql)

### Update dependencies locks

```shell
$ ./gradlew dependencies --write-locks
```

### Run the formatter

```shell
## Just check the format violations
$ ./gradlew spotlessCheck

## Fix the all format violations
$ ./gradlew spotlessApply
```

### Release a new gem

A new tag is pushed, then a new gem will be released. See [the Github Action CI Setting](./.github/workflows/main.yml).

## CHANGELOG

[CHANGELOG.md](./CHANGELOG.md)

## License

[MIT LICENSE](./LICENSE.txt)

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
