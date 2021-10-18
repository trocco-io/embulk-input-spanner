package org.embulk.input.spanner;

import java.util.List;
import java.util.Optional;
import javax.validation.Validation;
import javax.validation.Validator;
import org.apache.bval.jsr303.ApacheValidationProvider;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.SchemaConfig;

public class SpannerInputPlugin implements InputPlugin {
  private static final Validator VALIDATOR =
      Validation.byProvider(ApacheValidationProvider.class)
          .configure()
          .buildValidatorFactory()
          .getValidator();
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().withValidator(VALIDATOR).build();

  public interface PluginTask extends Task {
    // configuration option 1 (required integer)
    @Config("option1")
    public int getOption1();

    // configuration option 2 (optional string, null is not allowed)
    @Config("option2")
    @ConfigDefault("\"myvalue\"")
    public String getOption2();

    // configuration option 3 (optional string, null is allowed)
    @Config("option3")
    @ConfigDefault("null")
    public Optional<String> getOption3();

    // if you get schema from config
    @Config("columns")
    public SchemaConfig getColumns();
  }

  @Override
  public ConfigDiff transaction(ConfigSource config, InputPlugin.Control control) {
    final ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
    final PluginTask task = configMapper.map(config, PluginTask.class);

    Schema schema = task.getColumns().toSchema();
    int taskCount = 1; // number of run() method calls

    return resume(task.dump(), schema, taskCount, control);
  }

  @Override
  public ConfigDiff resume(
      TaskSource taskSource, Schema schema, int taskCount, InputPlugin.Control control) {
    control.run(taskSource, schema, taskCount);
    return CONFIG_MAPPER_FACTORY.newConfigDiff();
  }

  @Override
  public void cleanup(
      TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {}

  @Override
  public TaskReport run(TaskSource taskSource, Schema schema, int taskIndex, PageOutput output) {
    final TaskMapper taskMapper = CONFIG_MAPPER_FACTORY.createTaskMapper();
    final PluginTask task = taskMapper.map(taskSource, PluginTask.class);
    // Write your code here :)
    throw new UnsupportedOperationException("SpannerInputPlugin.run method is not implemented yet");
  }

  @Override
  public ConfigDiff guess(ConfigSource config) {
    return CONFIG_MAPPER_FACTORY.newConfigDiff();
  }
}
