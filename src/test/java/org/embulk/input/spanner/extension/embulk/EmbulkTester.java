package org.embulk.input.spanner.extension.embulk;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.plugin.PluginType;
import org.embulk.spi.ExecInternal;
import org.embulk.spi.ExecSessionInternal;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.Schema;
import org.embulk.spi.util.Pages;

public class EmbulkTester implements AutoCloseable {

  private final ExecSessionInternal execSessionInternal;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  EmbulkTester(ExecSessionInternal execSessionInternal) {
    this.execSessionInternal = execSessionInternal;
  }

  public void runInput(ConfigSource inConfig, Consumer<List<Object[]>> assertion) throws Throwable {
    if (closed.get()) {
      throw new IllegalStateException("EmbulkTester is already closed.");
    }
    try {
      ExecInternal.doWith(
          this.execSessionInternal,
          () -> {
            InputPlugin plugin =
                ExecInternal.newPlugin(InputPlugin.class, inConfig.get(PluginType.class, "type"));
            return plugin.transaction(
                inConfig,
                (taskSource, schema, taskCount) -> {
                  List<MockPageOutput> outputs =
                      IntStream.range(0, taskCount)
                          .mapToObj(taskIndex -> new MockPageOutput())
                          .collect(Collectors.toList());
                  List<TaskReport> reports =
                      IntStream.range(0, taskCount)
                          .mapToObj(
                              taskIndex ->
                                  (TaskReport)
                                      plugin.run(
                                          taskSource, schema, taskIndex, outputs.get(taskIndex)))
                          .collect(Collectors.toList());
                  List<Page> pages =
                      outputs.stream().flatMap(o -> o.pages.stream()).collect(Collectors.toList());
                  assertion.accept(toObjects(schema, pages));
                  return reports;
                });
          });
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @SuppressWarnings("deprecation")
  private List<Object[]> toObjects(Schema schema, List<Page> pages) {
    return Pages.toObjects(schema, pages);
  }

  @SuppressWarnings("deprecation")
  public ConfigSource newConfigSource() {
    return execSessionInternal.getModelManager().newConfigSource();
  }

  public ConfigSource loadFromYamlString(String yaml) {
    return new ConfigLoader(execSessionInternal.getModelManager()).fromYamlString(yaml);
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      execSessionInternal.cleanup();
    }
  }
}
