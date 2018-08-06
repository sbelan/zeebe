package io.zeebe.broker.exporter;

import io.zeebe.exporter.spi.Context;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.util.Map;

public class ExporterContext implements Context {
  private final String id;
  private final Map<String, Object> args;
  private final ExporterEnvironment env;

  public ExporterContext(
      final String id, final Map<String, Object> args, final ExporterEnvironment env) {
    this.id = id;
    this.args = args;
    this.env = env;
  }

  @Override
  public Logger getLogger() {
    return env.getLogger();
  }

  @Override
  public Path getWorkingDirectory() {
    return env.getWorkingDirectory();
  }

  @Override
  public Map<String, Object> getArgs() {
    return args;
  }

  @Override
  public String getId() {
    return id;
  }
}
