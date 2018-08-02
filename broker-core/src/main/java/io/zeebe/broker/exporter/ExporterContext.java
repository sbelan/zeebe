package io.zeebe.broker.exporter;

import io.zeebe.broker.Loggers;
import io.zeebe.exporter.spi.Context;
import org.slf4j.Logger;

import java.nio.file.Path;

public class ExporterContext implements Context {
  private final Logger logger;
  private final Path workingDirectory;

  public ExporterContext() {
    this.logger = Loggers.EXPORTERS;
    this.workingDirectory = null;
  }

  public ExporterContext(final Logger logger, final Path workingDirectory) {
    this.logger = logger;
    this.workingDirectory = workingDirectory;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDirectory;
  }
}
