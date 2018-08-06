package io.zeebe.broker.exporter;

import io.zeebe.broker.Loggers;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ExporterEnvironment {
  private final Logger logger;
  private final Path workingDirectory;

  public ExporterEnvironment() {
    this.logger = Loggers.EXPORTERS;
    this.workingDirectory = Paths.get(".").normalize();
  }

  public ExporterEnvironment(final Logger logger, final Path workingDirectory) {
    this.logger = logger;
    this.workingDirectory = workingDirectory;
  }

  public Logger getLogger() {
    return logger;
  }

  public Path getWorkingDirectory() {
    return workingDirectory;
  }
}
