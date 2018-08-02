package io.zeebe.exporter.spi;

import org.slf4j.Logger;

import java.nio.file.Path;

public interface Context {
  /** @return pre-configured logger for an exporter */
  Logger getLogger();

  /** @return Zeebe working directory * */
  Path getWorkingDirectory();
}
