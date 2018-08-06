package io.zeebe.exporter.spi;

import org.slf4j.Logger;

import java.nio.file.Path;
import java.util.Map;

public interface Context {
  /** @return pre-configured logger for an exporter */
  Logger getLogger();

  /** @return Zeebe working directory * */
  Path getWorkingDirectory();

  /** @return ID of the exporter **/
  String getId();

  /** @return Map of arguments specified in the configuration file **/
  Map<String, Object> getArgs();
}
