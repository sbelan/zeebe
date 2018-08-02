package io.zeebe.broker.exporter;

import io.zeebe.broker.system.configuration.ExporterCfg;

public class ExporterLoadException extends RuntimeException {
  private static final long serialVersionUID = -6900675963839784584L;
  private static final String FORMAT = "Unable to load exporter as defined by: %s";

  public ExporterLoadException(ExporterCfg config, Throwable cause) {
    super(String.format(FORMAT, config.toString()), cause);
  }
}
