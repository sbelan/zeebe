package io.zeebe.broker.exporter;

import java.util.Map;

public class ExporterLoadException extends RuntimeException {
  private static final long serialVersionUID = -6900675963839784584L;
  private static final String FORMAT = "Unable to load exporter %s with args: %s";

  public ExporterLoadException(String id, Map<String, Object> args, Throwable cause) {
    super(String.format(FORMAT, id, args.toString()), cause);
  }
}
