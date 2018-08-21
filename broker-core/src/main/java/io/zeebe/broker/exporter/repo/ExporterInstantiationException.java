package io.zeebe.broker.exporter.repo;

public class ExporterInstantiationException extends RuntimeException {
  private static final long serialVersionUID = -7231999951981994615L;
  private static final String MESSAGE_FORMAT = "Cannot instantiate exporter [%s]";

  public ExporterInstantiationException(final String id, Throwable cause) {
    super(String.format(MESSAGE_FORMAT, id), cause);
  }
}
