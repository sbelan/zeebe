package io.zeebe.broker.system.configuration;

import java.util.Map;

/**
 * Exporter component configuration. To be expanded eventually to allow enabling/disabling
 * exporters, and other general configuration.
 */
public class ExporterCfg implements ConfigurationEntry {
  /** locally unique ID of the exporter */
  private String id;

  /** path to the JAR file containing the exporter class */
  private String path;

  /** fully qualified class name pointing to the class implementing the exporter interface */
  private String className;

  /** whether or not the exporter should cause startup to fail */
  private boolean required;

  /** map of arguments to use when instantiating the exporter */
  private Map<String, Object> args;

  @Override
  public void init(BrokerCfg globalConfig, String brokerBase, Environment environment) {
    path = ConfigurationUtil.toAbsolutePath(path, brokerBase);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public boolean isRequired() {
    return required;
  }

  public void setRequired(boolean required) {
    this.required = required;
  }

  public Map<String, Object> getArgs() {
    return args;
  }

  public void setArgs(Map<String, Object> args) {
    this.args = args;
  }
}
