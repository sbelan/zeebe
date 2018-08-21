/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.exporter.util;

import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.spi.Exporter;
import java.util.function.Consumer;
import java.util.function.Function;

public class ControlledTestExporter implements Exporter {
  private Function<Context, Boolean> onValidate;
  private Consumer<Context> onConfigure;
  private Consumer<Controller> onOpen;
  private Runnable onClose;
  private Consumer<Record> onExport;

  private Context context;
  private Controller controller;

  public ControlledTestExporter onValidate(final Function<Context, Boolean> callback) {
    onValidate = callback;
    return this;
  }

  public ControlledTestExporter onConfigure(final Consumer<Context> callback) {
    onConfigure = callback;
    return this;
  }

  public ControlledTestExporter onOpen(final Consumer<Controller> callback) {
    onOpen = callback;
    return this;
  }

  public ControlledTestExporter onClose(final Runnable callback) {
    onClose = callback;
    return this;
  }

  public ControlledTestExporter onExport(final Consumer<Record> callback) {
    onExport = callback;
    return this;
  }

  public Context getContext() {
    return context;
  }

  public void setContext(final Context context) {
    this.context = context;
  }

  public Controller getController() {
    return controller;
  }

  public void setController(final Controller controller) {
    this.controller = controller;
  }

  @Override
  public void configure(final Context context) {
    this.context = context;

    if (onConfigure != null) {
      onConfigure.accept(context);
    }
  }

  @Override
  public void open(final Controller controller) {
    this.controller = controller;

    if (onOpen != null) {
      onOpen.accept(controller);
    }
  }

  @Override
  public void close() {
    if (onClose != null) {
      onClose.run();
    }
  }

  @Override
  public void export(final Record record) {
    if (onExport != null) {
      onExport.accept(record);
    }
  }
}
