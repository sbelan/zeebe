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
import java.util.ArrayList;
import java.util.List;

public class RecordingExporter extends ControlledTestExporter {
  private boolean wasValidated = false;
  private boolean wasOpened = false;
  private boolean wasConfigured = false;
  private boolean wasClosed = false;

  private final List<Record> exportedRecords = new ArrayList<>();

  public boolean wasValidated() {
    return wasValidated;
  }

  public boolean wasOpened() {
    return wasOpened;
  }

  public boolean wasConfigured() {
    return wasConfigured;
  }

  public boolean wasClosed() {
    return wasClosed;
  }

  public List<Record> getExportedRecords() {
    return exportedRecords;
  }

  @Override
  public void configure(Context context) {
    wasConfigured = true;
    super.configure(context);
  }

  @Override
  public boolean validate(Context context) throws Exception {
    wasValidated = true;
    return super.validate(context);
  }

  @Override
  public void open(Controller controller) {
    wasOpened = true;
    super.open(controller);
  }

  @Override
  public void close() {
    wasClosed = true;
    super.close();
  }

  @Override
  public void export(Record record) {
    exportedRecords.add(record);
    super.export(record);
  }
}
