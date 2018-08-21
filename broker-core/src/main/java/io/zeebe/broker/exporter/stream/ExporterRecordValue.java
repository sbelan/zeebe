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
package io.zeebe.broker.exporter.stream;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.ArrayProperty;
import io.zeebe.msgpack.property.LongProperty;
import io.zeebe.msgpack.property.StringProperty;
import io.zeebe.msgpack.value.ValueArray;

public class ExporterRecordValue extends UnpackedObject {
  public static final long POSITION_UNKNOWN = -1L;
  public static final String ID_UNKNOWN = "";

  private ArrayProperty<ExporterPosition> positionsProperty =
      new ArrayProperty<>("positions", new ExporterPosition());

  public ExporterRecordValue() {
    this.declareProperty(positionsProperty);
  }

  public ValueArray<ExporterPosition> getPositionsProperty() {
    return positionsProperty;
  }

  static class ExporterPosition extends UnpackedObject {
    private StringProperty idProperty = new StringProperty("id", ID_UNKNOWN);
    private LongProperty positionProperty = new LongProperty("position", POSITION_UNKNOWN);

    ExporterPosition() {
      this.declareProperty(idProperty);
      this.declareProperty(positionProperty);
    }

    StringProperty getIdProperty() {
      return idProperty;
    }

    LongProperty getPositionProperty() {
      return positionProperty;
    }
  }
}
