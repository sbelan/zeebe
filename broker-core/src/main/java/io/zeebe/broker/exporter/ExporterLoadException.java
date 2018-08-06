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
package io.zeebe.broker.exporter;

import java.util.Map;

public class ExporterLoadException extends RuntimeException {
  private static final long serialVersionUID = -6900675963839784584L;
  private static final String FORMAT = "Unable to load exporter %s with args: %s";

  public ExporterLoadException(String id, Map<String, Object> args, Throwable cause) {
    super(String.format(FORMAT, id, args.toString()), cause);
  }
}
