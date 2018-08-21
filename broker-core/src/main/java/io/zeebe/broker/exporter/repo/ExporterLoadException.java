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
package io.zeebe.broker.exporter.repo;

public class ExporterLoadException extends Exception {
  private static final long serialVersionUID = -9192947670450762759L;
  private static final String MESSAGE_FORMAT = "Cannot load exporter [%s]: %s";

  public ExporterLoadException(final String id, final String reason) {
    super(String.format(MESSAGE_FORMAT, id, reason));
  }

  public ExporterLoadException(final String id, final String reason, final Throwable cause) {
    super(String.format(MESSAGE_FORMAT, id, reason), cause);
  }
}
