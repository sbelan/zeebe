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
package io.zeebe.broker.system.configuration;

import java.util.Arrays;

public class DataCfg implements ConfigurationEntry {
  private String[] directories = new String[] {"data"};

  private String defaultLogSegmentSize = "512M";

  private String snapshotPeriod = "15m";

  private String snapshotReplicationPeriod = "5m";

  @Override
  public void init(BrokerCfg globalConfig, String brokerBase, Environment environment) {
    for (int i = 0; i < directories.length; i++) {
      directories[i] = ConfigurationUtil.toAbsolutePath(directories[i], brokerBase);
    }
  }

  public String[] getDirectories() {
    return directories;
  }

  public void setDirectories(String[] directories) {
    this.directories = directories;
  }

  public String getDefaultLogSegmentSize() {
    return defaultLogSegmentSize;
  }

  public void setDefaultLogSegmentSize(String defaultLogSegmentSize) {
    this.defaultLogSegmentSize = defaultLogSegmentSize;
  }

  public String getSnapshotPeriod() {
    return snapshotPeriod;
  }

  public void setSnapshotPeriod(final String snapshotPeriod) {
    this.snapshotPeriod = snapshotPeriod;
  }

  public String getSnapshotReplicationPeriod() {
    return snapshotReplicationPeriod;
  }

  public void setSnapshotReplicationPeriod(String snapshotReplicationPeriod) {
    this.snapshotReplicationPeriod = snapshotReplicationPeriod;
  }

  @Override
  public String toString() {
    return "DataCfg{"
        + "directories="
        + Arrays.toString(directories)
        + ", defaultLogSegmentSize='"
        + defaultLogSegmentSize
        + '\''
        + ", snapshotPeriod='"
        + snapshotPeriod
        + '\''
        + ", snapshotReplicationPeriod='"
        + snapshotReplicationPeriod
        + '\''
        + '}';
  }
}
