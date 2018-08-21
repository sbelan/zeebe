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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.zeebe.broker.exporter.repo.ExporterDescriptor;
import io.zeebe.broker.exporter.util.ControlledTestExporter;
import io.zeebe.broker.exporter.util.RecordingExporter;
import io.zeebe.broker.util.StreamProcessorRule;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;

public class ExporterStreamProcessorTest {
  private static final int PARTITION_ID = 1;
  private static final String EXPORTER_ID = "test";

  @Rule public StreamProcessorRule rule = new StreamProcessorRule();

  @Test
  public void shouldTestLifecycleOfExporters() {
    // given
    final List<Map<String, Object>> arguments =
        Arrays.asList(newConfig("foo", "bar"), newConfig("bar", "foo"));
    final List<RecordingExporter> exporters =
        Arrays.asList(new RecordingExporter(), new RecordingExporter());
    final List<ExporterDescriptor> descriptors =
        Arrays.asList(
            newMockedExporter("0", exporters.get(0), arguments.get(0)),
            newMockedExporter("1", exporters.get(1), arguments.get(1)));
    final ExporterStreamProcessor processor =
        new ExporterStreamProcessor(PARTITION_ID, descriptors);

    // when
    //    rule.initStreamProcessor(e -> processor).start();
    //
    //    // then
    //    for (int i = 0; i < exporters.size(); i++) {
    //      assertThat(exporters.get(i).wasConfigured()).isTrue();
    //      assertThat(exporters.get(i).wasOpened()).isFalse();
    //      assertThat(exporters.get(i).wasClosed()).isFalse();
    //
    //      assertThat(exporters.get(i).getContext().getConfiguration().getArguments())
    //          .isEqualTo(arguments.get(i));
    //    }
  }

  @Test
  public void shouldExport() {}

  private ExporterDescriptor newMockedExporter(
      final String id, final ControlledTestExporter exporter, final Map<String, Object> arguments) {
    final ExporterDescriptor descriptor =
        spy(new ExporterDescriptor(id, ControlledTestExporter.class, arguments));

    doAnswer(i -> exporter).when(descriptor).newInstance();

    return descriptor;
  }

  private Map<String, Object> newConfig(String... pairs) {
    final Map<String, Object> config = new HashMap<>();

    for (int i = 0; i < pairs.length; i += 2) {
      config.put(pairs[i], pairs[i + 1]);
    }

    return config;
  }
}
