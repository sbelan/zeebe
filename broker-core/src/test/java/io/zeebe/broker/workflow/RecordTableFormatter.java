package io.zeebe.broker.workflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import de.vandermeer.asciitable.AT_Renderer;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestWord;
import de.vandermeer.asciitable.CWC_LongestWordMin;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.util.CollectionUtil;
import io.zeebe.util.buffer.BufferUtil;

public class RecordTableFormatter {

  private List<TypedRecord<UnpackedObject>> records = new ArrayList<>();

  public void addRecords(List<TypedRecord<UnpackedObject>> records)
  {
    this.records.addAll(records.stream()
        .filter(r -> r.getMetadata().getValueType() != ValueType.NOOP).collect(Collectors.toList()));
  }

  public String toFormattedTable()
  {

    final List<StateMachine> stateMachines = new ArrayList<>();

    for (int i = 0; i < records.size(); i++)
    {
      final TypedRecord<UnpackedObject> record = records.get(i);

      final StateMachine statemachine = stateMachines.stream().filter(m -> m.key == record.getKey())
          .findFirst()
          .orElseGet(() -> {
            final StateMachine m = new StateMachine(records.size());
            m.key = record.getKey();
            stateMachines.add(m);
            return m;
          });

      statemachine.records[i] = record;
    }

    final AsciiTable table = new AsciiTable();
    table.addRule();
    final AT_Renderer renderer = AT_Renderer.create();
    renderer.setCWC(new CWC_LongestWord());
    table.setRenderer(renderer);

    for (StateMachine stateMachine : stateMachines)
    {
      final List<String> row = stateMachine.toRow();
      table.addRow(row);
      table.addRule();
    }

    table.setPaddingLeftRight(1);
    return table.render();

  }

  private static class StateMachine
  {
    long key;
    TypedRecord[] records;


    StateMachine(int cardinality)
    {
      records = new TypedRecord[cardinality];
    }

    public List<String> toRow()
    {
      final List<String> row = new ArrayList<>(records.length);

      boolean firstRecord = true;

      for (int i = 0; i < records.length; i++)
      {
        final TypedRecord record = records[i];
        if (record != null)
        {

          if (record.getMetadata().getValueType() == ValueType.WORKFLOW_INSTANCE)
          {
            row.add(formatWIRecord(record, firstRecord));
          }
          else
          {
            row.add(defaultFormat(record));
          }

          firstRecord = false;
        }
        else
        {
          row.add("");
        }
      }

      return row;
    }

  }

  private static String formatWIRecord(TypedRecord<UnpackedObject> record, boolean firstRecord)
  {
    final StringBuilder sb = new StringBuilder();
    final RecordMetadata metadata = record.getMetadata();

    sb.append(metadata.getIntent());

//    if (firstRecord)
//    {
      final WorkflowInstanceRecord wfRecord = new WorkflowInstanceRecord();
      final UnsafeBuffer buf = new UnsafeBuffer(new byte[record.getValue().getLength()]);
      record.getValue().write(buf, 0);

      wfRecord.wrap(buf);

//      sb.append(": [element: ");
      sb.append(" [");
      sb.append(BufferUtil.bufferAsString(wfRecord.getActivityId()));
//      sb.append(", element instance key: ");
//      sb.append(record.getKey());
//      sb.append(", scope instance key: ");
//      sb.append(wfRecord.getScopeKey());
      sb.append("]");

//    }

    return sb.toString();
  }

  private static String defaultFormat(TypedRecord<UnpackedObject> record)
  {
    final StringBuilder sb = new StringBuilder();
    sb.append(record.getMetadata().getValueType());
    sb.append(", ");
    sb.append(record.getMetadata().getIntent());
    return sb.toString();
  }
}
