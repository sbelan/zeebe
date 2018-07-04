package io.zeebe.broker.workflow.processor.v2;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.intent.Intent;

public class RecordWriter {

//  // TODO: always propagate request context if no response is sent
//
//  public void reject(RejectionType type, String reason)
//  {
//    // TODO: schedules record and response
//  }
//
//  public void accept(Intent intent, UnpackedObject value)
//  {
//
//  }
//
//  public void accept(Intent intent, UnpackedObject value, boolean respondOnCommit)
//  {
//
//  }

  // TODO: wäre nett, wenn einheitlich wäre, wann Responses gesendet werden, denn dann muss man
  // das bei der Benutzung dieser Klasse wissen

  // TODO: need to differentiate commands and events here

  public void publish(Intent intent, UnpackedObject value)
  {
    // TODO: writes record and responds based on flag
  }


  public void publish(long key, Intent intent, UnpackedObject value)
  {
    // TODO: writes record and responds based on flag
  }

}
