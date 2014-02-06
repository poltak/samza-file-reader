package samza.examples.databasereader.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class DatabaseTestStreamTask implements StreamTask
{
  private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "dbtest");

  @Override
  public void process(final IncomingMessageEnvelope incomingMessageEnvelope, final MessageCollector messageCollector,
                      final TaskCoordinator taskCoordinator) throws Exception
  {
    messageCollector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, incomingMessageEnvelope.getMessage()));
  }
}
