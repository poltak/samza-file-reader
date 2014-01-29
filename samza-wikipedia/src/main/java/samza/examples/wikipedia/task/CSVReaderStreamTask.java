package samza.examples.wikipedia.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;

public class CSVReaderStreamTask implements StreamTask
{
  private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "test-input");

  @Override
  public void process(final IncomingMessageEnvelope incomingMessageEnvelope, final MessageCollector messageCollector,
                      final TaskCoordinator taskCoordinator) throws Exception
  {
    BufferedReader bufferedReader = new BufferedReader(new FileReader(System.getenv("PWD") + "/test.csv"));
    String line;

    while ((line = bufferedReader.readLine()) != null)
    {
      messageCollector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, line));
    }
  }
}
