package samza.examples.wikipedia.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import samza.examples.wikipedia.system.WikipediaFeed;

import java.util.Map;

/**
 * Created by poltak on 15/01/2014.
 */
public class GentooFeedParserStreamTask implements StreamTask
{
  private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "gentoo-parsed");

  @Override
  public void process(final IncomingMessageEnvelope incomingMessageEnvelope, final MessageCollector messageCollector,
                      final TaskCoordinator taskCoordinator) throws Exception
  {
    Map<String, Object> jsonObject = (Map<String, Object>) incomingMessageEnvelope.getMessage();
    WikipediaFeed.WikipediaFeedEvent event = new WikipediaFeed.WikipediaFeedEvent(jsonObject);

    String parsedJson = "at "
                        + String.valueOf(event.getTime())
                        + ", "
                        + event.getSource()
                        + " said:\n"
                        + event.getRawEvent();
    messageCollector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, parsedJson));
  }
}
