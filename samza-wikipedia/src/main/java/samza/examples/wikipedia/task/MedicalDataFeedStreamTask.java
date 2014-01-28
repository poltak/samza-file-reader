/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package samza.examples.wikipedia.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import samza.examples.wikipedia.util.MedicalData;
import samza.examples.wikipedia.util.InvalidMedicalDataException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MedicalDataFeedStreamTask implements StreamTask
{
  private static final SystemStream OUTPUT_STREAM       = new SystemStream("kafka", "medical-test");
  private static final String       VALID_CSV_DELIMITER = ",";
  private static final String       VALID_TIME_FORMAT   = "hh:mm:ss";
  private static final int          VALID_NUM_FIELDS    = 7;

  @Override
  public void process(final IncomingMessageEnvelope incomingMessageEnvelope, final MessageCollector messageCollector,
                      final TaskCoordinator taskCoordinator)
  {
    Object dataReceived;
    try
    {
      // TODO: check that a String is actually received
      dataReceived = parseLineOfData((String) incomingMessageEnvelope.getMessage());
    } catch (Exception e)
    {
      dataReceived = "bad data";
    }
    //messageCollector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, dataReceived));
    messageCollector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, "hi"));
  }

  private MedicalData parseLineOfData(final String line) throws InvalidMedicalDataException, ParseException
  {
    String[] fields = line.split(VALID_CSV_DELIMITER);

    if (fields.length != VALID_NUM_FIELDS)
      throw new InvalidMedicalDataException("Invalid number of fields in supplied data.");

    return new MedicalData(
        fields[0],
        new SimpleDateFormat(VALID_TIME_FORMAT).parse(fields[1]),
        Integer.parseInt(fields[2]),
        Integer.parseInt(fields[3]),
        Integer.parseInt(fields[4]),
        Double.parseDouble(fields[5]),
        Double.parseDouble(fields[6])
    );
  }
}
