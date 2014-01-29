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
package samza.examples.wikipedia.system;

import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import samza.examples.wikipedia.system.MedicalDataFeed.MedicalDataFeedEvent;
import samza.examples.wikipedia.system.MedicalDataFeed.MedicalDataFeedListener;

public class MedicalDataConsumer extends BlockingEnvelopeMap implements MedicalDataFeedListener
{
  private final String          systemName;
  private final MedicalDataFeed feed;

  public MedicalDataConsumer(final String systemName, final MedicalDataFeed feed)
  {
    this.systemName = systemName;
    this.feed = feed;
  }

  public void onEvent(final MedicalDataFeedEvent event)
  {
    SystemStreamPartition systemStreamPartition =
        new SystemStreamPartition(systemName, event.getInputFileName(), new Partition(0));

    try
    {
      put(systemStreamPartition,
          new IncomingMessageEnvelope(systemStreamPartition, null, null, event));
    } catch (InterruptedException e)
    {
      // TODO
      e.printStackTrace();
    }
  }

  @Override
  public void start()
  {
    feed.start();
    feed.listen(this);
  }

  @Override
  public void stop()
  {
    feed.unlisten(this);
    feed.stop();
  }

  @Override
  public void register(final SystemStreamPartition systemStreamPartition, final String startingOffset)
  {
    super.register(systemStreamPartition, startingOffset);
  }
}
