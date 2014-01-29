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
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MedicalDataConsumer implements SystemConsumer
{
  private static final String SYSTEM_NAME = "medicaldata";
  private static final String STREAM_NAME = "test";

  private final SystemStreamPartition ssp;
  private final Reader          fileReader;
  private       BufferedReader  bufferedReader;

  public MedicalDataConsumer(final String systemName, final String pathToInputFile) throws FileNotFoundException
  {
    this.fileReader = new FileReader(pathToInputFile);

    this.ssp = new SystemStreamPartition(SYSTEM_NAME, STREAM_NAME, new Partition(0));
  }

  @Override
  public void start()
  {
    this.bufferedReader = new BufferedReader(fileReader);
  }

  @Override
  public void stop()
  {
    try
    {
      this.bufferedReader.close();
    } catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  @Override
  public void register(final SystemStreamPartition systemStreamPartition, final String startingOffset)
  {
  }

  @Override
  public List<IncomingMessageEnvelope> poll(final Map<SystemStreamPartition, Integer> systemStreamPartitionIntegerMap,
                                            final long l)
      throws InterruptedException
  {
    List<IncomingMessageEnvelope> list = new ArrayList<IncomingMessageEnvelope>();

    String line;
//    try
//    {
//      if ((line = bufferedReader.readLine()) != null)
//      {
//        list.add(new IncomingMessageEnvelope(ssp, "", null, line));
//      }
//    } catch (IOException ignored)
//    {
//    }
    list.add(new IncomingMessageEnvelope(ssp, "", null, "hi"));
    return list;
  }
}
