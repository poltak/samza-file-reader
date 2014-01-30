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

import java.io.*;

public class MedicalDataConsumer extends BlockingEnvelopeMap
{
  private static final String SYSTEM_NAME = "medicaldata";
  private static final String STREAM_NAME = "test";

  private final SystemStreamPartition ssp;
  private       Reader                fileReader;
  private       BufferedReader        bufferedReader;

  /**
   * Sets up the SystemStreamPartition and FileReader.
   */
  public MedicalDataConsumer(final String systemName, final String pathToInputFile)
  {
    // TODO: Don't actually hard-code these!!!; change them back after working
    this.ssp = new SystemStreamPartition(SYSTEM_NAME, STREAM_NAME, new Partition(0));

    try
    {
      this.fileReader = new FileReader(pathToInputFile);
    } catch (FileNotFoundException e)
    {
      IncomingMessageEnvelope message = new IncomingMessageEnvelope(ssp, null, null, "file not found");
      try
      {
        put(ssp, message);
      } catch (InterruptedException e1)
      {
        e1.printStackTrace();
      }
    }

  }

  /**
   * Not sure if this is entirely necessary for what I want to do. If it is, it's probably a big problem.
   */
  @Override
  public void register(final SystemStreamPartition systemStreamPartition, final String startingOffset)
  {
    super.register(systemStreamPartition, startingOffset);
  }

  /**
   * What is done at the initialisation of this SystemConsumer (?)
   */
  @Override
  public void start()
  {
    this.bufferedReader = new BufferedReader(fileReader);

//    List<IncomingMessageEnvelope> list = new ArrayList<IncomingMessageEnvelope>();

    String line;
    try
    {
      while ((line = bufferedReader.readLine()) != null)
      {
        IncomingMessageEnvelope message = new IncomingMessageEnvelope(ssp, null, null, line);
        put(ssp, message);
      }
    } catch (IOException e)
    {
      IncomingMessageEnvelope message = new IncomingMessageEnvelope(ssp, null, null, "can't read from file");
      try
      {
        put(ssp, message);
      } catch (InterruptedException e1)
      {
        e1.printStackTrace();
      }
    } catch (InterruptedException e)
    {
      IncomingMessageEnvelope message = new IncomingMessageEnvelope(ssp, null, null, "");
      try
      {
        put(ssp, message);
      } catch (InterruptedException e1)
      {
        e1.printStackTrace();
      }
    }
  }

  /**
   * What is done at the destruction of this SystemConsumer (?)
   */
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

  /**
   * This logic should be called a number of times going by the method name "poll" (?).
   * Should continue to read lines from file open in the Reader, adding that line to a new IncomingMessageEnvelope.
   * @param systemStreamPartitionIntegerMap Not used in this example.
   * @param l Not used in this example.
   * @return List of IncomingMessageEnvelopes which are then put onto their specific SystemStreamPartition (?)
   */
//  @Override
//  public List<IncomingMessageEnvelope> poll(final Map<SystemStreamPartition, Integer> systemStreamPartitionIntegerMap,
//                                            final long l)
//      throws InterruptedException
//  {
//    List<IncomingMessageEnvelope> list = new ArrayList<IncomingMessageEnvelope>();
//
//    String line;
//    try
//    {
//      if ((line = bufferedReader.readLine()) != null)
//      {
//        list.add(new IncomingMessageEnvelope(ssp, "", null, line));
//      }
//    } catch (IOException e)
//    {
//      throw new InterruptedException(e.getMessage());
//    }
//    return list;
//  }
}
