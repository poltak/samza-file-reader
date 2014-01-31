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
   * Also sends a test message saying "THIS IS A TEST" to the SSP.
   */
  public MedicalDataConsumer(final String systemName, final String pathToInputFile) throws FileNotFoundException
  {
    // TODO: Don't actually hard-code these!!!; change them back after working
    this.ssp = new SystemStreamPartition(SYSTEM_NAME, STREAM_NAME, new Partition(0));

    // TODO: Remove this after everything works, as it is just for debugging purposes.
    try
    {
      put(ssp, new IncomingMessageEnvelope(ssp, null, null, "THIS IS A TEST"));
    } catch (InterruptedException e)
    {
      e.printStackTrace();
    }

    this.fileReader = new FileReader(pathToInputFile);
  }

  /**
   * The WikipediaSystem seemed fine by calling the superclasses' register implementation, and this System shouldn't need
   * to deal with anything further here for now.
   */
  @Override
  public void register(final SystemStreamPartition systemStreamPartition, final String startingOffset)
  {
    super.register(systemStreamPartition, startingOffset);
    String path = systemStreamPartition.getStream();

    try
    {
      this.fileReader = new FileReader(path.split(".")[1]);
    } catch (FileNotFoundException e)
    {
      e.printStackTrace();
    }
  }

  /**
   * Reads from the file in a BufferedReader, line-by-line, putting each line in an IncomingMessageEnvelope to be put
   * on to their specified SystemStreamPartition.
   * Once no more lines left to be read from the file, noMoreMessage is set to true via setIsAtHead() call.
   */
  @Override
  public void start()
  {
    this.bufferedReader = new BufferedReader(fileReader);

    String line;
    try
    {
      while ((line = bufferedReader.readLine()) != null)
      {
        IncomingMessageEnvelope message = new IncomingMessageEnvelope(ssp, null, null, line);
        put(ssp, message);
      }

      setIsAtHead(ssp, true);
    } catch (IOException e)   // TODO: Properly handle these Exceptions.
    {
      e.printStackTrace();
    } catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  }

  /**
   * Frees access to file when done.
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


  // NOTE: No longer used since extending BlockingEnvelopeMap implementation of SystemConsumer. The logic previously
  //        done in here is now in the start() method.

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
