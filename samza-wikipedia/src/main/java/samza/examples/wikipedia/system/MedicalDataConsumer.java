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
  private final SystemStreamPartition ssp;
  private final Reader                fileReader;
  private       BufferedReader        bufferedReader;

  /**
   * Sets up the SystemStreamPartition and FileReader to work with the specified system, stream and files.
   */
  public MedicalDataConsumer(final String systemName, final String streamName, final String pathToInputFile) throws FileNotFoundException
  {
    this.ssp = new SystemStreamPartition(systemName, streamName, new Partition(0));
    this.fileReader = new FileReader(pathToInputFile);
  }

  @Override
  public void register(final SystemStreamPartition systemStreamPartition, final String startingOffset)
  {
    super.register(systemStreamPartition, startingOffset);
  }

  /**
   * Constructs a new BufferedReader on the previously constructed FileReader and attempts to read in the input files.
   * If the file read is successful, setIsHead() is called to specify that the SystemStreamPartition has "caught up".
   */
  @Override
  public void start()
  {
    this.bufferedReader = new BufferedReader(fileReader);

    Thread fileReadingThread = new Thread(
      new Runnable()
      {
        @Override
        public void run()
        {
          try
          {
            readInputFiles();
            setIsAtHead(ssp, true);
          } catch (InterruptedException e)
          {
            e.getStackTrace();
            stop();
          }
        }
      }
    );

    fileReadingThread.start();
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

  /**
   * Reads from the file in a BufferedReader, line-by-line, putting each line in an IncomingMessageEnvelope to be put
   * onto the specified SystemStreamPartition.
   */
  private void readInputFiles() throws InterruptedException
  {
    String line;

    try
    {
      while ((line = bufferedReader.readLine()) != null)
      {
        put(ssp, new IncomingMessageEnvelope(ssp, null, null, line));
      }
    } catch (IOException e)
    {
      put(ssp, new IncomingMessageEnvelope(ssp, null, null, "ERROR: Cannot read from input file:\n" + e.getMessage()));
    }
  }



//  private class ReadFile implements Runnable
//  {
//    /**
//     * When an object implementing interface <code>Runnable</code> is used to create a thread, starting the thread causes
//     * the object's <code>run</code> method to be called in that separately executing thread.
//     * <p/>
//     * The general contract of the method <code>run</code> is that it may take any action whatsoever.
//     *
//     * @see Thread#run()
//     */
//    @Override
//    public void run()
//    {
//      try
//      {
//        readInputFiles();
//        setIsAtHead(ssp, true);
//      } catch (InterruptedException e)
//      {
//        e.getStackTrace();
//        stop();
//      }
//    }
//  }
}
