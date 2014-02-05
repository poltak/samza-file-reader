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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class FileReaderConsumer extends BlockingEnvelopeMap
{
  private final SystemStreamPartition systemStreamPartition;
  private final Reader                fileReader;
  private final BufferedReader        bufferedReader;

  /**
   * Sets up the SystemConsumer for reading from the specified file.
   * @param systemName The name of this system.
   * @param streamName The name of the stream upon which to place file contents.
   * @param pathToInputFile The filesystem path to the file from which to read from.
   * @throws FileNotFoundException Thrown if the <code>pathToInputFile</code> arg does not point to a readable file.
   */
  public FileReaderConsumer(final String systemName, final String streamName, final String pathToInputFile)
      throws FileNotFoundException
  {
    this.systemStreamPartition = new SystemStreamPartition(systemName, streamName, new Partition(0));
    this.fileReader = new FileReader(pathToInputFile);
    this.bufferedReader = new BufferedReader(fileReader);
  }

  @Override
  public void register(final SystemStreamPartition systemStreamPartition, final String startingOffset)
  {
    super.register(systemStreamPartition, startingOffset);
  }

  /**
   * Constructs a new BufferedReader on the previously constructed FileReader and attempts to read in the input file.
   * Reading from the file and placing the contents onto the SystemStreamPartition is done on a separate thread.
   * If the file read is successful, setIsHead() is called to specify that the SystemStreamPartition has "caught up".
   */
  @Override
  public void start()
  {
    Thread fileReadingThread = new Thread(
      new Runnable()
      {
        @Override
        public void run()
        {
          try
          {
            readInputFiles();
            setIsAtHead(systemStreamPartition, true);
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
   *
   * Calling from a separate thread is advised.
   * @throws InterruptedException Thrown if System is interrupted while attempting to place file contents onto the
   * specified SystemStreamPartition.
   */
  private void readInputFiles() throws InterruptedException
  {
    String line;

    try
    {
      while ((line = bufferedReader.readLine()) != null)
      {
        put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, null, null, line));
      }
    } catch (IOException e)
    {
      put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, null, null,
              "ERROR: Cannot read from input file:\n" + e.getMessage()));
    }
  }


  /**
   * Threshold used to determine when there are too many IncomingMessageEnvelopes to be put onto the BlockingQueue.
   */
  private static final int BOUNDED_QUEUE_THRESHOLD = 100;

  /**
   * Constructs a new bounded BlockingQueue of IncomingMessageEnvelopes. The bound is determined by the
   * <code>BOUNDED_QUEUE_THRESHOLD</code> constant.
   * @return A bounded queue used for queueing IncomingMessageEnvelopes to be sent to their specified destinations.
   */
  @Override
  protected BlockingQueue<IncomingMessageEnvelope> newBlockingQueue()
  {
    return new LinkedBlockingQueue<IncomingMessageEnvelope>(BOUNDED_QUEUE_THRESHOLD);
  }
}