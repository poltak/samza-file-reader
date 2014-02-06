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
package samza.examples.databasereader.system;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionSystemAdmin;
import samza.examples.databasereader.util.InvalidDbmsTypeException;

import java.sql.SQLException;

public class DatabaseReaderSystemFactory implements SystemFactory
{
  /**
   * Determines the name of the stream upon which the query output will be placed.
   */
  private static final String OUTPUT_STREAM_NAME = "query-output";

  private static DatabaseReaderParameters getParametersFromConfig(Config config, String systemName)
      throws InvalidDbmsTypeException
  {
    String host = config.get("systems." + systemName + ".host");
    int port = config.getInt("systems." + systemName + ".port");
    String username = config.get("systems." + systemName + ".username");
    String password = config.get("systems." + systemName + ".password");
    String dbmsType = config.get("systems." + systemName + ".dbms");
    String databaseName = config.get("systems." + systemName + ".dbname");

    return new DatabaseReaderParameters(host, port, username, password, dbmsType, databaseName);
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry metricsRegistry)
  {
    final DatabaseReaderParameters params = getParametersFromConfig(config, systemName);

    try
    {
      return new DatabaseReaderConsumer(systemName, OUTPUT_STREAM_NAME, params);
    } catch (SQLException e)
    {
      e.printStackTrace();
      return null;
    } catch (ClassNotFoundException e)
    {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry metricsRegistry)
  {
    throw new SamzaException("Cannot produce to read only database.");
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config)
  {
    return new SinglePartitionSystemAdmin();
  }
}
