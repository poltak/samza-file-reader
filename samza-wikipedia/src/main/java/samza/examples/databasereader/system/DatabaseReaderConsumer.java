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

import org.apache.samza.util.BlockingEnvelopeMap;
import samza.examples.databasereader.util.InvalidDbmsTypeException;
import samza.examples.databasereader.util.SupportedDbmsTypes;

public class DatabaseReaderConsumer extends BlockingEnvelopeMap
{
  private final String databaseUrl;
  private final String dbmsDriver;

  /**
   * Sets up the SystemConsumer for reading from the specified database.
   *
   * @param systemName
   * @param outputStreamName
   * @param parameters
   */
  public DatabaseReaderConsumer(final String systemName, final String outputStreamName,
                                final DatabaseReaderParameters parameters)
  {
    databaseUrl =
        "jdbc:" + parameters.getDbmsType() +
        "://" + parameters.getHost() +
        ":" + parameters.getPort() +
        "/" + parameters.getDatabaseName();
    dbmsDriver = getDbmsDriverFromType(parameters.getDbmsType());
  }

  /**
   * Switches between available JDBC drivers depending on the user specified DBMS type.
   *
   * @param dbmsType A String denoting the type of DBMS that the user has specified in the config.
   * @return A valid
   */
  private static String getDbmsDriverFromType(String dbmsType) throws InvalidDbmsTypeException
  {
    if (dbmsType.equals(SupportedDbmsTypes.MYSQL.asString()))
      return SupportedDbmsTypes.MYSQL.getDriver();
    // TODO: include more supported drivers
    else
      return null;
  }

  @Override
  public void start()
  {

  }

  @Override
  public void stop()
  {

  }
}
