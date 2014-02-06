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

import samza.examples.databasereader.util.InvalidDbmsTypeException;
import samza.examples.databasereader.util.InvalidSystemParametersException;
import samza.examples.databasereader.util.SupportedDbmsTypes;

/**
 * Aggregate class containing user specified system parameters for the database connection.
 */
public class DatabaseReaderParameters
{
  private static final int PORT_LOWER_BOUND = 1;
  private static final int PORT_UPPER_BOUND = 50000;

  private final String host;
  private final int port;
  private final String username;
  private final String password;
  private final SupportedDbmsTypes dbmsType;
  private final String databaseName;

  /**
   * Constructs a new aggregate object containing all user specified system parameters.
   * @param host DNS resolvable hostname of the database (or IP address).
   * @param port Port through which access to the database is made.
   * @param username User with database access' username. Optional.
   * @param password User with database access' password. Optional.
   * @param dbmsType Type of DMBS as a string.
   * @param databaseName The name of the database.
   * @throws InvalidDbmsTypeException Thrown if user specified DBMS type is not valid.
   */
  public DatabaseReaderParameters(final String host, final int port, final String username, final String password,
                                  final String dbmsType, final String databaseName)
      throws InvalidSystemParametersException
  {
    if (port < PORT_LOWER_BOUND || port > PORT_UPPER_BOUND)
    {
      throw new InvalidSystemParametersException();
    }

    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.dbmsType = SupportedDbmsTypes.getTypeFromString(dbmsType);
    this.databaseName = databaseName;
  }

  public String getHost()
  {
    return host;
  }

  public int getPort()
  {
    return port;
  }

  public String getUsername()
  {
    return username;
  }

  public String getPassword()
  {
    return password;
  }

  public SupportedDbmsTypes getDbmsType()
  {
    return dbmsType;
  }

  public String getDatabaseName()
  {
    return databaseName;
  }
}
