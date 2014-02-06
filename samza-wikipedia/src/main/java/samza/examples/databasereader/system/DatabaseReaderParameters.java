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
import samza.examples.databasereader.util.SupportedDbmsTypes;

/**
 * Aggregate class containing details for the system to connect to specified database.
 */
public class DatabaseReaderParameters
{
  private final String host;
  private final int port;
  private final String username;
  private final String password;
  private final SupportedDbmsTypes dbmsType;
  private final String databaseName;

  public DatabaseReaderParameters(final String host, final int port, final String username, final String password,
                                  final String dbmsType, final String databaseName)
      throws InvalidDbmsTypeException
  {
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
