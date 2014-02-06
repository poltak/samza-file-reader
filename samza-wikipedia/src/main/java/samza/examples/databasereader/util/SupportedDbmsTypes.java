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
package samza.examples.databasereader.util;

public enum SupportedDbmsTypes
{
  // Add supported DBMS types and their corresponding JDBC drivers here
  MYSQL   ("mysql", "com.mysql.jdbc.Driver");


  private final String value;
  private final String driver;

  SupportedDbmsTypes(final String value, final String driver)
  {
    this.value = value;
    this.driver = driver;
  }

  public String asString()
  {
    return value;
  }

  public String getDriver()
  {
    return driver;
  }

  public static SupportedDbmsTypes getTypeFromString(String dbmsType) throws InvalidDbmsTypeException
  {
    if (dbmsType.equals(MYSQL.asString()))
    {
      return MYSQL;
    } else
    {
      throw new InvalidDbmsTypeException();
    }
  }
}
