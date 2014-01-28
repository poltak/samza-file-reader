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

package samza.examples.wikipedia.util;

import java.util.Date;

public class MedicalData
{
  private final String id;
  private final Date   timestamp;
  private final int bloodPressureSystolic;
  private final int bloodPressureDiastolic;
  private final int heartRate;
  private final double co2Percentage;
  private final double oxygenPercentage;

  public MedicalData(final String id,
                     final Date timestamp,
                     final int bloodPressureSystolic,
                     final int bloodPressureDiastolic,
                     final int heartRate,
                     final double co2Percentage,
                     final double oxygenPercentage)
      throws InvalidMedicalDataException
  {
    this.id = id;
    // TODO: Possible source of error.
    this.timestamp = new Date(timestamp.getTime());
    this.bloodPressureSystolic = bloodPressureSystolic;
    this.bloodPressureDiastolic = bloodPressureDiastolic;
    this.heartRate = heartRate;
    this.co2Percentage = co2Percentage;
    this.oxygenPercentage = oxygenPercentage;
  }

  @Override
  public String toString()
  {
    return "ID: " + id + "\n" +
           "Timestamp:" + timestamp.toString() + "\n" +
           "Blood Pressure (S): " + bloodPressureSystolic + "\n" +
           "Blood Pressure (D): " + bloodPressureDiastolic + "\n" +
           "Heart Rate: " + heartRate + "\n" +
           "CO2 Percentage: " + co2Percentage + "%\n" +
           "Oxygen Percentage: " + oxygenPercentage + "%";
  }
}
