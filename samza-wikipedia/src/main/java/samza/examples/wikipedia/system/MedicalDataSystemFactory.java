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

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionSystemAdmin;

import java.io.FileNotFoundException;

public class MedicalDataSystemFactory implements SystemFactory
{
  /**
   * Gets SystemConsumers for the MedicalData system.
   * @param systemName The name of the system (as defined in the config?).
   * @param config Provides access to the properties file where variables for the System may be defined.
   * @param metricsRegistry Unused for now (trying to get things working without worrying about metrics just yet).
   * @return New SystemConsumer that converts the file contents into a Samza-compatible stream.
   */
  @Override
  public SystemConsumer getConsumer(final String systemName, final Config config, final MetricsRegistry metricsRegistry)
  {
    // TODO: Don't actually hardcode this!!! Use systemName parameter
    String pathToInputFile = config.get("systems.medicaldata.inputpath");
    return new MedicalDataConsumer(systemName, pathToInputFile);
  }

  /**
   * Unneeded as we just want to read from a file, rather than write.
   */
  @Override
  public SystemProducer getProducer(final String systemName, final Config config, final MetricsRegistry metricsRegistry)
  {
    throw new SamzaException("Cannot produce to read only data.");
  }

  /**
   * Not entirely sure what this does, although going by the hello-samza example, this should be fine.
   */
  @Override
  public SystemAdmin getAdmin(final String systemName, final Config config)
  {
    return new SinglePartitionSystemAdmin();
  }
}
