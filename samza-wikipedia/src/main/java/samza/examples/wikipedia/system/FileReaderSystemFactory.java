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

public class FileReaderSystemFactory implements SystemFactory {
    /**
     * Determines the name of the stream upon which the file contents will be placed.
     */
    private static final String OUTPUT_STREAM_NAME = "file-contents";

    /**
     * Gets SystemConsumers for the MedicalData system.
     *
     * @param systemName The name of the System.
     * @param config     Provides access to the properties file where variables for the System may be defined.
     * @return New SystemConsumer that converts the file contents into a Samza-compatible stream.
     */
    @Override
    public SystemConsumer getConsumer(final String systemName, final Config config, final MetricsRegistry metricsRegistry) {
        String pathToInputFile = config.get("systems." + systemName + ".inputpath");

        try {
            return new FileReaderConsumer(systemName, OUTPUT_STREAM_NAME, pathToInputFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public SystemProducer getProducer(final String systemName, final Config config, final MetricsRegistry metricsRegistry) {
        throw new SamzaException("Cannot produce to read only data.");
    }

    @Override
    public SystemAdmin getAdmin(final String systemName, final Config config) {
        return new SinglePartitionSystemAdmin();
    }
}
