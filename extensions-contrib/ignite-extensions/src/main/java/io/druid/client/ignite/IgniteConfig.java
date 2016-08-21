/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client.ignite;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.logger.Logger;


public class IgniteConfig {

    public static final String DEFAULT_CONFIG_FILE = "META-INF/ignite/druid-ignite-client.xml";

    private final static Logger log = new Logger(IgniteConfig.class);

    @JsonProperty
    private String configFile;


    @JsonCreator
    public IgniteConfig(@JsonProperty("configFile") String configFile) {
        System.out.println("IGNITE CONFIG CONSTRUCTOR CALLED");
        this.configFile = configFile;
        log.info("IgniteConfig created");
    }

    @JsonProperty
    public String getConfigFile() {
        System.out.println("IGNITE CONFIG PROPERTY GETCONFIGFILE CALLED");
        return configFile;
    }

    public String resolveConfigFile() {
        return (configFile != null) ? configFile : DEFAULT_CONFIG_FILE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IgniteConfig that = (IgniteConfig) o;

        if (configFile != null ? !configFile.equals(that.configFile) : that.configFile != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = configFile != null ? configFile.hashCode() : 0;
        return result;
    }
}
