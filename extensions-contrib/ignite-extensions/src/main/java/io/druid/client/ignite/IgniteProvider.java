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


import com.google.inject.Provider;
import com.metamx.common.logger.Logger;
import io.druid.client.cache.Cache;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.io.Closeable;
import java.io.IOException;

public class IgniteProvider implements Closeable, Provider<Ignite> {

    private final static Logger log = new Logger(IgniteProvider.class);

    protected IgniteConfig config;
    protected Ignite ignite;

    public IgniteProvider(IgniteConfig config) {
        this.config = config;
        log.info("IgniteProvider created");
        log.info("config file " + config.resolveConfigFile());
    }

    @Override
    public Ignite get() {
        if(ignite == null) {
            start();
        }
        log.info("Ignite successfully requested");
        return ignite;
    }

    public void start() {
        try {

            IgniteConfiguration igniteConfig = new IgniteConfiguration();
            igniteConfig.setGridName("druid-cache");
            igniteConfig.setClientMode(false);
            CacheConfiguration<Cache.NamedKey, byte[]> cacheConfig = new CacheConfiguration<>();
            cacheConfig.setName("druid-local-query-cache");
            cacheConfig.setCacheMode(CacheMode.valueOf("LOCAL"));
            cacheConfig.setAtomicityMode(CacheAtomicityMode.valueOf("ATOMIC"));
            cacheConfig.setSwapEnabled(false);
            cacheConfig.setStatisticsEnabled(true);
            FifoEvictionPolicy evictionPolicy = new FifoEvictionPolicy();
            evictionPolicy.setMaxSize(10000);
            cacheConfig.setEvictionPolicy(evictionPolicy);
            igniteConfig.setCacheConfiguration(cacheConfig);

            ignite = Ignition.start(igniteConfig);

            log.info("Ignition successful");
        } catch(IgniteException ie) {
            log.info("Ignition failed " + ie.getMessage());
            ie.printStackTrace();
        }
    }

    @Override
    public void close()  throws IOException {
        log.info("closing IgniteConnector");
        if(ignite != null) {
            try {
                ignite.close();
            } catch (IgniteException ie) {
                throw new IOException(ie);
            }
            finally {
                ignite = null;
            }
        }
    }
}
