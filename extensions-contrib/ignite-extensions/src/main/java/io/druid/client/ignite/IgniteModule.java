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

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.client.cache.Cache;
import io.druid.client.cache.IgniteCacheModule;
import io.druid.client.cache.IgniteLocalCacheProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
import org.apache.ignite.Ignite;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class IgniteModule implements DruidModule {

    private final static Logger log = new Logger(IgniteModule.class);


    public IgniteModule() {
        System.out.println("IGNITE MODULE CREATED");
    }

    @Override
    public void configure(Binder binder) {
        System.out.println("IGNITE MODULE CONFIGURE METHOD CALLED");

        // initialize Ignite configuration object
        IgniteConfig config = new IgniteConfig("/opt/druid/conf/ignite/druid-ignite-client.xml");

        // provide Ignite main class
        binder.bind(Ignite.class).toProvider(createIgniteProvider(config)).in(LazySingleton.class);

        // provide Ignite cache provider class
        binder.bind(Cache.class).toProvider(IgniteLocalCacheProvider.class).in(LazySingleton.class);

        System.out.println("IGNITE MODULE - IGNITE PROVIDER SUCCESSFULLY REGISTERED");
    }

    public IgniteProvider createIgniteProvider(IgniteConfig config) {
        return new IgniteProvider(config);
    }

    @Override
    public List<? extends Module> getJacksonModules() {
        System.out.println("IGNITE MODULE GET JACKSON MODULES CALLED");
        return Collections.emptyList();
    }
}
