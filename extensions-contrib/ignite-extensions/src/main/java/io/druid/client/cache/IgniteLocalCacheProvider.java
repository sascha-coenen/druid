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

package io.druid.client.cache;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.metamx.common.logger.Logger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

@JsonTypeName("ignitelocal")
public class IgniteLocalCacheProvider implements CacheProvider {

    private final static Logger log = new Logger(IgniteLocalCacheProvider.class);

    protected Ignite ignite;
    protected IgniteDruidCache igniteDruidCache;


    @JsonCreator
    public IgniteLocalCacheProvider() {
        System.out.println("IGNITE LOCAL CACHE PROVIDER - CREATED");
    }

    @Inject
    public void inject(Injector injector) {
        System.out.println("IGNITE LOCAL CACHE PROVIDER - IGNITE BEING INJECTED");
        try {
            ignite = injector.getInstance(Ignite.class);
            System.out.println("IGNITE LOCAL CACHE PROVIDER - IGNITE INJECTION SUCCESSFUL, ignite=" + ignite);
        } catch(Throwable t) {
            System.out.println("IGNITE LOCAL CACHE PROVIDER - EXCEPTION, message: " + t.getMessage());
        }
    }

    @Override
    public Cache get() {
        log.info("IgniteLocalCacheProvider.get() called");
        if(igniteDruidCache == null) {
            IgniteCache<Cache.NamedKey, byte[]> cache = ignite.<Cache.NamedKey, byte[]>cache("druid-local-query-cache");
            igniteDruidCache = new IgniteDruidCache(cache, true);
        }
        log.info("IgniteLocalCacheProvider.get() call returned " + ((igniteDruidCache != null) ? "valid" : "empty") + " cache object");
        return igniteDruidCache;
    }
}
