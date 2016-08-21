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


import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// TODO: add withLZ4Compressor decorator

public class IgniteDruidCache implements Cache {

    private final static Logger log = new Logger(IgniteDruidCache.class);

    protected IgniteCache<Cache.NamedKey, byte[]> cache;
    protected boolean isLocal;
    protected long numErrors = 0;


    public IgniteDruidCache(IgniteCache<Cache.NamedKey, byte[]> cache, boolean isLocal) {
        this.cache = cache;
        this.isLocal = isLocal;
        log.info("IgniteDruidCache created");
    }


    @Override
    public byte[] get(NamedKey key) {
        try {
            log.info("Ignite Cache: about to fetch a cache entry");
            byte[] val = cache.get(key);
            log.info("Ignite Cache: cache fetch " + ((val != null) ? "successful" : "missed"));
            return  val;
        } catch (Throwable t) {
            numErrors++;
            log.info("exception while getting single cache entry " + t.getMessage());
            throw t;
        }
    }

    @Override
    public void put(NamedKey key, byte[] value) {
        try {
            log.info("about to put entry into cache");
            cache.put(key, value);
        } catch (Throwable t) {
            numErrors++;
            log.info("exception while setting single cache entry " + t.getMessage());
            throw t;
        }
    }

    @Override
    public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys) {
        Set<NamedKey> keySet = Sets.newHashSet(keys);
        try {
            log.info("IgniteCache - about to bulk get cache entries");
            return cache.getAll(keySet);
        } catch (Throwable t) {
            numErrors += keySet.size();
            log.info("exception while bulk-getting cache entries " + t.getMessage());
            throw t;
        }
    }

    @Override
    public void close(String namespace) {
        ScanQuery<NamedKey, byte[]> cacheQuery = new ScanQuery<>((key, val) -> key.namespace.equals(namespace));
        try (QueryCursor<javax.cache.Cache.Entry<NamedKey, byte[]>> cursor = cache.query(cacheQuery)) {
            Set<NamedKey> keysToRemove = new HashSet<>();
            for (javax.cache.Cache.Entry<NamedKey, byte[]> entry : cursor)
                keysToRemove.add(entry.getKey());
            cache.removeAll(keysToRemove);
            log.info("removing of cache entries for namespace " + namespace + " successful");
        } catch (Throwable t) {
            log.info("removing of cache entries for namespace " + namespace + " failed");
            throw t;
        }
    }

    @Override
    public boolean isLocal() {
        return this.isLocal;
    }

    @Override
    public CacheStats getStats() {
        CacheMetrics metrics = cache.metrics();
        log.info("IGNITE DRUID CACHE: hit count: " + metrics.getCacheHits());
        return new io.druid.client.cache.CacheStats(
                metrics.getCacheHits(),
                metrics.getCacheMisses(),
                metrics.getSize(),
                metrics.getOffHeapAllocatedSize(),
                metrics.getCacheEvictions(),
                0,
                numErrors
        );
    }

    @Override
    public void doMonitor(ServiceEmitter emitter) {
        // TODO: add ignite-specific metrics here
    }
}
