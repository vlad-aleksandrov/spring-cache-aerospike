/*
 * Copyright 2015 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package us.swcraft.springframework.cache.aerospike;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.util.Assert;

import us.swcraft.springframework.store.persistence.AerospikeTemplate;
import us.swcraft.springframework.store.serialization.Serializer;

import com.aerospike.client.IAerospikeClient;


@SuppressWarnings("rawtypes")
public class AerospikeCacheManager implements CacheManager {

    private final String defaultNamespace;
    private final String defaultSetname;

    private String defaultCacheName;

    private int defaultTimeToLiveInSeconds;

    // Aerospike clients to configure AerospikeTemplate instance (one per Cache)
    private IAerospikeClient aerospikeClient;

    private Serializer serializer;

    // lazy initialized caches
    private ConcurrentHashMap<String, AerospikeCache> caches = new ConcurrentHashMap<>(32);

    
    public AerospikeCacheManager(String defaultNamespace, String defaultSetname, int defaultTimeToLiveInSeconds,
            IAerospikeClient aerospikeClient, Serializer serializer) {
        Assert.hasText(defaultNamespace, "namespace can't be null");
        Assert.hasText(defaultSetname, "default setname can't be null");
        Assert.notNull(aerospikeClient, "aerospike client can't be null");
        Assert.notNull(serializer, "serializer can't be null");
        this.defaultNamespace = defaultNamespace;
        this.defaultSetname = defaultSetname;
        this.defaultCacheName = this.defaultNamespace + ":" + this.defaultSetname;

        this.aerospikeClient = aerospikeClient;
        this.serializer = serializer;
        
        // pre-build default cache
        createCache(defaultCacheName, defaultTimeToLiveInSeconds);
    }

    @Override
    public Cache getCache(final String name) {
        final String cacheName = (name == null) ? defaultCacheName : name;
        
        String fullName = null;
        if (cacheName.contains(":")) {
            fullName = cacheName;
        } else {
            fullName = defaultNamespace + ":" + cacheName;
        }        
        
        if (!caches.containsKey(fullName)) {
            createCache(fullName);
        }
        return caches.get(fullName);
    }

    @Override
    public Collection<String> getCacheNames() {
        return caches.keySet();
    }

    public AerospikeCache createCache(final String name) {
        return createCache(name, defaultTimeToLiveInSeconds);
    }

    public AerospikeCache createCache(final String name, int timeToLive) {
        Assert.hasText(name, "Cache name can't be empty");
        AerospikeTemplate template = null;

        if (name.contains(":")) {
            // namespace explicitly defined
            String[] nameParts = name.split(":", 2);
            template = buildAerospikeTemplate(nameParts[0], nameParts[1]);
        } else {
            template = buildAerospikeTemplate(defaultNamespace, name);
        }
        template.setExpiration(timeToLive);
        // initialize the template before passing it to cache
        template.init();
        final AerospikeCache cache = new AerospikeCache(template, serializer);
        caches.put(cache.getName(), cache);
        return cache;

    }

    private AerospikeTemplate buildAerospikeTemplate(final String namespace, final String setname) {
        final AerospikeTemplate template = new AerospikeTemplate();
        template.setAerospikeClient(aerospikeClient);
        template.setNamespace(namespace);
        template.setSetname(setname);
        template.init();
        return template;
    }

}
