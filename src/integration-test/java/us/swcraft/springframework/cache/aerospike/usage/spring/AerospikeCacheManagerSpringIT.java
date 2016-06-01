/*
 * Copyright 2015 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package us.swcraft.springframework.cache.aerospike.usage.spring;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

import javax.inject.Inject;
import javax.inject.Named;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import us.swcraft.springframework.cache.aerospike.config.annotation.AerospikeCacheConfig;
import us.swcraft.springframework.cache.aerospike.config.annotation.EnableAerospikeCacheManager;
import us.swcraft.springframework.store.StoreCompression;
import us.swcraft.springframework.store.serialization.KryoSerializer;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.ClientPolicy;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class AerospikeCacheManagerSpringIT {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Inject
    @Named("aerospikeCacheManager")
    private CacheManager aerospikeCacheManager;

    @Inject
    private ISomeCacheableService cacheableService;

    @Test
    public void when_contextStarted_thenNoExceptions() {
        log.info("Spring context loaded. Aerospike Cache manager: {}", aerospikeCacheManager);
    }

    @Test
    public void service_cacheableDescription() {
        String desc1i = cacheableService.getDescription(1);
        String desc1c = cacheableService.getDescription(1);
        assertThat(desc1c, is(desc1i));
        assertThat(desc1c, not(sameInstance(desc1i)));

        String name = "cache:ITUUID";
        Cache c = aerospikeCacheManager.getCache(name);
        String cachedDescription = c.get(1, String.class);
        assertThat(cachedDescription, is(desc1i));
    }

    @Test(expected = IllegalStateException.class)
    public void service_cacheableValue() {
        // no cache name is set in  @Cacheable
        cacheableService.getValue(2);
    }
    
    @Test
    public void service_cacheableName() {
        String name1i = cacheableService.getName(1);
        String name1c = cacheableService.getName(1);
        assertThat(name1c, is(name1i));
        assertThat(name1c, not(sameInstance(name1i)));

        String name = "cache:ITDEFAULT";
        Cache c = aerospikeCacheManager.getCache(name);
        String cachedName = c.get(1, String.class);
        assertThat(cachedName, is(name1i));
    }

    @Configuration
    @PropertySource(value = "classpath:/application.properties")
    @EnableAerospikeCacheManager(
            serializerClass = KryoSerializer.class,
            compression = StoreCompression.SNAPPY,
            defaultNamespace = "cache",
            defaultCacheName = "ITDEFAULT",
            defaultTimeToLiveInSeconds = 300,
            caches = {
                    @AerospikeCacheConfig(name = "ITUUID", timeToLiveInSeconds = 100)
            })
    @ComponentScan(basePackages = "us.swcraft.springframework.cache.aerospike.usage.spring")
    @EnableCaching
    static class Config {

        @Inject
        private Environment env;

        @Bean(name = "someCacheableService")
        public ISomeCacheableService someCacheableService() throws Exception {
            return new SomeCacheableService();
        }

        @Bean(destroyMethod = "close")
        public IAerospikeClient aerospikeClient() throws Exception {
            final ClientPolicy defaultClientPolicy = new ClientPolicy();
            final IAerospikeClient client = new AerospikeClient(defaultClientPolicy,
                    new Host(env.getProperty("aerospike.host"),
                            Integer.valueOf(env.getProperty("aerospike.port"))));
            return client;
        }

        @Bean(destroyMethod = "close")
        public IAsyncClient aerospikeAsyncClient() throws Exception {
            final AsyncClientPolicy defaultAsyncClientPolicy = new AsyncClientPolicy();
            final IAsyncClient client = new AsyncClient(defaultAsyncClientPolicy,
                    new Host(env.getProperty("aerospike.host"),
                            Integer.valueOf(env.getProperty("aerospike.port"))));
            return client;
        }
    }

}
