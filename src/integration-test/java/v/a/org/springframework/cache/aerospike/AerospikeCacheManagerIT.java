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
package v.a.org.springframework.cache.aerospike;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

import javax.inject.Inject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import v.a.org.springframework.store.StoreCompression;
import v.a.org.springframework.store.persistence.AerospikeTemplate;
import v.a.org.springframework.store.serialization.FastStoreSerializer;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.ClientPolicy;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class AerospikeCacheManagerIT {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Inject
    private AerospikeCacheManager aerospikeCacheManager;

    @Test
    public void when_contextStarted_thenNoExceptions() {
        log.info("Spring context loaded. Aerospike Cache manager: {}", aerospikeCacheManager);
    }

    @Test
    public void defaultCacheCreated() {
        assertThat(aerospikeCacheManager.getCache("cache:ITC"), notNullValue());
    }

    @Test
    public void getCache() {
        String name = "cache:ITNEW";
        Cache c1 = aerospikeCacheManager.getCache(name);
        assertThat(c1, notNullValue());
        Cache c2 = aerospikeCacheManager.getCache(name);
        assertThat(c2, notNullValue());
        assertThat(c1, sameInstance(c2));
    }

    @Test
    public void createCache_fullName() {
        String name = "cache:ITF";
        aerospikeCacheManager.createCache(name, 600);
        assertThat(aerospikeCacheManager.getCache(name), notNullValue());
    }

    @Test
    public void createCache_setNameOnly() {
        String name = "ITS";
        aerospikeCacheManager.createCache(name, 600);
        assertThat(aerospikeCacheManager.getCache("cache:ITS"), notNullValue());
        assertThat(((AerospikeTemplate) aerospikeCacheManager.getCache("cache:ITS").getNativeCache()).getNamespace(),
                is("cache"));
        assertThat(((AerospikeTemplate) aerospikeCacheManager.getCache("cache:ITS").getNativeCache()).getSetname(),
                is("ITS"));
    }

    @Configuration
    @PropertySource(value = "classpath:/application.properties")
    static class Config {

        @Inject
        private Environment env;

        @Bean(destroyMethod = "close")
        public IAerospikeClient aerospikeClient() throws Exception {
            final ClientPolicy defaultClientPolicy = new ClientPolicy();
            final IAerospikeClient client = new AerospikeClient(defaultClientPolicy, new Host(
                    env.getProperty("aerospike.host"),
                    Integer.valueOf(env.getProperty("aerospike.port"))));
            return client;
        }

        @Bean(destroyMethod = "close")
        public IAsyncClient aerospikeAsyncClient() throws Exception {
            final AsyncClientPolicy defaultAsyncClientPolicy = new AsyncClientPolicy();
            final IAsyncClient client = new AsyncClient(defaultAsyncClientPolicy, new Host(
                    env.getProperty("aerospike.host"),
                    Integer.valueOf(env.getProperty("aerospike.port"))));
            return client;
        }

        @Bean
        @Inject
        public AerospikeCacheManager aerospikeCacheManager(IAerospikeClient aerospikeClient,
                IAsyncClient aerospikeAsyncClient) {
            final AerospikeCacheManager aerospikeCacheManager = new AerospikeCacheManager("cache", "ITD", 600,
                    aerospikeClient, aerospikeAsyncClient, new FastStoreSerializer(StoreCompression.NONE));
            return aerospikeCacheManager;
        }

    }

}
