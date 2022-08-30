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
package us.swcraft.springframework.cache.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import us.swcraft.springframework.store.StoreCompression;
import us.swcraft.springframework.store.persistence.AerospikeTemplate;
import us.swcraft.springframework.store.serialization.FSTSerializer;

import javax.inject.Inject;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
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
        aerospikeCacheManager.createCache(name, 850);
        assertThat(aerospikeCacheManager.getCache(name), notNullValue());
        assertThat(aerospikeCacheManager.getCache(name).getNativeCache(), notNullValue());
        assertThat(((AerospikeTemplate) aerospikeCacheManager.getCache("cache:ITF").getNativeCache()).getNamespace(),
                is("cache"));
        assertThat(((AerospikeTemplate) aerospikeCacheManager.getCache("cache:ITF").getNativeCache()).getSetname(),
                is("ITF"));
        assertThat(((AerospikeTemplate) aerospikeCacheManager.getCache("cache:ITF").getNativeCache()).getExpiration(),
                is(850));     

    }

    @Test
    public void createCache_setNameOnly() {
        String name = "ITS";
        aerospikeCacheManager.createCache(name, 1200);
        assertThat(aerospikeCacheManager.getCache("cache:ITS"), notNullValue());
        assertThat(((AerospikeTemplate) aerospikeCacheManager.getCache("cache:ITS").getNativeCache()).getNamespace(),
                is("cache"));
        assertThat(((AerospikeTemplate) aerospikeCacheManager.getCache("cache:ITS").getNativeCache()).getSetname(),
                is("ITS"));
        assertThat(((AerospikeTemplate) aerospikeCacheManager.getCache("cache:ITS").getNativeCache()).getExpiration(),
                is(1200));        
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

        @SuppressWarnings("rawtypes")
        @Bean
        @Inject
        public AerospikeCacheManager aerospikeCacheManager(IAerospikeClient aerospikeClient) {
            final AerospikeCacheManager aerospikeCacheManager = new AerospikeCacheManager("cache", "ITD", 600,
                    aerospikeClient, new FSTSerializer(StoreCompression.NONE));
            return aerospikeCacheManager;
        }

    }

}
