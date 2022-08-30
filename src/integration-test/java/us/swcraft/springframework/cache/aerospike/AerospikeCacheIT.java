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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import javax.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import us.swcraft.springframework.store.StoreCompression;
import us.swcraft.springframework.store.persistence.AerospikeTemplate;
import us.swcraft.springframework.store.serialization.FSTSerializer;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;

@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class AerospikeCacheIT {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Inject
    private AerospikeCache aerospikeCache;

    @BeforeEach
    public void prepare() {
        aerospikeCache.clear();
    }

    @Test
    public void when_contextStarted_thenNoExceptions() {
        log.info("Spring context loaded. Aerospike Cache: {}", aerospikeCache);
    }

    @Test
    public void getName() {
        assertThat(aerospikeCache.getName(), is("cache:ITC"));
    }

    @Test
    public void getNativeCache() {
        assertThat(aerospikeCache.getNativeCache() instanceof AerospikeTemplate, is(true));
    }

    @Test
    public void evict() {
        aerospikeCache.put("A", "B");
        assertThat(aerospikeCache.get("A").get(), is("B"));
        aerospikeCache.evict("A");
        assertThat(aerospikeCache.get("A"), nullValue());
    }

    @Test
    public void put_null() {
        aerospikeCache.put("A", null);
        assertThat(aerospikeCache.get("A").get(), nullValue());
    }

    @Test
    public void put_string() {
        String val = "Polymorphic value classes";
        aerospikeCache.put("A", val);
        Object result = aerospikeCache.get("A").get();
        assertThat(result, is(val));
    }

    @Test
    public void get_withType() {
        String val = "Polymorphic value classes";
        aerospikeCache.put("A", val);
        assertThat(aerospikeCache.get("A", String.class), is(val));
    }

    @Test
    public void get_withIncorrectType() {
        IllegalStateException thrown = Assertions.assertThrows(IllegalStateException.class, () -> {
            String val = "Polymorphic value classes";
            aerospikeCache.put("A", val);
            aerospikeCache.get("A", Integer.class);
        }, "IllegalStateException was expected");

        assertThat(thrown.getMessage(), is("cache entry 'A' has been found but failed to match 'Integer' type"));
    }

    @Test
    public void putIfAbsent() {
        String val = "Polymorphic value classes";
        ValueWrapper result1 = aerospikeCache.putIfAbsent("A", val);
        assertThat(result1, nullValue());
        assertThat(aerospikeCache.get("A").get(), is(val));

        ValueWrapper result2 = aerospikeCache.putIfAbsent("A", "DEADBEED");
        assertThat(result2, notNullValue());
        assertThat(result2.get(), is(not("DEADBEED")));
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

        @Bean(initMethod = "init")
        @Inject
        public AerospikeTemplate aerospikeTemplate(final IAerospikeClient aerospikeClient) {
            final AerospikeTemplate aerospikeTemplate = new AerospikeTemplate();
            aerospikeTemplate.setAerospikeClient(aerospikeClient);

            aerospikeTemplate.setNamespace("cache");
            aerospikeTemplate.setSetname("ITC");
            aerospikeTemplate.setExpiration(600);
            return aerospikeTemplate;
        }

        @SuppressWarnings("rawtypes")
        @Bean
        @Inject
        public AerospikeCache aerospikeCache(final AerospikeTemplate aerospikeTemplate) {
            return new AerospikeCache(aerospikeTemplate, new FSTSerializer(StoreCompression.NONE));
        }

    }

}
