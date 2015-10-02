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
package v.a.org.springframework.store.persistence;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.query.IndexType;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class AerospikeTemplateIT {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Inject
    private AerospikeTemplate template;

    @Before
    public void prepare() {
        template.deleteAll();
    }

    @Test
    public void when_contextStarted_thenNoExceptions() {
        log.info("Spring context loaded. Aerospike Template: {}", template);
    }

    @Test
    public void delete() {
        String id = UUID.randomUUID().toString();
        Set<Bin> bins = new HashSet<>();
        bins.add(new Bin("key", id));
        bins.add(new Bin("expired", 10000));
        template.persist(id, bins);
        Record result = template.fetch(id);
        assertThat(result, notNullValue());
        template.delete(id);
        assertThat(template.fetch(id), nullValue());
    }

    @Test
    public void deleteBin() {
        String id = UUID.randomUUID().toString();
        Set<Bin> bins = new HashSet<>();
        bins.add(new Bin("key", id));
        bins.add(new Bin("A", 10000));
        bins.add(new Bin("B", 20000));
        template.persist(id, bins);
        Record result = template.fetch(id);
        assertThat(result, notNullValue());
        assertThat(result.getInt("A"), is(10000));
        assertThat(result.getInt("B"), is(20000));
        template.deleteBin(id, "B");
        result = template.fetch(id);
        assertThat(result.getValue("B"), nullValue());
        assertThat(result.getInt("A"), is(10000));
    }

    @Test
    public void persist_fetch() {
        String id = UUID.randomUUID().toString();
        Set<Bin> bins = new HashSet<>();
        bins.add(new Bin("key", id));
        bins.add(new Bin("expired", 10000));
        template.persist(id, bins);
        Record result = template.fetch(id);
        assertThat(result, notNullValue());
        assertThat(result.getString("key"), is(id));
        assertThat(result.getLong("expired"), is(10000L));
    }

    @Test
    public void persistIfAbsent_newRecordMultiBin() {
        String id = UUID.randomUUID().toString();
        Set<Bin> bins = new HashSet<>();
        bins.add(new Bin("key", id));
        bins.add(new Bin("expired", 10000));
        template.persistIfAbsent(id, bins);
        Record result = template.fetch(id);
        assertThat(result, notNullValue());
        assertThat(result.getString("key"), is(id));
        assertThat(result.getLong("expired"), is(10000L));
    }

    @Test(expected = AerospikeException.class)
    public void persistIfAbsent_existingRecordMultiBin() {
        String id = UUID.randomUUID().toString();
        Set<Bin> bins = new HashSet<>();
        bins.add(new Bin("key", id));
        bins.add(new Bin("expired", 10000));
        template.persist(id, bins);
        Record result = template.fetch(id);
        assertThat(result, notNullValue());

        Set<Bin> extrabins = new HashSet<>();
        extrabins.add(new Bin("A", "ALPHA"));
        extrabins.add(new Bin("Z", "OMEGA"));
        template.persistIfAbsent(id, extrabins);
    }

    @Test
    public void persistIfAbsent_newRecordSingleBin() {
        String id = UUID.randomUUID().toString();
        template.persistIfAbsent(id, new Bin("key", id));
        Record result = template.fetch(id);
        assertThat(result, notNullValue());
        assertThat(result.getString("key"), is(id));
    }

    @Test(expected = AerospikeException.class)
    public void persistIfAbsent_existingRecordSingleBin() {
        String id = UUID.randomUUID().toString();
        Set<Bin> bins = new HashSet<>();
        bins.add(new Bin("key", id));
        bins.add(new Bin("expired", 10000));
        template.persist(id, bins);
        Record result = template.fetch(id);
        assertThat(result, notNullValue());
        template.persistIfAbsent(id, new Bin("A", "ALPHA"));
    }

    @Test
    public void createIndex_fetchRange() {
        template.createIndex("expired", "expiredIndxIT", IndexType.NUMERIC);
        String id = UUID.randomUUID().toString();
        Set<Bin> bins = new HashSet<>();
        bins.add(new Bin("sessionId", id));
        bins.add(new Bin("expired", 1000));
        template.persist(id, bins);
        Set<String> result = template.fetchRange("sessionId", "expired", 999, 1001);
        assertThat(result.size(), is(1));
        for (String key : result) {
            assertThat(key, is(id));
        }
    }

    @Test
    public void hasKey() {
        assertThat("not exist", template.hasKey(UUID.randomUUID().toString()), is(false));
        String id = UUID.randomUUID().toString();
        Set<Bin> bins = new HashSet<>();
        bins.add(new Bin("key", id));
        bins.add(new Bin("expired", Long.MAX_VALUE));
        template.persist(id, bins);
        assertThat("exist", template.hasKey(id), is(true));
    }

    @Test
    public void touch_exist() throws InterruptedException {
        String id = UUID.randomUUID().toString();
        template.persist(id, new Bin("key", id));
        Record result = template.fetch(id);
        assertThat(result, notNullValue());
        assertThat(result.getString("key"), is(id));
        int exp1 = result.expiration;
        Thread.sleep(2000);
        template.touch(id);
        result = template.fetch(id);
        int exp2 = result.expiration;
        assertThat(exp1, is(not(exp2)));
    }

    @Test(expected = AerospikeException.class)
    public void touch_notExist() {
        template.touch(UUID.randomUUID().toString());
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

        @Bean(initMethod = "init")
        @Inject
        public AerospikeTemplate aerospikeTemplate(IAerospikeClient aerospikeClient, IAsyncClient aerospikeAsyncClient) {
            final AerospikeTemplate aerospikeTemplate = new AerospikeTemplate();
            aerospikeTemplate.setAerospikeClient(aerospikeClient);
            aerospikeTemplate.setAerospikeAsyncClient(aerospikeAsyncClient);

            aerospikeTemplate.setNamespace("cache");
            aerospikeTemplate.setSetname("IT");
            aerospikeTemplate.setExpiration(600);
            return aerospikeTemplate;
        }
    }

}
