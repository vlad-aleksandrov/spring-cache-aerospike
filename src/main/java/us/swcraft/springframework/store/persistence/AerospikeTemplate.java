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
package us.swcraft.springframework.store.persistence;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;

/**
 * Helper class that simplifies Aerospike data access code.
 * <br>
 * Once configured, this class is thread-safe.
 * <br>
 * Note that while the template is generified, it is up to the serializers/deserializers to properly convert the given
 * Objects to and from binary data.
 * <br>
 * <b>This is the central class in Aerospike support</b>.
 * 
 * @author Vlad Aleksandrov
 */
public class AerospikeTemplate extends AerospikeAccessor implements AerospikeOperations<String> {

    private final static Bin[] BIN_ARRAY_TYPE = new Bin[0];
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Namespace name.
     */
    private String namespace;

    /**
     * Aerospike set name.
     */
    private String setname;

    private int expiration;

    private WritePolicy deletePolicy;
    private WritePolicy writePolicyUpdate;
    private WritePolicy writePolicyCommitMaster;
    private WritePolicy writePolicyCreateOnly;
    private Policy readPolicy;

    public void init() {
        Assert.hasLength(namespace, "Aerospike 'namespace' name is not configured");
        Assert.hasLength(setname, "Aerospike 'setname' name is not configured");

        deletePolicy = new WritePolicy();
        deletePolicy.commitLevel = CommitLevel.COMMIT_MASTER;
        deletePolicy.totalTimeout = 2000;

        writePolicyUpdate = new WritePolicy();
        writePolicyUpdate.expiration = expiration;
        writePolicyUpdate.recordExistsAction = RecordExistsAction.UPDATE;
        writePolicyUpdate.commitLevel = CommitLevel.COMMIT_ALL;
        writePolicyUpdate.totalTimeout = 2000;

        writePolicyCommitMaster = new WritePolicy();
        writePolicyCommitMaster.recordExistsAction = RecordExistsAction.UPDATE;
        writePolicyCommitMaster.commitLevel = CommitLevel.COMMIT_MASTER;
        writePolicyCommitMaster.totalTimeout = 2000;

        writePolicyCreateOnly = new WritePolicy();
        writePolicyCreateOnly.expiration = expiration;
        writePolicyCreateOnly.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        writePolicyCreateOnly.commitLevel = CommitLevel.COMMIT_ALL;
        writePolicyCreateOnly.totalTimeout = 2000;

        readPolicy = new Policy();
        readPolicy.totalTimeout = 2000;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasKey(final String key) {
        log.trace("has {} key?", key);
        Assert.notNull(key, "key can't be null");
        final Key recordKey = new Key(namespace, setname, key);
        return getAerospikeClient().exists(readPolicy, recordKey);
    }

    @Override
    public void delete(final String key) {
        log.trace("delete {} key", key);
        Assert.notNull(key, "key can't be null");
        final Key recordKey = new Key(namespace, setname, key);
        getAerospikeClient().delete(deletePolicy, recordKey);
    }

    @Override
    public void deleteBin(final String key, final String binName) {
        log.trace("delete {} bin in record key {}", binName, key);
        Assert.notNull(key, "key can't be null");
        final Key recordKey = new Key(namespace, setname, key);
        Assert.notNull(binName, "bin name can't be null");
        final Bin bin = Bin.asNull(binName);
        getAerospikeClient().put(deletePolicy, recordKey, bin);
    }

    @Override
    public void persist(final String key, final Bin bin) {
        log.trace("persist {} bin in record key {}", bin, key);
        Assert.notNull(key, "key can't be null");
        final Key recordKey = new Key(namespace, setname, key);
        Assert.notNull(bin, "bin can't be null");
        getAerospikeClient().put(writePolicyUpdate, recordKey, bin);
    }

    @Override
    public void persistIfAbsent(final String key, final Bin bin) {
        log.trace("persist {} bin in record key {}", bin, key);
        Assert.notNull(key, "key can't be null");
        final Key recordKey = new Key(namespace, setname, key);
        Assert.notNull(bin, "bin can't be null");
        getAerospikeClient().put(writePolicyCreateOnly, recordKey, bin);
    }

    @Override
    public void persist(final String key, final Set<Bin> bins) {
        Assert.notNull(key, "key can't be null");
        final Key recordKey = new Key(namespace, setname, key);
        Assert.notNull(bins, "bins can't be null");
        Assert.notEmpty(bins, "bins should have data to store");
        getAerospikeClient().put(writePolicyUpdate, recordKey, bins.toArray(BIN_ARRAY_TYPE));
    }

    @Override
    public void persistIfAbsent(final String key, final Set<Bin> bins) {
        Assert.notNull(key, "key can't be null");
        final Key recordKey = new Key(namespace, setname, key);
        Assert.notNull(bins, "bins can't be null");
        Assert.notEmpty(bins, "bins should have data to store");
        getAerospikeClient().put(writePolicyCreateOnly, recordKey, bins.toArray(BIN_ARRAY_TYPE));
    }

    @Override
    public Record fetch(final String key) {
        Assert.notNull(key, "key can't be null");
        final Key recordKey = new Key(namespace, setname, key);
        return getAerospikeClient().get(readPolicy, recordKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createIndex(final String binName, final String indexName, final IndexType indexType) {
        final Policy policy = new Policy();
        policy.totalTimeout = 0; // Do not timeout on index create.

        final IndexTask task = getAerospikeClient().createIndex(policy, namespace, setname, indexName, binName, indexType);
        task.waitTillComplete();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> fetchRange(final String idBinName, final String indexedBinName, final long begin, final long end) {

        final Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(setname);
        stmt.setBinNames(indexedBinName);
        stmt.setFilter(Filter.range(indexedBinName, begin, end));

        final RecordSet rs = getAerospikeClient().query(null, stmt);
        final Set<String> result = new HashSet<>();
        try {
            while (rs.next()) {
                Key key = rs.getKey();
                log.trace("Found key: {}", key);
                Record record = getAerospikeClient().get(readPolicy, key, idBinName);
                if (record != null) {
                    result.add(record.getString(idBinName));
                }
            }
        } finally {
            rs.close();
        }
        return result;
    }

    @Override
    public void deleteAll() {
        getAerospikeClient().scanAll(new ScanPolicy(), namespace, setname, new ScanCallback() {
            public void scanCallback(Key key, Record record) throws AerospikeException {
                getAerospikeClient().delete(writePolicyCommitMaster, key);
            }
        }, new String[] {});
    }

    @Override
    public void touch(final String key) {
        Assert.notNull(key, "key can't be null");
        final Key recordKey = new Key(namespace, setname, key);
        getAerospikeClient().touch(writePolicyCommitMaster, recordKey);
    }

    public void setNamespace(final String namespace) {
        this.namespace = namespace;
    }

    public void setSetname(final String setname) {
        this.setname = setname;
    }

    public void setExpiration(final int expiration) {
        this.expiration = expiration;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSetname() {
        return setname;
    }

    public int getExpiration() {
        return expiration;
    }

}
