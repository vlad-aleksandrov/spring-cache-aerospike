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

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import us.swcraft.springframework.store.persistence.AerospikeTemplate;
import us.swcraft.springframework.store.serialization.SerializationException;
import us.swcraft.springframework.store.serialization.Serializer;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;

@SuppressWarnings("rawtypes")
public class AerospikeCache implements Cache {

    private static final Logger log = LoggerFactory.getLogger(AerospikeCache.class);

    private static final String VALUE_BIN = "V";
    private static final String CLASS_NAME_BIN = "C";

    private final AerospikeTemplate template;
    private final Serializer serializer;

    public AerospikeCache(final AerospikeTemplate template, Serializer serializer) {
        this.template = template;
        this.serializer = serializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return template.getNamespace() + ":" + template.getSetname();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object getNativeCache() {
        return template;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        log.trace("Clear cache: {}", template.getSetname());
        template.deleteAll();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void evict(final Object key) {
        log.trace("Evicting {} from cache: {}", key, template.getSetname());
        template.delete(key.toString());
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void put(final Object key, final Object value) {
        final Set<Bin> binsToSave = new HashSet<>(2, 1);
        if (value != null) {
            final String className = value.getClass().getName();
            binsToSave.add(new Bin(CLASS_NAME_BIN, className));
            binsToSave.add(new Bin(VALUE_BIN, serializer.serialize(value)));
        } else {
            binsToSave.add(new Bin(CLASS_NAME_BIN, "NIL"));
            binsToSave.add(new Bin(VALUE_BIN, "NIL"));
        }
        log.trace("Persisting {}={} in cache: {}", key, binsToSave, template.getSetname());
        template.persist(key.toString(), binsToSave);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public ValueWrapper get(final Object key) {
        final String k = key.toString();
        final Record record = template.fetch(k);
        if (record == null) {
            log.trace("Not found: {}", k);
            return null;
        } else {
            final String className = record.getString(CLASS_NAME_BIN);
            if ("NIL".equals(className)) {
                // null-value stored
                log.trace("Got: {}=null", k);
                template.touch(k);
                return new SimpleValueWrapper(null);
            }
            final byte[] binaryContent = (byte[]) record.getValue(VALUE_BIN);
            try {
                final Object value = serializer.deserialize(binaryContent, Class.forName(className));
                log.trace("Got: {}={}", key, value);
                template.touch(k);
                return new SimpleValueWrapper(value);
            } catch (SerializationException | ClassNotFoundException e) {
                log.warn("Class {} deserialization issue: {}", className, e.getMessage());
                log.trace("", e);
                return null;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(final Object key, final Class<T> type) {
        final String k = key.toString();
        final Record record = template.fetch(k);
        if (record == null) {
            log.trace("Not found: {}", k);
            return null;
        }

        final String className = record.getString(CLASS_NAME_BIN);
        if (className == null) {
            log.trace("Got: {}=null", k);
            template.touch(k);
            return null;
        }

        try {
            if (type.isAssignableFrom(Class.forName(className))) {
                final Object value = serializer.deserialize((byte[])record.getValue(VALUE_BIN), type);
                log.trace("Got: {}={}", k, value);
                template.touch(k);
                return (T) value;
            } else {
                throw new IllegalStateException("cache entry '" + key + "' has been found but failed to match '" + type
                        + "' type");
            }
        } catch (SerializationException | ClassNotFoundException e) {
            log.warn("Class {} deserialization issue: {}", className, e.getMessage());
            log.trace("", e);
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ValueWrapper putIfAbsent(final Object key, final Object value) {
        final ValueWrapper existingValue = get(key);
        if (existingValue == null) {
            put(key, value);
            return null;
        } else {
            return existingValue;
        }
    }

    @Override
    public String toString() {
        return "AerospikeCache [name=" + getName() + "]";
    }

}