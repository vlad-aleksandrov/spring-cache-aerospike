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
package v.a.org.springframework.store.serialization;

import java.lang.reflect.InvocationHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.reflect.ReflectionFactory;
import v.a.org.springframework.store.StoreCompression;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.LocaleSerializer;

import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyMapSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptySetSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonMapSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonSetSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.KryoReflectionFactorySupport;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;

/**
 * A Kryo specialization that uses sun's {@link ReflectionFactory} to create new instance for classes without a default
 * constructor.
 *
 * @param <T>
 */
public class KryoReflectionSupportSerializer<T> extends KryoSerializer<T> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public KryoReflectionSupportSerializer() {
        super.init();
    }

    public KryoReflectionSupportSerializer(final StoreCompression compressionType) {
        super(compressionType);
    }

    /**
     * Configure kryo instance, customize settings.
     * 
     * @param kryo
     */
    public void customConfiguration(final Kryo kryo) {
        log.trace("Register extra serializers");

        kryo.addDefaultSerializer(Locale.class, LocaleSerializer.class);

        kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
        kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
        kryo.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
        kryo.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
        kryo.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer());
        kryo.register(Collections.singleton("").getClass(), new CollectionsSingletonSetSerializer());
        kryo.register(Collections.singletonMap("", "").getClass(), new CollectionsSingletonMapSerializer());
        kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
        kryo.register(InvocationHandler.class, new JdkProxySerializer());
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        SynchronizedCollectionsSerializer.registerSerializers(kryo);

        // guava ImmutableList
        ImmutableListSerializer.registerSerializers(kryo);

    }

    public Kryo getKryoInstance() {
        log.trace("Create instance of KryoReflectionFactorySupport");
        return new KryoReflectionFactorySupport();
    }

}
