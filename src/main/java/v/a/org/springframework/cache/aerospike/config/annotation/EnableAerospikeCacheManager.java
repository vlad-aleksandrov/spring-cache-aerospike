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
package v.a.org.springframework.cache.aerospike.config.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import v.a.org.springframework.store.StoreCompression;
import v.a.org.springframework.store.serialization.FastStoreSerializer;
import v.a.org.springframework.store.serialization.Serializer;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.IAsyncClient;

/**
 * Add this annotation to an {@code @Configuration} class to expose the {@link CacheManager} as a bean named
 * "aerospikeCacheManager" and backed by Aerospike.
 * 
 * In order to leverage the annotation, a single instance of each {@link IAerospikeClient} and {@link IAsyncClient} must
 * be provided. For example:
 *
 * <pre>
 * {@literal @Configuration}
 * {@literal EnableAerospikeCacheManager}
 * public class AerospikeCacheConfig {
 *     
 *     {@literal @Bean(destroyMethod = "close")}
 *     public AerospikeClient aerospikeClient() throws Exception {
 *         return new AerospikeClient("localhost", 3000);
 *     }
 * 
 *     {@literal @Bean(destroyMethod = "close")}
 *     public AsyncClient aerospikeAsyncClient() throws Exception {
 *         return new AsyncClient("localhost", 3000);
 *     }
 * 
 * }
 * </pre>
 *
 * More advanced configurations can extend {@link AerospikeCacheConfiguration} instead.
 *
 * @author Vlad Aleksandrov
 */
@Retention(value = java.lang.annotation.RetentionPolicy.RUNTIME)
@Target(value = { java.lang.annotation.ElementType.TYPE })
@Documented
@Import(AerospikeCacheConfiguration.class)
@Configuration
public @interface EnableAerospikeCacheManager {

    int defaultTimeToLiveInSeconds() default 1800;

    /**
     * Default Aerospike namespace for caching is <code>cache</code>.
     * 
     */
    String defaultNamespace() default "cache";

    /**
     * Aerospike setname for default cache.
     * 
     */
    String defaultCacheName() default "default";

    /**
     * Compression applied to stored value. Default is NONE. Current supported compression is <code>SNAPPY</code>.
     *
     */
    StoreCompression compression() default StoreCompression.NONE;

    /**
     * Cached value serializer class implementing {@link Serializer} interface.
     * 
     */
    @SuppressWarnings("rawtypes")
    Class<? extends Serializer> serializerClass() default FastStoreSerializer.class;

    /**
     * Pre-configured caches.
     */
    AerospikeCacheConfig[] caches() default {};
    
}