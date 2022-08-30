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
package us.swcraft.springframework.cache.aerospike.config.annotation;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

import us.swcraft.springframework.cache.aerospike.AerospikeCacheManager;
import us.swcraft.springframework.store.StoreCompression;
import us.swcraft.springframework.store.serialization.Serializer;

import com.aerospike.client.IAerospikeClient;

/**
 * Exposes the {@link CacheManager} as a bean named "aerospikeCacheManager" and backed by Aerospike.
 * In order to use this a single instance of each {@link IAerospikeClient} must be exposed as a
 * Bean.
 *
 * @author Vlad Aleksandrov
 *
 * @see EnableAerospikeCacheManager
 */
@Configuration
@SuppressWarnings("rawtypes")
@ComponentScan("us.swcraft")
public class AerospikeCacheConfiguration implements ImportAware, BeanClassLoaderAware {

    private ClassLoader beanClassLoader;

    private Integer defaultTimeToLiveInSeconds = 1800;
    /**
     * Default Aerospike namespace is <code>cache</code>.
     */
    private String defaultNamespace;

    private String defaultCacheName;

    private Class<? extends Serializer> serializerClass;

    private StoreCompression compression;

    /**
     * Pre-configured caches.
     */
    private AnnotationAttributes[] cachesConfiguration;

    @Inject
    @Bean(name = "aerospikeCacheManager")
    public AerospikeCacheManager aerospikeCacheManager(final IAerospikeClient aerospikeClient) {
        final AerospikeCacheManager aerospikeCacheManager = new AerospikeCacheManager(defaultNamespace,
                defaultCacheName,  defaultTimeToLiveInSeconds, aerospikeClient, buildSerializer());

        // pre-build configured caches
        for (AnnotationAttributes cacheConfigAttrs : cachesConfiguration) {
            final String name = cacheConfigAttrs.getString("name");
            final int timeToLiveInSeconds = cacheConfigAttrs.getNumber("timeToLiveInSeconds");
            aerospikeCacheManager.createCache(name, timeToLiveInSeconds);
        }
        return aerospikeCacheManager;
    }

    private Serializer buildSerializer() {
        try {
            return serializerClass.getConstructor(StoreCompression.class).newInstance(compression);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Unable to build serializer " + serializerClass, e);
        }
    }

    public void setDefaultTimeToLiveInSeconds(int defaultTimeToLiveInSeconds) {
        this.defaultTimeToLiveInSeconds = defaultTimeToLiveInSeconds;
    }

    public void setImportMetadata(AnnotationMetadata importMetadata) {
        Map<String, Object> enableAttrMap = importMetadata.getAnnotationAttributes(EnableAerospikeCacheManager.class
                .getName());
        AnnotationAttributes enableAttrs = AnnotationAttributes.fromMap(enableAttrMap);
        if (enableAttrs == null) {
            // search parent classes
            Class<?> currentClass = ClassUtils.resolveClassName(importMetadata.getClassName(), beanClassLoader);
            for (Class<?> classToInspect = currentClass; classToInspect != null; classToInspect = classToInspect
                    .getSuperclass()) {
                EnableAerospikeCacheManager enableWebSecurityAnnotation = AnnotationUtils.findAnnotation(
                        classToInspect,
                        EnableAerospikeCacheManager.class);
                if (enableWebSecurityAnnotation == null) {
                    continue;
                }
                enableAttrMap = AnnotationUtils
                        .getAnnotationAttributes(enableWebSecurityAnnotation);
                enableAttrs = AnnotationAttributes.fromMap(enableAttrMap);
            }
        }
        defaultTimeToLiveInSeconds = enableAttrs.getNumber("defaultTimeToLiveInSeconds");
        defaultNamespace = enableAttrs.getString("defaultNamespace");
        defaultCacheName = enableAttrs.getString("defaultCacheName");
        compression = enableAttrs.getEnum("compression");
        serializerClass = enableAttrs.getClass("serializerClass");

        cachesConfiguration = enableAttrs.getAnnotationArray("caches");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.springframework.beans.factory.BeanClassLoaderAware#setBeanClassLoader(java.lang.ClassLoader)
     */
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.beanClassLoader = classLoader;
    }

}
