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

import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

import v.a.org.springframework.store.persistence.AerospikeTemplate;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.IAsyncClient;

/**
 * Exposes the {@link CacheManager} as a bean named "aerospikeCacheManager" and backed by Aerospike.
 * In order to use this a single instance of each {@link IAerospikeClient} and
 * {@link IAsyncClient} must be exposed as a Bean.
 *
 * @author Vlad Aleksandrov
 *
 * @see EnableAerospikeCacheManager
 */
@Configuration
public class AerospikeCacheConfiguration implements ImportAware, BeanClassLoaderAware {

    private ClassLoader beanClassLoader;

    private Integer defaultTimeToLiveInSeconds = 1800;
    /**
     * Default Aerospike namespace is <code>cache</code>.
     */
    private String namespace = "cache";




    @Bean(initMethod = "init")
    @Inject
    public AerospikeTemplate sessionAerospikeTemplate(final IAerospikeClient aerospikeClient,
            final IAsyncClient asyncAerospikeClient) {
        final AerospikeTemplate template = new AerospikeTemplate();
        template.setAerospikeClient(aerospikeClient);
        template.setAerospikeAsyncClient(asyncAerospikeClient);
        template.setNamespace(namespace);
        template.setSetname(setname);
        return template;
    }

    @Bean
    public AerospikeStoreSessionRepository sessionRepository() {
        final AerospikeStoreSessionRepository sessionRepository = new AerospikeStoreSessionRepository(actorSystem,
                springExtension);
        sessionRepository.setDefaultMaxInactiveInterval(maxInactiveIntervalInSeconds);
        return sessionRepository;
    }

    @Bean
    public <S extends ExpiringSession> SessionRepositoryFilter<? extends ExpiringSession> springSessionRepositoryFilter(
            SessionRepository<S> sessionRepository, ServletContext servletContext) {
        SessionRepositoryFilter<S> sessionRepositoryFilter = new SessionRepositoryFilter<S>(sessionRepository);
        sessionRepositoryFilter.setServletContext(servletContext);
        if (httpSessionStrategy != null) {
            sessionRepositoryFilter.setHttpSessionStrategy(httpSessionStrategy);
        }
        return sessionRepositoryFilter;
    }

    public void setMaxInactiveIntervalInSeconds(int maxInactiveIntervalInSeconds) {
        this.maxInactiveIntervalInSeconds = maxInactiveIntervalInSeconds;
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
                EnableAerospikeCacheManager enableWebSecurityAnnotation = AnnotationUtils.findAnnotation(classToInspect,
                        EnableAerospikeCacheManager.class);
                if (enableWebSecurityAnnotation == null) {
                    continue;
                }
                enableAttrMap = AnnotationUtils
                        .getAnnotationAttributes(enableWebSecurityAnnotation);
                enableAttrs = AnnotationAttributes.fromMap(enableAttrMap);
            }
        }
        maxInactiveIntervalInSeconds = enableAttrs.getNumber("maxInactiveIntervalInSeconds");
        namespace = enableAttrs.getString("namespace");
        setname = enableAttrs.getString("setname");
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
