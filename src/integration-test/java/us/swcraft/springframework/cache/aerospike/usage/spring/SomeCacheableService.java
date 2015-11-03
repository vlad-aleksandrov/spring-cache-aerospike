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
package us.swcraft.springframework.cache.aerospike.usage.spring;

import java.util.UUID;

import org.springframework.cache.annotation.Cacheable;

public class SomeCacheableService implements ISomeCacheableService {
    
    @Override
    @Cacheable(cacheManager = "aerospikeCacheManager", value="ITUUID")
    public String getDescription(Integer id) {
        return new StringBuilder().append(id).append(":").append(UUID.randomUUID()).toString();
    }
    
    @Override
    @Cacheable(cacheManager = "aerospikeCacheManager", value="ITDEFAULT")
    public String getName(Integer id) {
        return new StringBuilder().append(id).append(":").append(UUID.randomUUID()).toString();
    }
    
    /**
     * Uses default cache.
     * @param id
     * @return
     */
    @Override
    @Cacheable(cacheManager = "aerospikeCacheManager")
    public String getValue(Integer id) {
        return new StringBuilder().append(id).append(":").append(UUID.randomUUID()).toString();
    }


}
