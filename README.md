# About spring-cache-aerospike

Full featured Aerospike cache backend for Spring.

# Overview

A cache manager implementation that stores data in Aerospike. 

Data cached must implement `Serializable` interface if you plan to use FST serializer.

# Architecture

TBD: provides the cache creation, saving, and loading functionality.

# Usage

Add `@EnableAerospikeCacheManager` annotation to your `@Configuration` class to expose the Spring `CacheManager` as a bean named `aerospikeCacheManager` and backed by Aerospike.

In order to leverage the annotation, a single instance of each `IAerospikeClient` and `IAsyncClient` must be provided. For example:

```
@Configuration
@EnableAerospikeCacheManager
@EnableCaching
public class AerospikeCacheConfig {
     
    @Bean(destroyMethod = "close")
    public AerospikeClient aerospikeClient() throws Exception {
        return new AerospikeClient("localhost", 3000);
    }
 
    @Bean(destroyMethod = "close")
    public AsyncClient aerospikeAsyncClient() throws Exception {
        return new AsyncClient("localhost", 3000);
    }
 
}
```

More advanced configurations can extend `AerospikeCacheConfiguration`} instead.

# Configuration

`@EnableAerospikeCacheManager` annotation has optional parameters you can use for cache configuration fine-tuning.

* `int defaultTimeToLiveInSeconds` defines TTL for cached entries. Default is **1800** sec.
 * `-1` - the cached entity is stored indefinitely
 * `0` - default TTL for namespace as configured in Aerospike
 * `any positive value` - actual TTL in seconds  
* `String defaultNamespace` - Default Aerospike namespace used by cache manager for persisting caches.  Default name is `cache`.
* `String defaultCacheName` - Aerospike setname inside namespace for default cache.  Default name is `default`.
* `Class<? extends Serializer> serializerClass` - cached value serializer class implementing `Serializer` interface. Provided implementations are [Kryo](https://github.com/EsotericSoftware/kryo) and [Snappy](https://github.com/dain/snappy)
 * `FSTSerializer.class` - cached value must implement `Serializable` interface
 * `KryoSerializer.class`- cached value must have default "no-args" constructor
 * `KryoReflectionSupportSerializer.class` - Kryo serializer that uses sun's `ReflectionFactory` to create new instance for classes without a default constructor.
* `StoreCompression compression` - cached value compression type. Supported types are `NONE` and `SNAPPY` (see [Snappy](https://github.com/dain/snappy)).  Default is `NONE`.
* `AerospikeCacheConfig[] caches` - pre-configured caches. If cache name is not defined here, it will be created automatically with default parameters. `AerospikeCacheConfig` parameters are:
 * `String name` - cache name in *namespace:setname* format. If name does not have *namespace* part, the cache will be created in `defaultNamespace`. 
 * `int timeToLiveInSeconds` - cached entry TTL for particular cache

## Example


```
    @EnableAerospikeCacheManager(
(1)            serializerClass = FSTSerializer.class,   
(2)            compression = StoreCompression.SNAPPY,   
(3)            defaultNamespace = "cache",              
(4)            defaultCacheName = "ITDEFAULT",          
(5)            defaultTimeToLiveInSeconds = 300,        
               caches = {
(6)                    @AerospikeCacheConfig(name = "preconfigured", timeToLiveInSeconds = 100),           
(7)                    @AerospikeCacheConfig(name = "persistentcache:another", timeToLiveInSeconds = 600)
(8)                    @AerospikeCacheConfig(name = "cachewithdefaulttl")
               }
    )
```
1. Use `FSTSerializer` for value serialization
2. Compress value with `SNAPPY` compression
3. `cache` is a default Aerospike namespace
4. `ITDEFAULT` is a setname for default cache
5. Default TTL for caches is 300 sec
6. Defines `preconfigured` cache in default namespace with TTL 100 sec
7. Defines `another` cache in `persistentcache` namespace with TTL 600 sec   
8. Defines `cachewithdefaulttl` cache in default namespace with default TTL 300 sec a set via `defaultTimeToLiveInSeconds` parameter



## Serialization customization

TBD

## Compression Configuration

TBD


