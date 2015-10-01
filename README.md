# About spring-cache-aerospike

Full featured Aerospike cache backend for Spring.

# Overview

A cache manager implementation that stores data in Aerospike for easy distribution of requests across a cluster of services. 

Data cached must be Serializable.

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

`@EnableAerospikeCacheManager` annotation has optional parameters you can use for cache configuration fine tuning.

* `int defaultTimeToLiveInSeconds` defines TTL for cached entries. Default is 1800 sec.
* `String defaultNamespace` - Default Aerospike namespace used by cache manager for persisting caches.  Default name is `cache`.
* `String defaultCacheName` - Aerospike setname inside namespace for default cache.  Default name is `default`.
* `StoreCompression compression` - Compression applied to stored value. Default is `NONE`. Current supported compression is `SNAPPY`.
* `Class<? extends Serializer> serializerClass` - cached value serializer class implementing `Serializer` interface. Provided implementations are [Kryo](https://github.com/EsotericSoftware/kryo) and [Snappy](https://github.com/dain/snappy)

* `StoreCompression compression` - cached value compression type. Supported types are `NONE` and `SNAPPY` (see [Snappy](https://github.com/dain/snappy)).  Default is `NONE`.
* `AerospikeCacheConfig[] caches` - pre-configured caches. If cache name is not defined her, it will be created automatically with default parameters



## Serialization Configuration
## Compression Configuration


