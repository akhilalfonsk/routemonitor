package com.causefinder.routemonitor.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.concurrent.TimeUnit;

@Configuration
@EnableCaching
public class CaffeineCacheConfig {

    @Bean
    @Primary
    public CacheManager cacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager("RouteDataCache");
        cacheManager.setCaffeine(routeDataCacheBuilder());
        return cacheManager;
    }

    private Caffeine<Object, Object> routeDataCacheBuilder() {
        return Caffeine.newBuilder()
                .initialCapacity(100)
                .maximumSize(500)
                .expireAfterWrite(60, TimeUnit.MINUTES);
    }


    @Bean
    public CacheManager realTimeStopDataCacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager("RealTimeStopDataCache");
        cacheManager.setCaffeine(realTimeStopDataCacheBuilder());
        return cacheManager;
    }

    private Caffeine<Object, Object> realTimeStopDataCacheBuilder() {
        return Caffeine.newBuilder()
                .initialCapacity(500)
                .maximumSize(1000)
                .expireAfterWrite(1, TimeUnit.MINUTES);
    }
}