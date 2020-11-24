package io.arenadata.dtm.query.execution.core.configuration.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
@EnableCaching
public class CacheConfiguration {

    public static final String ENTITY_CACHE = "entity";
    public static final String HOT_DELTA_CACHE = "hotDelta";
    public static final String OK_DELTA_CACHE = "okDelta";
    public static final String ADB_DATAMART_CACHE = "adb_datamart";
    public static final String ADG_DATAMART_CACHE = "adg_datamart";
    public static final String ADQM_DATAMART_CACHE = "adqm_datamart";

    @Bean("caffeineCacheManager")
    public CacheManager cacheManager(CacheProperties cacheProperties) {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager(ENTITY_CACHE, HOT_DELTA_CACHE, OK_DELTA_CACHE, ADB_DATAMART_CACHE, ADG_DATAMART_CACHE, ADQM_DATAMART_CACHE);
        cacheManager.setCaffeine(caffeineCacheBuilder(cacheProperties));
        return cacheManager;
    }

    private Caffeine<Object, Object> caffeineCacheBuilder(CacheProperties cacheProperties) {
        return Caffeine.newBuilder()
            .initialCapacity(cacheProperties.getInitialCapacity())
            .maximumSize(cacheProperties.getMaximumSize())
            .expireAfterAccess(cacheProperties.getExpireAfterAccessMinutes(), TimeUnit.MINUTES)
            .recordStats();
    }

}
