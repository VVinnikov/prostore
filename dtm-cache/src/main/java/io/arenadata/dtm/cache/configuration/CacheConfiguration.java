package io.arenadata.dtm.cache.configuration;

import io.arenadata.dtm.cache.factory.CacheManagerFactory;
import io.arenadata.dtm.cache.factory.CaffeineCacheManagerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration()
@DependsOn({"cacheProperties"})
public class CacheConfiguration {

    @Bean("caffeineCacheManagerFactory")
    public CaffeineCacheManagerFactory caffeineCacheManagerFactory() {
        return new CaffeineCacheManagerFactory();
    }

    @Bean("caffeineCacheManager")
    public CacheManager caffeineCacheManager(@Qualifier("caffeineCacheManagerFactory") CacheManagerFactory caffeineCacheManagerFactory,
                                             @Qualifier("cacheProperties") CacheProperties cacheProperties) {
        return caffeineCacheManagerFactory.create(cacheProperties);
    }

}
