package io.arenadata.dtm.query.execution.core.configuration.cache;

import io.arenadata.dtm.cache.factory.CaffeineCacheServiceFactory;
import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.core.dto.cache.EntityKey;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class CacheConfiguration {

    public static final String CORE_QUERY_TEMPLATE_CACHE = "coreQueryTemplateCache";
    public static final String ENTITY_CACHE = "entity";
    public static final String HOT_DELTA_CACHE = "hotDelta";
    public static final String OK_DELTA_CACHE = "okDelta";

    @Bean("entityCacheService")
    public CacheService<EntityKey, Entity> entityCacheService(@Qualifier("coffeineCacheManager")
                                                                      CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<EntityKey, Entity>(cacheManager)
                .create(ENTITY_CACHE);
    }

    @Bean("hotDeltaCacheService")
    public CacheService<String, HotDelta> hotDeltaCacheService(@Qualifier("coffeineCacheManager")
                                                                       CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<String, HotDelta>(cacheManager)
                .create(HOT_DELTA_CACHE);
    }

    @Bean("okDeltaCacheService")
    public CacheService<String, OkDelta> okDeltaCacheService(@Qualifier("coffeineCacheManager")
                                                                     CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<String, OkDelta>(cacheManager)
                .create(OK_DELTA_CACHE);
    }

    @Bean("coreQueryTemplateCacheService")
    public CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService(@Qualifier("coffeineCacheManager")
                                                                                        CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<QueryTemplateKey, SourceQueryTemplateValue>(cacheManager)
                .create(CORE_QUERY_TEMPLATE_CACHE);
    }

}
