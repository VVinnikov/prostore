package io.arenadata.dtm.query.execution.plugin.adg.configuration.cache;

import io.arenadata.dtm.cache.factory.CaffeineCacheServiceFactory;
import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static io.arenadata.dtm.query.execution.plugin.adg.AdgDataSourcePlugin.ADG_DATAMART_CACHE;
import static io.arenadata.dtm.query.execution.plugin.adg.AdgDataSourcePlugin.ADG_QUERY_TEMPLATE_CACHE;

@Configuration
public class CacheConfiguration {

    @Bean("adgQueryTemplateCacheService")
    public CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService(@Qualifier("coffeineCacheManager")
                                                                                        CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<QueryTemplateKey, QueryTemplateValue>(cacheManager)
                .create(ADG_QUERY_TEMPLATE_CACHE);
    }

    @Bean("adgDatamartCacheService")
    public CacheService<String, String> datamartCacheService(@Qualifier("coffeineCacheManager")
                                                                     CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<String, String>(cacheManager)
                .create(ADG_DATAMART_CACHE);
    }

}
