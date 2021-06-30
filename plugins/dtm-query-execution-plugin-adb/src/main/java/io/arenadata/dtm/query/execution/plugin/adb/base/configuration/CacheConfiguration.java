package io.arenadata.dtm.query.execution.plugin.adb.base.configuration;

import io.arenadata.dtm.cache.factory.CaffeineCacheServiceFactory;
import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static io.arenadata.dtm.query.execution.plugin.adb.base.service.AdbDtmDataSourcePlugin.ADB_DATAMART_CACHE;
import static io.arenadata.dtm.query.execution.plugin.adb.base.service.AdbDtmDataSourcePlugin.ADB_QUERY_TEMPLATE_CACHE;

@Configuration
public class CacheConfiguration {

    @Bean("adbQueryTemplateCacheService")
    public CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService(@Qualifier("caffeineCacheManager")
                                                                                        CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<QueryTemplateKey, QueryTemplateValue>(cacheManager)
                .create(ADB_QUERY_TEMPLATE_CACHE);
    }

    @Bean("adbDatamartCacheService")
    public CacheService<String, String> datamartCacheService(@Qualifier("caffeineCacheManager")
                                                                     CacheManager cacheManager) {
        return new CaffeineCacheServiceFactory<String, String>(cacheManager)
                .create(ADB_DATAMART_CACHE);
    }

}
